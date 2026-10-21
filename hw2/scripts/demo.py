from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import secrets
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import connect


base_url = "http://127.0.0.1:8000"
suffix = secrets.token_hex(4)


def call(method, path, body=None, token=None, expected=200):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request(base_url + path, data=json.dumps(body).encode() if body is not None else None, headers=headers, method=method)
    try:
        response = urlopen(request)
    except HTTPError as error:
        response = error
    raw = response.read()
    data = json.loads(raw) if raw else None
    print(f"{method} {path}: {response.status}, request_id={response.headers.get('X-Request-Id')}")
    if not path.startswith("/auth/"):
        print(json.dumps(data, ensure_ascii=False))
    assert response.status == expected, data
    return data


def account(role):
    credentials = {"username": f"demo_{role.lower()}_{suffix}", "password": secrets.token_urlsafe(18)}
    user = call("POST", "/auth/register", credentials, expected=201)
    if role != "USER":
        with connect() as db:
            db.execute("UPDATE users SET role = %s WHERE id = %s", (role, user["id"]))
    pair = call("POST", "/auth/login", credentials)
    return user, pair["access_token"]


seller, seller_token = account("SELLER")
buyer, buyer_token = account("USER")
product_data = {"name": "Demo book", "description": None, "price": 100, "stock": 10, "category": "books", "status": "ACTIVE"}
product = call("POST", "/products", product_data, seller_token, 201)
product_path = f"/products/{product['id']}"
call("GET", "/products?page=0&size=20&status=ACTIVE&category=books", token=buyer_token)
now = datetime.now(timezone.utc)
promo = call("POST", "/promo-codes", {
    "code": "DEMO_" + suffix.upper(), "discount_type": "PERCENTAGE", "discount_value": 10,
    "min_order_amount": 150, "max_uses": 1, "valid_from": (now - timedelta(days=1)).isoformat(),
    "valid_until": (now + timedelta(days=1)).isoformat(),
}, seller_token, 201)
order = call("POST", "/orders", {"items": [{"product_id": product["id"], "quantity": 2}], "promo_code": promo["code"]}, buyer_token, 201)
order_path = f"/orders/{order['id']}"
call("PUT", product_path, {**product_data, "price": 200, "stock": 8}, seller_token)
call("GET", order_path, token=buyer_token)
call("PUT", order_path, {"items": [{"product_id": product["id"], "quantity": 999}]}, buyer_token, 409)
call("GET", product_path, token=buyer_token)
call("PUT", order_path, {"items": [{"product_id": product["id"], "quantity": 3}]}, buyer_token)
call("PUT", order_path, {"items": [{"product_id": product["id"], "quantity": 999}]}, buyer_token, 429)
call("GET", product_path, token=buyer_token)
call("POST", order_path + "/cancel", token=buyer_token)
call("POST", order_path + "/cancel", token=buyer_token, expected=409)
call("GET", product_path, token=buyer_token)
call("DELETE", product_path, token=seller_token, expected=204)
call("GET", product_path, token=buyer_token)

with connect() as db:
    for table, column, id in [("products", "id", product["id"]), ("orders", "id", order["id"]),
                              ("order_items", "order_id", order["id"]), ("promo_codes", "id", promo["id"]),
                              ("user_operations", "user_id", buyer["id"])]:
        query = f"SELECT * FROM {table} WHERE {column} = %s"
        rows = db.execute(query, (id,)).fetchall()
        print(f"{query} [{id}]")
        print(json.dumps(rows, default=str, ensure_ascii=False, indent=2))
