from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import os

from psycopg.errors import UniqueViolation

from app.auth import decode, dummy_hash, password_hash, tokens
from app.errors import ApiError


MAX_AMOUNT = Decimal("9999999999.99")
MAX_INT64 = 9223372036854775807


def health(request):
    db = request.state.db
    db.execute("SELECT 1")
    return {"status": "ok"}


def register(request, body):
    db = request.state.db
    try:
        return db.execute(
            "INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id, username, role",
            (body.username, password_hash.hash(body.password.get_secret_value())),
        ).fetchone()
    except UniqueViolation:
        raise ApiError(409, "USERNAME_TAKEN", "Имя пользователя уже занято")


def login(request, body):
    db = request.state.db
    user = db.execute("SELECT * FROM users WHERE username = %s", (body.username,)).fetchone()
    valid = password_hash.verify(body.password.get_secret_value(), user["password_hash"] if user else dummy_hash)
    if user is None or not valid:
        raise ApiError(401, "TOKEN_INVALID", "Неверный логин или пароль")
    return tokens(user)


def refresh(request, body):
    db = request.state.db
    payload = decode(body.refresh_token, "refresh")
    user = db.execute("SELECT * FROM users WHERE id = %s", (int(payload["sub"]),)).fetchone()
    if user is None or user["role"] != payload["role"]:
        raise ApiError(401, "REFRESH_TOKEN_INVALID", "Пользователь или роль изменились")
    return tokens(user)


def get_product(request, principal, id):
    db = request.state.db
    product = db.execute("SELECT * FROM products WHERE id = %s", (id,)).fetchone()
    if product is None:
        raise ApiError(404, "PRODUCT_NOT_FOUND", "Товар не найден", {"product_id": id})
    return product


def create_product(request, principal, body):
    db = request.state.db
    user = principal
    return db.execute(
        """INSERT INTO products (name, description, price, stock, category, status, seller_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING *""",
        (body.name, body.description, body.price, body.stock, body.category, body.status.value, user["id"]),
    ).fetchone()


def list_products(request, principal, page, size, status, category):
    db = request.state.db
    offset = page * size
    if offset > MAX_INT64:
        raise ApiError(400, "VALIDATION_ERROR", "Слишком большой номер страницы", {
            "fields": [{"field": "query.page", "message": "Смещение страницы превышает int64"}],
        })
    conditions = []
    params = []
    if status is not None:
        conditions.append("status = %s")
        params.append(status.value)
    if category is not None:
        conditions.append("category = %s")
        params.append(category)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = db.execute(
        "WITH filtered AS (SELECT * FROM products" + where + ") " + """
        SELECT page_products.*, totals.total
        FROM (SELECT count(*) AS total FROM filtered) AS totals
        LEFT JOIN (
            SELECT * FROM filtered ORDER BY id LIMIT %s OFFSET %s
        ) AS page_products ON TRUE
        ORDER BY page_products.id""",
        [*params, size, offset],
    ).fetchall()
    total = rows[0]["total"]
    for row in rows:
        row.pop("total")
    return {"items": [row for row in rows if row["id"] is not None], "totalElements": total, "page": page, "size": size}


def owned_product(db, user, id):
    product = db.execute("SELECT * FROM products WHERE id = %s FOR UPDATE", (id,)).fetchone()
    if product is None:
        raise ApiError(404, "PRODUCT_NOT_FOUND", "Товар не найден", {"product_id": id})
    if user["role"] != "ADMIN" and product["seller_id"] != user["id"]:
        raise ApiError(403, "ACCESS_DENIED", "Товар принадлежит другому продавцу")
    return product


def update_product(request, principal, id, body):
    db = request.state.db
    user = principal
    owned_product(db, user, id)
    reserved = db.execute(
        """SELECT COALESCE(SUM(order_items.quantity), 0) AS quantity
        FROM order_items JOIN orders ON orders.id = order_items.order_id
        WHERE product_id = %s AND orders.status IN ('CREATED', 'PAYMENT_PENDING')""",
        (id,),
    ).fetchone()["quantity"]
    if body.stock + reserved > MAX_INT64:
        raise ApiError(400, "VALIDATION_ERROR", "Недостаточно места для возврата резерва", {
            "fields": [{"field": "body.stock", "message": f"С учётом резерва {reserved} максимум {MAX_INT64 - reserved}"}],
        })
    return db.execute(
        """UPDATE products SET name = %s, description = %s, price = %s, stock = %s,
        category = %s, status = %s WHERE id = %s RETURNING *""",
        (body.name, body.description, body.price, body.stock, body.category, body.status.value, id),
    ).fetchone()


def archive_product(request, principal, id):
    db = request.state.db
    user = principal
    owned_product(db, user, id)
    db.execute("UPDATE products SET status = 'ARCHIVED' WHERE id = %s", (id,))


def create_promo(request, principal, body):
    db = request.state.db
    if body.valid_until <= body.valid_from:
        raise ApiError(400, "VALIDATION_ERROR", "Неверный срок промокода", {
            "fields": [{"field": "valid_until", "message": "Должен быть позже valid_from"}],
        })
    try:
        return db.execute(
            """INSERT INTO promo_codes (code, discount_type, discount_value, min_order_amount,
            max_uses, valid_from, valid_until, active) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
            (body.code, body.discount_type.value, body.discount_value, body.min_order_amount,
             body.max_uses, body.valid_from, body.valid_until, body.active),
        ).fetchone()
    except UniqueViolation:
        raise ApiError(409, "PROMO_CODE_EXISTS", "Такой промокод уже существует")


def lock_user(db, id):
    db.execute("SELECT id FROM users WHERE id = %s FOR UPDATE", (id,))


def check_interval(db, user_id, operation):
    interval = timedelta(minutes=float(os.environ.get("ORDER_INTERVAL_MINUTES", "1")))
    latest = db.execute(
        """SELECT created_at FROM user_operations WHERE user_id = %s AND operation_type = %s
        ORDER BY created_at DESC LIMIT 1""", (user_id, operation),
    ).fetchone()
    if latest and datetime.now(timezone.utc) - latest["created_at"] < interval:
        raise ApiError(429, "ORDER_LIMIT_EXCEEDED", "Слишком частая операция", {"interval_minutes": interval.total_seconds() / 60})


def record_operation(db, user_id, operation):
    db.execute("INSERT INTO user_operations (user_id, operation_type, created_at) VALUES (%s, %s, clock_timestamp())", (user_id, operation))


def quantities(items):
    result = {}
    for item in items:
        if item.product_id in result:
            raise ApiError(400, "VALIDATION_ERROR", "Товар повторяется в заказе", {"fields": [{"field": "items", "message": f"Повтор product_id={item.product_id}"}]})
        result[item.product_id] = item.quantity
    return result


def lock_products(db, ids):
    rows = db.execute("SELECT * FROM products WHERE id = ANY(%s) ORDER BY id FOR UPDATE", (sorted(ids),)).fetchall()
    return {row["id"]: row for row in rows}


def reserve(db, requested, products):
    for id in requested:
        if id not in products:
            raise ApiError(404, "PRODUCT_NOT_FOUND", "Товар не найден", {"product_id": id})
        if products[id]["status"] != "ACTIVE":
            raise ApiError(409, "PRODUCT_INACTIVE", "Товар неактивен", {"product_id": id})
    missing = [{"product_id": id, "requested": quantity, "available": products[id]["stock"]}
               for id, quantity in requested.items() if products[id]["stock"] < quantity]
    if missing:
        raise ApiError(409, "INSUFFICIENT_STOCK", "Недостаточно товара", {"products": missing})
    for id, quantity in requested.items():
        db.execute("UPDATE products SET stock = stock - %s WHERE id = %s", (quantity, id))
    return sum((products[id]["price"] * quantity for id, quantity in requested.items()), Decimal("0.00"))


def promo_discount(promo, total, already_used=False):
    now = datetime.now(timezone.utc)
    uses = promo["current_uses"] - int(already_used)
    if not promo["active"] or uses >= promo["max_uses"] or not promo["valid_from"] <= now <= promo["valid_until"]:
        raise ApiError(422, "PROMO_CODE_INVALID", "Промокод недействителен")
    if total < promo["min_order_amount"]:
        raise ApiError(422, "PROMO_CODE_MIN_AMOUNT", "Сумма ниже минимальной для промокода", {"min_order_amount": str(promo["min_order_amount"])})
    if promo["discount_type"] == "PERCENTAGE":
        discount = (total * promo["discount_value"] / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if promo["discount_value"] > 70 or discount > total * Decimal("0.70"):
            raise ApiError(422, "PROMO_CODE_INVALID", "Процентная скидка превышает 70%")
        return discount
    return min(total, promo["discount_value"])


def validate_amounts(total, discount):
    fields = [
        {"field": field, "message": f"Не должно превышать {MAX_AMOUNT} руб."}
        for field, amount in [("total_amount", total - discount), ("discount_amount", discount)]
        if amount > MAX_AMOUNT
    ]
    if fields:
        raise ApiError(400, "VALIDATION_ERROR", "Слишком большая сумма заказа", {"fields": fields})


def save_items(db, order_id, requested, products):
    for id, quantity in requested.items():
        db.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, price_at_order) VALUES (%s, %s, %s, %s)",
            (order_id, id, quantity, products[id]["price"]),
        )


def order_response(db, order):
    order["items"] = db.execute(
        "SELECT id, product_id, quantity, price_at_order FROM order_items WHERE order_id = %s ORDER BY id", (order["id"],),
    ).fetchall()
    return order


def create_order(request, principal, body):
    db = request.state.db
    user = principal
    lock_user(db, user["id"])
    check_interval(db, user["id"], "CREATE_ORDER")
    active = db.execute("SELECT id FROM orders WHERE user_id = %s AND status IN ('CREATED', 'PAYMENT_PENDING')", (user["id"],)).fetchone()
    if active:
        raise ApiError(409, "ORDER_HAS_ACTIVE", "У пользователя уже есть активный заказ")
    requested = quantities(body.items)
    products = lock_products(db, requested)
    total = reserve(db, requested, products)
    discount = Decimal("0.00")
    promo_id = None
    if body.promo_code is not None:
        promo = db.execute("SELECT * FROM promo_codes WHERE code = %s FOR UPDATE", (body.promo_code,)).fetchone()
        if promo is None:
            raise ApiError(422, "PROMO_CODE_INVALID", "Промокод не найден")
        discount = promo_discount(promo, total)
        promo_id = promo["id"]
        db.execute("UPDATE promo_codes SET current_uses = current_uses + 1 WHERE id = %s", (promo_id,))
    validate_amounts(total, discount)
    order = db.execute(
        """INSERT INTO orders (user_id, status, promo_code_id, total_amount, discount_amount)
        VALUES (%s, 'CREATED', %s, %s, %s) RETURNING *""", (user["id"], promo_id, total - discount, discount),
    ).fetchone()
    save_items(db, order["id"], requested, products)
    record_operation(db, user["id"], "CREATE_ORDER")
    return order_response(db, order)


def owned_order(db, user, id, lock=False):
    if lock:
        owner = db.execute("SELECT user_id FROM orders WHERE id = %s", (id,)).fetchone()
        if owner:
            for user_id in sorted({owner["user_id"], user["id"]}):
                lock_user(db, user_id)
    order = db.execute("SELECT * FROM orders WHERE id = %s" + (" FOR UPDATE" if lock else " FOR SHARE"), (id,)).fetchone()
    if order is None:
        raise ApiError(404, "ORDER_NOT_FOUND", "Заказ не найден")
    if user["role"] != "ADMIN" and order["user_id"] != user["id"]:
        raise ApiError(403, "ORDER_OWNERSHIP_VIOLATION", "Заказ принадлежит другому пользователю")
    return order


def get_order(request, principal, id):
    db = request.state.db
    user = principal
    return order_response(db, owned_order(db, user, id))


def update_order(request, principal, id, body):
    db = request.state.db
    user = principal
    order = owned_order(db, user, id, lock=True)
    if order["status"] != "CREATED":
        raise ApiError(409, "INVALID_STATE_TRANSITION", "Изменять можно только CREATED")
    check_interval(db, user["id"], "UPDATE_ORDER")
    old_items = db.execute("SELECT * FROM order_items WHERE order_id = %s", (id,)).fetchall()
    requested = quantities(body.items)
    products = lock_products(db, set(requested) | {item["product_id"] for item in old_items})
    for item in old_items:
        products[item["product_id"]]["stock"] += item["quantity"]
        db.execute("UPDATE products SET stock = stock + %s WHERE id = %s", (item["quantity"], item["product_id"]))
    total = reserve(db, requested, products)
    discount = Decimal("0.00")
    promo_id = order["promo_code_id"]
    if promo_id is not None:
        promo = db.execute("SELECT * FROM promo_codes WHERE id = %s FOR UPDATE", (promo_id,)).fetchone()
        try:
            discount = promo_discount(promo, total, already_used=True)
        except ApiError as error:
            if error.code != "PROMO_CODE_MIN_AMOUNT":
                raise
            db.execute("UPDATE promo_codes SET current_uses = current_uses - 1 WHERE id = %s", (promo_id,))
            promo_id = None
    validate_amounts(total, discount)
    order = db.execute(
        "UPDATE orders SET total_amount = %s, discount_amount = %s, promo_code_id = %s WHERE id = %s RETURNING *",
        (total - discount, discount, promo_id, id),
    ).fetchone()
    db.execute("DELETE FROM order_items WHERE order_id = %s", (id,))
    save_items(db, id, requested, products)
    record_operation(db, user["id"], "UPDATE_ORDER")
    return order_response(db, order)


def cancel_order(request, principal, id):
    db = request.state.db
    user = principal
    order = owned_order(db, user, id, lock=True)
    if order["status"] not in {"CREATED", "PAYMENT_PENDING"}:
        raise ApiError(409, "INVALID_STATE_TRANSITION", "Отмена разрешена только для CREATED и PAYMENT_PENDING")
    items = db.execute("SELECT * FROM order_items WHERE order_id = %s", (id,)).fetchall()
    lock_products(db, {item["product_id"] for item in items})
    for item in items:
        db.execute("UPDATE products SET stock = stock + %s WHERE id = %s", (item["quantity"], item["product_id"]))
    if order["promo_code_id"] is not None:
        db.execute("UPDATE promo_codes SET current_uses = current_uses - 1 WHERE id = %s", (order["promo_code_id"],))
    order = db.execute("UPDATE orders SET status = 'CANCELED' WHERE id = %s RETURNING *", (id,)).fetchone()
    return order_response(db, order)
