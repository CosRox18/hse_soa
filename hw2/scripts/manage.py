import argparse
from getpass import getpass
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.auth import password_hash
from app.db import connect
from app.generated.models import RegisterRequest


parser = argparse.ArgumentParser()
parser.add_argument("username")
parser.add_argument("--role", choices=["USER", "SELLER", "ADMIN"], default="USER")
args = parser.parse_args()
body = RegisterRequest(username=args.username, password=getpass("Password: "))
with connect() as db:
    user = db.execute(
        "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s) RETURNING id, username, role",
        (body.username, password_hash.hash(body.password.get_secret_value()), args.role),
    ).fetchone()
print(user)
