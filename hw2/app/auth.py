from datetime import datetime, timedelta, timezone
import os
from uuid import uuid4

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt
from pwdlib import PasswordHash

from app.db import get_db
from app.errors import ApiError


password_hash = PasswordHash.recommended()
dummy_hash = password_hash.hash("unused-password")
bearer = HTTPBearer(auto_error=False)


def tokens(user):
    now = datetime.now(timezone.utc)
    result = {"token_type": "bearer"}
    for kind, lifetime in [("access", timedelta(minutes=20)), ("refresh", timedelta(days=14))]:
        payload = {
            "sub": str(user["id"]), "role": user["role"], "type": kind,
            "iat": now, "exp": now + lifetime, "jti": str(uuid4()),
            "iss": "marketplace", "aud": "marketplace",
        }
        result[f"{kind}_token"] = jwt.encode(payload, os.environ["JWT_SECRET"], algorithm="HS256")
    return result


def decode(token, kind):
    code = "REFRESH_TOKEN_INVALID" if kind == "refresh" else "TOKEN_INVALID"
    try:
        payload = jwt.decode(
            token, os.environ["JWT_SECRET"], algorithms=["HS256"],
            issuer="marketplace", audience="marketplace",
            options={"require": ["sub", "role", "type", "iat", "exp", "jti"]},
        )
        if payload["type"] != kind or int(payload["sub"]) <= 0:
            raise ValueError
        return payload
    except jwt.ExpiredSignatureError:
        raise ApiError(401, "TOKEN_EXPIRED" if kind == "access" else code, "Токен истёк")
    except (jwt.InvalidTokenError, ValueError, TypeError):
        raise ApiError(401, code, "Невалидный токен")


def current_user(request: Request, credentials: HTTPAuthorizationCredentials | None = Depends(bearer), db=Depends(get_db, scope="function")):
    if credentials is None:
        raise ApiError(401, "TOKEN_INVALID", "Нужен Bearer access token")
    payload = decode(credentials.credentials, "access")
    user = db.execute("SELECT id, username, role FROM users WHERE id = %s", (int(payload["sub"]),)).fetchone()
    if user is None or user["role"] != payload["role"]:
        raise ApiError(401, "TOKEN_INVALID", "Пользователь или роль изменились")
    request.state.user_id = user["id"]
    require_role(user, request.app.state.roles[request.scope["route"].operation_id])
    return user


def require_role(user, roles):
    if user["role"] not in roles:
        raise ApiError(403, "ACCESS_DENIED", "Недостаточно прав")


def authorize(requirement_sets, credentials):
    return credentials["bearerAuth"]
