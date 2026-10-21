from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from uuid import uuid4

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException
import yaml

from app.errors import ApiError
from app import services
from app.auth import authorize, current_user
from app.db import get_db
from app.generated.server.application import build_router
from app.generated.server.security import bearer_auth


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("marketplace")
app = FastAPI(title="Marketplace")
spec = yaml.safe_load((Path(__file__).resolve().parents[1] / "openapi/marketplace.yaml").read_text())
app.state.roles = {
    operation["operationId"]: operation["x-roles"]
    for methods in spec["paths"].values()
    for operation in methods.values()
}
app.dependency_overrides[bearer_auth] = current_user
app.include_router(build_router(
    service=services, authorize=authorize,
    dependencies=[Depends(get_db, scope="function")],
))
app.openapi = lambda: spec


def mask(value):
    if isinstance(value, dict):
        return {key: "***" if any(secret in key.lower() for secret in ("password", "token", "secret")) else mask(item) for key, item in value.items()}
    if isinstance(value, list):
        return [mask(item) for item in value]
    return value


@app.middleware("http")
async def log_request(request: Request, call_next):
    request_id = str(uuid4())
    started = time.perf_counter()
    timestamp = datetime.now(timezone.utc).isoformat()
    body = None
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        raw = await request.body()
        try:
            body = mask(json.loads(raw)) if raw else None
            json.dumps(body, allow_nan=False)
        except (ValueError, UnicodeDecodeError, RecursionError):
            body = "<invalid JSON>"
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("Unhandled API error")
        response = JSONResponse(ApiError(500, "INTERNAL_ERROR", "Внутренняя ошибка").body(), status_code=500)
    response.headers["X-Request-Id"] = request_id
    entry = {
        "request_id": request_id, "method": request.method, "endpoint": request.url.path,
        "status_code": response.status_code, "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        "user_id": getattr(request.state, "user_id", None), "timestamp": timestamp,
    }
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        entry["body"] = body
    logger.info(json.dumps(entry, ensure_ascii=False))
    return response


@app.exception_handler(ApiError)
async def api_error(request, error):
    return JSONResponse(error.body(), status_code=error.status)


@app.exception_handler(RequestValidationError)
async def validation_error(request, error):
    fields = [{"field": ".".join(str(part) for part in item["loc"]), "message": item["msg"]} for item in error.errors()]
    return JSONResponse(ApiError(400, "VALIDATION_ERROR", "Невалидные входные данные", {"fields": fields}).body(), status_code=400)


@app.exception_handler(HTTPException)
async def http_error(request, error):
    if error.status_code == 400:
        return JSONResponse(ApiError(400, "VALIDATION_ERROR", "Не удалось разобрать запрос", {
            "fields": [{"field": "body", "message": str(error.detail)}],
        }).body(), status_code=400)
    return JSONResponse(ApiError(error.status_code, "HTTP_ERROR", str(error.detail)).body(), status_code=error.status_code)
