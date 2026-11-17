"""Liveness (`/health`, no dependencies) and readiness (`/ready`).

`/ready` checks that the database answers and that the Alembic head revision is applied, and (only when
MICROGRID_REDIS_URL is configured) that Redis answers PING. Failures report the exception class only, never a
message, URL or credentials. Storage checks arrive later.
"""

from __future__ import annotations

import redis
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text

from microgrid_api.db.migrate import head_revision

router = APIRouter(tags=["health"])


class HealthOut(BaseModel):
    status: str = "ok"


class ReadyOut(BaseModel):
    ready: bool
    checks: dict[str, str]


def _probe_db(request: Request) -> None:
    with request.app.state.engine.connect() as conn:
        conn.execute(text("SELECT 1"))


def _redis_client(url: str) -> redis.Redis:
    # Short timeouts so a dead broker cannot hang the readiness probe.
    return redis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)


def _probe_redis(url: str) -> None:
    client = _redis_client(url)
    try:
        client.ping()
    finally:
        client.close()


def _probe_schema(request: Request) -> bool:
    """True when the database's alembic_version row equals the packaged head revision."""
    with request.app.state.engine.connect() as conn:
        try:
            rows = conn.execute(text("SELECT version_num FROM alembic_version")).scalars().all()
        except Exception:  # table missing: never migrated
            return False
    return rows == [head_revision()]


@router.get("/health", response_model=HealthOut, summary="Liveness")
def health() -> HealthOut:
    return HealthOut()


@router.get("/ready", response_model=ReadyOut, responses={503: {"model": ReadyOut}}, summary="Readiness")
def ready(request: Request) -> ReadyOut | JSONResponse:
    try:
        _probe_db(request)
    except Exception as exc:  # any driver error means "not ready"; report class only, never the URL/credentials
        out = ReadyOut(ready=False, checks={"db": f"unavailable: {type(exc).__name__}"})
        return JSONResponse(status_code=503, content=out.model_dump())
    try:
        migrated = _probe_schema(request)
    except Exception as exc:
        out = ReadyOut(ready=False, checks={"db": "ok", "schema": f"unavailable: {type(exc).__name__}"})
        return JSONResponse(status_code=503, content=out.model_dump())
    if not migrated:
        out = ReadyOut(
            ready=False, checks={"db": "ok", "schema": "not migrated: run `python -m microgrid_api.db.migrate`"}
        )
        return JSONResponse(status_code=503, content=out.model_dump())
    checks = {"db": "ok", "schema": "ok"}
    redis_url = request.app.state.settings.redis_url
    if redis_url:
        try:
            _probe_redis(redis_url)
        except Exception as exc:  # report the class only: redis error text can include the host or password
            checks["redis"] = f"unavailable: {type(exc).__name__}"
            return JSONResponse(status_code=503, content=ReadyOut(ready=False, checks=checks).model_dump())
        checks["redis"] = "ok"
    return ReadyOut(ready=True, checks=checks)
