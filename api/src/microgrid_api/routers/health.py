"""Liveness (`/health`, no dependencies) and readiness (`/ready`).

`/ready` checks that the database answers and that the Alembic head revision is applied. Redis and storage checks
arrive with story 1.10.
"""

from __future__ import annotations

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
    return ReadyOut(ready=True, checks={"db": "ok", "schema": "ok"})
