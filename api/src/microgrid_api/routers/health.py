"""Liveness (`/health`, no dependencies) and readiness (`/ready`, checks the database)."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text

router = APIRouter(tags=["health"])


class HealthOut(BaseModel):
    status: str = "ok"


class ReadyOut(BaseModel):
    ready: bool
    checks: dict[str, str]


def _probe_db(request: Request) -> None:
    with request.app.state.engine.connect() as conn:
        conn.execute(text("SELECT 1"))


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
    return ReadyOut(ready=True, checks={"db": "ok"})
