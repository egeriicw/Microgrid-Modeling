"""Run endpoints (FR-WEB-2, FR-PLAT): create, list, detail, SSE progress, cancel, plus the demo composition list.

Trust boundary (see services/runs.py): the client never supplies the `data` section or any path. The server builds it
from MICROGRID_DATA_DIR (symlink-contained) and stores it in the snapshot; API responses omit it so server paths do not
leak. Users see only their own runs; admins see all. A run that belongs to someone else answers 404, not 403, so ids
cannot be probed.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import uuid
from collections.abc import AsyncIterator, Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import anyio.to_thread
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse
from microgrid.community_load.permissions import FilterPermissionError, validate_filters_allowed
from microgrid.community_load.synthetic.config import config_from_dict
from pydantic import BaseModel, ConfigDict, StrictInt, model_validator
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

from microgrid_api.auth import Principal, current_principal
from microgrid_api.config import Settings
from microgrid_api.db.models import Run, RunEvent, RunStatus
from microgrid_api.services import runs as svc

logger = logging.getLogger("microgrid_api.runs")
router = APIRouter(tags=["runs"])

TERMINAL = frozenset({RunStatus.succeeded.value, RunStatus.failed.value, RunStatus.cancelled.value})
RunStatusName = Literal["queued", "running", "succeeded", "failed", "cancelled"]


class RunCreate(BaseModel):
    """Exactly one of ``config`` (a scenario WITHOUT a `data` section) or ``composition_id`` (a predefined one)."""

    model_config = ConfigDict(extra="forbid")

    config: dict[str, Any] | None = None
    composition_id: str | None = None
    seeds: dict[str, StrictInt] | None = None

    @model_validator(mode="after")
    def _one_source(self) -> RunCreate:
        if (self.config is None) == (self.composition_id is None):
            raise ValueError("send exactly one of 'config' or 'composition_id'")
        for key, value in (self.seeds or {}).items():
            if not 0 <= value < 2**32:
                raise ValueError(f"seed {key} must be between 0 and {2**32 - 1}")
        return self


class RunOut(BaseModel):
    id: uuid.UUID
    status: RunStatusName
    owner: str | None
    seeds: dict[str, Any]
    engine_version: str
    progress: dict[str, Any]
    cancel_requested: bool
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class RunDetail(RunOut):
    composition_snapshot: dict[str, Any]
    result: dict[str, Any] | None


class RunList(BaseModel):
    items: list[RunOut]
    total: int
    limit: int
    offset: int


class DemoCompositions(BaseModel):
    items: list[str]


def get_session(request: Request) -> Any:
    with request.app.state.session_factory() as session:
        yield session


def _out(run: Run) -> RunOut:
    return RunOut.model_validate(run, from_attributes=True)


def _detail(run: Run) -> RunDetail:
    snapshot = {k: v for k, v in run.composition_snapshot.items() if k != "data"}  # never expose server paths
    if isinstance(snapshot.get("output"), dict):
        snapshot["output"] = {k: v for k, v in snapshot["output"].items() if k != "base_dir"}
    base = _out(run).model_dump()
    return RunDetail(**base, composition_snapshot=snapshot, result=run.result)


def _visible_run(session: Session, principal: Principal, run_id: uuid.UUID) -> Run:
    try:
        run = svc.get_run(session, run_id)
    except LookupError:
        raise HTTPException(404, "run not found") from None
    if principal.role != "admin" and run.owner != principal.subject:
        raise HTTPException(404, "run not found")
    return run


def _build_snapshot(settings: Settings, config: dict[str, Any], run_id: uuid.UUID) -> dict[str, Any]:
    """Server-built `data` section and per-run `output.base_dir` (<MICROGRID_OUTPUT_DIR>/<run id>)."""
    try:
        data: dict[str, str] = svc.server_side_data_section(settings)
    except ValueError:
        logger.error("cannot build the data section: data directory missing or outside its root")
        raise HTTPException(503, "the server data directory is not available") from None
    if settings.output_dir is None:
        logger.error("MICROGRID_OUTPUT_DIR is not configured")
        raise HTTPException(503, "the server output directory is not available")
    snapshot = {**copy.deepcopy(config), "data": data}
    output = snapshot.get("output")
    if output is None:
        output = {}
    if not isinstance(output, dict):
        raise HTTPException(422, "'output' must be a mapping")
    snapshot["output"] = {**output, "base_dir": str(settings.output_dir.resolve() / str(run_id))}
    return snapshot


def _check_caps(settings: Settings, snapshot: dict[str, Any]) -> None:
    try:
        parsed = config_from_dict(copy.deepcopy(snapshot), source="<run config>")
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    if parsed.composition.total_buildings > settings.max_total_buildings:
        raise HTTPException(
            422, f"composition.total_buildings exceeds the server cap of {settings.max_total_buildings}"
        )
    if parsed.ensemble.max_runs > settings.max_ensemble_runs:
        raise HTTPException(422, f"ensemble.max_runs exceeds the server cap of {settings.max_ensemble_runs}")


@router.get("/demo-compositions", response_model=DemoCompositions, summary="Predefined compositions")
def demo_compositions(request: Request, _: Principal = Depends(current_principal)) -> DemoCompositions:
    return DemoCompositions(items=sorted(request.app.state.demo_compositions))


@router.post("/runs", response_model=RunOut, status_code=202, summary="Create and queue a run")
def create_run(
    body: RunCreate,
    request: Request,
    principal: Principal = Depends(current_principal),
    session: Session = Depends(get_session),
) -> RunOut:
    settings: Settings = request.app.state.settings
    demo = principal.role == "demo"
    if demo and (body.config is not None or body.seeds is not None):
        raise HTTPException(403, "the demo role may only run a predefined composition with its fixed seed")
    if body.composition_id is not None:
        library: dict[str, dict[str, Any]] = request.app.state.demo_compositions
        if body.composition_id not in library:
            raise HTTPException(404, "unknown composition_id")
        config = copy.deepcopy(library[body.composition_id])
    else:
        assert body.config is not None
        config = copy.deepcopy(body.config)
        if "data" in config:
            raise HTTPException(
                422, "the 'data' section is built by the server and must not be sent; remove it from the config"
            )
        if isinstance(config.get("output"), dict) and "base_dir" in config["output"]:
            raise HTTPException(422, "output.base_dir is set by the server and must not be sent; remove it")
        bad = svc.path_keys_present(config)
        if bad:
            raise HTTPException(422, f"path fields are not allowed in a client config: {bad}")
    if demo:  # fixed stored seed unless the predefined composition pins its own
        pinned = dict(config.get("rng") or {})
        config["rng"] = {k: pinned.get(k, settings.demo_seed) for k in svc.SEED_KEYS}
    run_id = uuid.uuid4()
    snapshot = _build_snapshot(settings, config, run_id)
    _check_caps(settings, snapshot)
    try:
        validate_filters_allowed(principal.role, snapshot)  # permission flag filters.use
        run = svc.create_run(
            session, snapshot, body.seeds, trusted_data_paths=True, owner=principal.subject, run_id=run_id
        )
    except FilterPermissionError as exc:
        raise HTTPException(403, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    try:
        request.app.state.enqueue(run.id)
    except Exception as exc:  # broker down: the run can never start, so do not leave it queued
        logger.error("enqueue failed for run %s (%s)", run.id, type(exc).__name__)
        run.status = RunStatus.failed.value
        run.error_message = "could not queue the run (queue unavailable)"
        session.commit()
        raise HTTPException(503, "the run queue is unavailable; try again later") from None
    return _out(run)


@router.get("/runs", response_model=RunList, summary="List runs (own runs; admin sees all)")
def list_runs(
    principal: Principal = Depends(current_principal),
    session: Session = Depends(get_session),
    status: RunStatusName | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=1_000_000),
) -> RunList:
    where = []
    if principal.role != "admin":
        where.append(Run.owner == principal.subject)
    if status is not None:
        where.append(Run.status == status)
    total = session.scalar(select(func.count()).select_from(Run).where(*where)) or 0
    rows = session.scalars(
        select(Run).where(*where).order_by(Run.created_at.desc(), Run.id).limit(limit).offset(offset)
    ).all()
    return RunList(items=[_out(r) for r in rows], total=total, limit=limit, offset=offset)


@router.get("/runs/{run_id}", response_model=RunDetail, summary="Run detail including the result")
def get_run(
    run_id: uuid.UUID, principal: Principal = Depends(current_principal), session: Session = Depends(get_session)
) -> RunDetail:
    return _detail(_visible_run(session, principal, run_id))


@router.post("/runs/{run_id}/cancel", response_model=RunOut, summary="Request cancellation")
def cancel(
    run_id: uuid.UUID,
    request: Request,
    principal: Principal = Depends(current_principal),
    session: Session = Depends(get_session),
) -> RunOut:
    _visible_run(session, principal, run_id)
    return _out(request.app.state.cancel_run(session, run_id))


@router.get(
    "/runs/{run_id}/results",
    summary="Full results document (JSON file written by the worker)",
    response_class=FileResponse,
)
def get_results(
    run_id: uuid.UUID,
    request: Request,
    principal: Principal = Depends(current_principal),
    session: Session = Depends(get_session),
) -> FileResponse:
    run = _visible_run(session, principal, run_id)
    settings: Settings = request.app.state.settings
    if run.status != RunStatus.succeeded.value or settings.output_dir is None:
        raise HTTPException(404, "results not available")
    base = (run.composition_snapshot.get("output") or {}).get("base_dir")
    if not isinstance(base, str):
        raise HTTPException(404, "results not available")
    path = (Path(base) / svc.RESULTS_FILENAME).resolve()
    if not path.is_relative_to(settings.output_dir.resolve()) or not path.is_file():
        logger.warning("results file for run %s missing or outside the output root", run_id)
        raise HTTPException(404, "results not available")
    return FileResponse(path, media_type="application/json")


def _poll(factory: Callable[[], Session], run_id: uuid.UUID, after: int) -> tuple[str, list[dict[str, Any]]]:
    """Status first, then events: events are written before the terminal status, so a terminal status read first
    guarantees the event read after it is complete."""
    with factory() as session:
        status = session.scalar(select(Run.status).where(Run.id == run_id)) or RunStatus.failed.value
        rows = session.scalars(
            select(RunEvent).where(RunEvent.run_id == run_id, RunEvent.id > after).order_by(RunEvent.id)
        ).all()
        events = [
            {
                "id": r.id,
                "data": {"stage": r.stage, "message": r.message, "payload": r.payload, "ts": r.ts.isoformat()},
            }
            for r in rows
        ]
    return status, events


@router.get("/runs/{run_id}/events", summary="Server-sent progress events (resume with Last-Event-ID)")
async def run_events(
    run_id: uuid.UUID,
    request: Request,
    principal: Principal = Depends(current_principal),
    last_event_id: str | None = Header(default=None),
) -> EventSourceResponse:
    factory = request.app.state.session_factory

    def check() -> None:
        with factory() as session:
            _visible_run(session, principal, run_id)

    await anyio.to_thread.run_sync(check)
    try:
        after = int(last_event_id) if last_event_id else 0
        if not 0 <= after <= 2**31 - 1:
            raise ValueError
    except ValueError:
        raise HTTPException(422, "Last-Event-ID must be an integer event id") from None
    interval = request.app.state.settings.sse_poll_interval_s

    async def stream() -> AsyncIterator[dict[str, str]]:
        cursor = after
        while True:
            status, events = await anyio.to_thread.run_sync(_poll, factory, run_id, cursor)
            for ev in events:
                cursor = ev["id"]
                yield {"event": "progress", "id": str(ev["id"]), "data": json.dumps(ev["data"])}
            if status in TERMINAL and not events:
                yield {"event": "status", "data": json.dumps({"status": status})}
                return
            await asyncio.sleep(interval)

    return EventSourceResponse(stream(), ping=15)
