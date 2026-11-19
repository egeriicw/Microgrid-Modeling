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
from fastapi.responses import JSONResponse, Response
from microgrid.community_load.permissions import FilterPermissionError, can, validate_filter_edit
from microgrid.community_load.synthetic.config import config_from_dict
from pydantic import BaseModel, ConfigDict, StrictInt, model_validator
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

from microgrid_api.auth import Principal, current_principal
from microgrid_api.composition_schemas import CompositionRequest
from microgrid_api.config import Settings
from microgrid_api.db.models import Run, RunEvent, RunStatus
from microgrid_api.filter_schemas import MAX_CATEGORIES, FilterSelections, selections_to_plain
from microgrid_api.services import compositions as comp_svc
from microgrid_api.services import exposed_attributes as ea_svc
from microgrid_api.services import runs as svc

logger = logging.getLogger("microgrid_api.runs")
router = APIRouter(tags=["runs"])

TERMINAL = frozenset({RunStatus.succeeded.value, RunStatus.failed.value, RunStatus.cancelled.value})
RunStatusName = Literal["queued", "running", "succeeded", "failed", "cancelled"]


class RunCreate(BaseModel):
    """Exactly one of ``config`` (a scenario WITHOUT a `data` section), ``composition_id`` (a predefined one) or
    ``composition`` (building types with counts or fraction ranges; the server builds the scenario)."""

    model_config = ConfigDict(extra="forbid")

    config: dict[str, Any] | None = None
    composition_id: str | None = None
    composition: CompositionRequest | None = None
    seeds: dict[str, StrictInt] | None = None
    # FR-ENG-11: attribute filters by exposed-attribute key, per composition category. Needs the filters.use flag.
    # Raw column-level `attributes` inside `config.archetype_filters` are refused; these selections are turned into
    # engine filters by the server with the ACTIVE exposed-attribute configuration.
    filter_selections: FilterSelections | None = None

    @model_validator(mode="after")
    def _one_source(self) -> RunCreate:
        given = [x for x in (self.config, self.composition_id, self.composition) if x is not None]
        if len(given) != 1:
            raise ValueError("send exactly one of 'config', 'composition_id' or 'composition'")
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


class RunTemperatureFile(BaseModel):
    """The temperature source the run declared (FR-ENG-10). ``source`` is None for runs stored before it was
    recorded."""

    source: str | None
    file: str | None  # file NAME only, never a path
    column: str | None
    time_column: str | None
    units: str | None


class RunDataFiles(BaseModel):
    """Which data files the run read, as file NAMES only (FR-DATA-8 provenance; server paths are never shown).
    None for runs stored before the names were recorded."""

    resstock_characteristics_file: str | None
    comstock_characteristics_file: str | None
    temperature: RunTemperatureFile


class RunDetail(RunOut):
    composition_snapshot: dict[str, Any]
    data_files: RunDataFiles
    result: dict[str, Any] | None
    # FR-ENG-11 e: the column-level filters the engine applied ("<source>/<category>" -> filters), the user's
    # selections with the exposed-attribute configuration version used, and the matched pool sizes (from the result;
    # None until the worker has built the pool).
    effective_filters: dict[str, list[dict[str, Any]]]
    filter_selections: dict[str, Any] | None
    pool_summary: dict[str, Any] | None


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
    snapshot = svc.public_snapshot(run.composition_snapshot)  # never expose server paths
    # the engine's copy of the config inside the result holds them too
    result = svc.public_results_document(copy.deepcopy(run.result)) if run.result is not None else None
    pool = result.get("pool_summary") if result is not None else None
    base = _out(run).model_dump()
    return RunDetail(
        **base,
        composition_snapshot=snapshot,
        data_files=RunDataFiles.model_validate(svc.data_files_provenance(run.composition_snapshot)),
        result=result,
        effective_filters=ea_svc.effective_filters(run.composition_snapshot),
        filter_selections=run.filter_selections,
        pool_summary=pool if isinstance(pool, dict) else None,
    )


MAX_CONFIG_DEPTH = 32


def config_too_deep(obj: Any, limit: int = MAX_CONFIG_DEPTH) -> bool:
    """True when dicts/lists nest more than ``limit`` levels (iterative: no recursion on hostile input)."""
    stack: list[tuple[Any, int]] = [(obj, 1)]
    while stack:
        node, depth = stack.pop()
        if isinstance(node, dict):
            children: Any = node.values()
        elif isinstance(node, list):
            children = node
        else:
            continue
        if depth > limit:
            return True
        stack.extend((child, depth + 1) for child in children)
    return False


def check_config_shape(config: dict[str, Any]) -> None:
    """Size limits on a client config, checked BEFORE it is copied or parsed: nesting depth and category counts."""
    if config_too_deep(config):
        raise HTTPException(422, f"the config is nested more than {MAX_CONFIG_DEPTH} levels deep")
    composition = config.get("composition")
    categories = composition.get("categories") if isinstance(composition, dict) else None
    if isinstance(categories, dict) and len(categories) > MAX_CATEGORIES:
        raise HTTPException(422, f"composition.categories has {len(categories)} entries; the limit is {MAX_CATEGORIES}")
    af = config.get("archetype_filters")
    for source, cats in af.items() if isinstance(af, dict) else ():
        if isinstance(cats, dict) and len(cats) > MAX_CATEGORIES:
            raise HTTPException(
                422, f"archetype_filters.{source} has {len(cats)} entries; the limit is {MAX_CATEGORIES}"
            )


def selection_http_error(exc: ValueError, principal: Principal) -> HTTPException:
    """422 for a selection/filter problem. Admins see the library's detail; other roles see the public message,
    which names only exposed attributes and their own categories (never columns or data values)."""
    if isinstance(exc, ea_svc.SelectionError) and principal.role != "admin":
        logger.info("selection refused for %s: %r", principal.subject, str(exc))  # %r: no raw newlines in logs
        return HTTPException(422, exc.public)
    return HTTPException(422, str(exc))


def check_pools(settings: Settings, parsed: Any, exposed: Any, principal: Principal) -> dict[str, Any]:
    """Pool preview on a parsed, server-built snapshot (FR-ENG-11 c/d on the run path). Refuses (422) a filter that
    does not fit the data and a category that may receive buildings but matches none, so a run never fails later
    with only an exception class name. Returns the previews."""
    assert settings.data_dir is not None  # the server-built data section exists
    try:
        previews = ea_svc.preview(parsed, settings.data_dir, exposed)
    except ea_svc.DataUnavailable as exc:
        raise HTTPException(503, str(exc)) from None
    except KeyError as exc:
        logger.error("pool check: characteristics data lacks column %s", exc)
        raise HTTPException(503, "the server's characteristics data lacks a column this category needs") from None
    except (ValueError, OverflowError) as exc:
        raise selection_http_error(exc if isinstance(exc, ValueError) else ValueError(str(exc)), principal) from None
    empty = ea_svc.unsampleable_categories(parsed, previews)
    if empty:
        raise HTTPException(
            422,
            f"no buildings match the filters of category {', '.join(repr(n) for n in empty)}, so it cannot be "
            "sampled; preview the pool and loosen the filters, or set its max_fraction to 0",
        )
    return previews


def check_client_config(config: dict[str, Any]) -> None:
    """Refuse anything in a client config the server must own: paths, the data section, raw column filters."""
    check_config_shape(config)
    if "data" in config:
        raise HTTPException(
            422, "the 'data' section is built by the server and must not be sent; remove it from the config"
        )
    if isinstance(config.get("output"), dict) and "base_dir" in config["output"]:
        raise HTTPException(422, "output.base_dir is set by the server and must not be sent; remove it")
    bad = svc.path_keys_present(config)
    if bad:
        raise HTTPException(422, f"path fields are not allowed in a client config: {bad}")
    raw = ea_svc.raw_attribute_filter_locations(config)
    if raw:
        raise HTTPException(
            422,
            f"raw attribute filters (column names) are not accepted: {raw}; send exposed-attribute selections in "
            "'filter_selections' instead",
        )


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
        svc.require_data_files(data)
    except svc.DataSectionError as exc:
        logger.error("cannot build the data section: %s", exc)  # setting/key names only, never values
        raise HTTPException(503, exc.public) from None
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


def _check_caps(settings: Settings, snapshot: dict[str, Any]) -> Any:
    """Parse the snapshot with the engine's strict parser, enforce the server caps, and return the parsed config."""
    try:
        parsed = config_from_dict(copy.deepcopy(snapshot), source="<run config>")
    except (ValueError, OverflowError) as exc:
        raise HTTPException(422, str(exc)) from None
    if parsed.composition.total_buildings > settings.max_total_buildings:
        raise HTTPException(
            422, f"composition.total_buildings exceeds the server cap of {settings.max_total_buildings}"
        )
    if parsed.ensemble.max_runs > settings.max_ensemble_runs:
        raise HTTPException(422, f"ensemble.max_runs exceeds the server cap of {settings.max_ensemble_runs}")
    return parsed


def run_inputs(role: str) -> list[str]:
    """Which of the `POST /runs` inputs a role may send (the rule `create_run` enforces; shown by `GET /me`)."""
    return ["composition_id"] if role == "demo" else ["composition", "composition_id", "config"]


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
    overrides = settings.permission_overrides
    demo = principal.role == "demo"
    sent = [k for k in ("config", "composition_id", "composition") if getattr(body, k) is not None]
    if any(k not in run_inputs(principal.role) for k in sent) or (demo and body.seeds is not None):
        raise HTTPException(403, "the demo role may only run a predefined composition with its fixed seed")
    if demo and body.filter_selections is not None:  # even only empty lists: demo never uses filters
        raise HTTPException(403, "role 'demo' does not have the filters.use permission")
    saved: dict[str, Any] | None = None  # the stored composition, when one is being run (validate_filter_edit)
    if body.composition_id is not None:
        library: dict[str, dict[str, Any]] = request.app.state.demo_compositions
        if body.composition_id not in library:
            raise HTTPException(404, "unknown composition_id")
        saved = library[body.composition_id]
        config = copy.deepcopy(saved)
    elif body.composition is not None:
        try:
            config = comp_svc.build_scenario(body.composition.to_composition(), request.app.state.building_registry)
        except comp_svc.CompositionError as exc:
            raise HTTPException(422, str(exc)) from None
        check_client_config(config)  # the built scenario obeys the same rules as a client config (defence in depth)
    else:
        assert body.config is not None
        check_config_shape(body.config)  # depth and size before anything copies or walks it
        config = copy.deepcopy(body.config)
        check_client_config(config)
    uses_filters = body.filter_selections is not None and any(body.filter_selections.values())
    # Permission first: a caller without the flag gets the same 403 whatever the selections say (no validation
    # detail about categories or attributes before the refusal).
    if uses_filters and not can(principal.role, "filters.use", overrides):
        raise HTTPException(403, f"role {principal.role!r} does not have the filters.use permission")
    if body.filter_selections is not None:  # every named category must exist, even with an empty list
        try:
            ea_svc.check_selection_categories(config, body.filter_selections)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
    provenance: dict[str, Any] | None = None
    active: ea_svc.ActiveConfiguration | None = None
    if uses_filters:
        assert body.filter_selections is not None
        try:
            active = ea_svc.active_configuration(session)
        except ea_svc.InvalidStoredConfiguration as exc:
            raise HTTPException(503, str(exc)) from None
        plain = selections_to_plain(body.filter_selections)
        try:
            config = ea_svc.apply_selections(config, plain, active.exposed)
        except ValueError as exc:
            raise selection_http_error(exc, principal) from None
        provenance = {"exposed_attributes_version": active.version, "categories": plain}
    if demo:  # fixed stored seed unless the predefined composition pins its own
        pinned = dict(config.get("rng") or {})
        config["rng"] = {k: pinned.get(k, settings.demo_seed) for k in svc.SEED_KEYS}
    run_id = uuid.uuid4()
    snapshot = _build_snapshot(settings, config, run_id)
    parsed = _check_caps(settings, snapshot)
    if active is not None:  # selections were applied: refuse now what would only fail later in the worker
        check_pools(settings, parsed, active.exposed, principal)
    try:
        # permission flag filters.use: adding, changing or removing filters relative to the stored composition
        validate_filter_edit(principal.role, new=snapshot, saved=saved, overrides=overrides)
        run = svc.create_run(
            session,
            snapshot,
            body.seeds,
            trusted_data_paths=True,
            owner=principal.subject,
            run_id=run_id,
            filter_selections=provenance,
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
    summary="Full results document (JSON written by the worker; server paths removed)",
    response_class=JSONResponse,
)
def get_results(
    run_id: uuid.UUID,
    request: Request,
    principal: Principal = Depends(current_principal),
    session: Session = Depends(get_session),
) -> Response:
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
    try:
        doc = json.loads(path.read_text())
    except (OSError, ValueError):
        logger.error("results file for run %s is unreadable", run_id)
        raise HTTPException(404, "results not available") from None
    if not isinstance(doc, dict):
        raise HTTPException(404, "results not available")
    # Filtered on the way out too, so files written before the worker stripped paths never leak them.
    # json.dumps as the worker wrote it (Starlette's JSONResponse would refuse a NaN the engine may have produced).
    return Response(json.dumps(svc.public_results_document(doc)), media_type="application/json")


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
