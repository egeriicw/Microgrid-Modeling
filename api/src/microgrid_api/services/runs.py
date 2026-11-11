"""Run lifecycle operations (FR-PLAT-2). Pure database logic; the worker and (later) the HTTP routers call these.

Cancellation design (decided in story 1.10): the cooperative cancel flag is the ``runs.cancel_requested`` column,
not a Redis key. Reasons: (1) the database is already the single source of truth for run state, so the flag
survives a worker restart, a Redis flush and a broker outage; (2) no second store to keep consistent; (3) the
worker polls it between engine runs with one tiny indexed read, which is negligible next to one ensemble run.
Cancel latency is therefore at most ONE engine run (the engine checks the flag before each run and never
interrupts a run in progress). Celery ``revoke`` is added by ``worker.cancel_run`` as a best-effort extra so a
task that is still waiting in the queue is dropped; it is never the only mechanism.
"""

from __future__ import annotations

import copy
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from microgrid import __version__ as ENGINE_VERSION  # noqa: N812
from microgrid.community_load.synthetic.config import RNGConfig, config_from_dict
from microgrid.community_load.synthetic.rng import resolve_seeds
from microgrid_api.db.models import Run, RunStatus

SEED_KEYS = ("composition_seed", "archetype_seed", "perturbation_seed")


def _now() -> datetime:
    return datetime.now(UTC)


def _resolved_seeds(snapshot: dict[str, Any], seeds: dict[str, Any] | None) -> dict[str, int]:
    """Seeds from the snapshot's ``rng`` section, overridden by ``seeds``; unset ones are drawn fresh now."""
    merged: dict[str, Any] = dict(snapshot.get("rng") or {})
    merged.update(seeds or {})
    unknown = sorted(set(merged) - set(SEED_KEYS))
    if unknown:
        raise ValueError(f"unknown seed keys {unknown}; allowed: {list(SEED_KEYS)}")
    resolved = asdict(resolve_seeds(RNGConfig(**merged)))
    return {k: int(resolved[k]) for k in SEED_KEYS}


def create_run(
    session: Session,
    composition_snapshot: dict[str, Any],
    seeds: dict[str, Any] | None = None,
    *,
    validate: bool = True,
) -> Run:
    """Store a queued run. Unset seeds are resolved HERE, before execution, so the exact seeds are on the row and
    the stored snapshot alone replays the run. ``validate`` (default) checks the config with the engine's strict
    parser so a bad config fails at creation, not later in the worker. The caller's dict is not mutated."""
    snapshot = copy.deepcopy(composition_snapshot)
    resolved = _resolved_seeds(snapshot, seeds)
    snapshot["rng"] = dict(resolved)
    if validate:
        config_from_dict(copy.deepcopy(snapshot), source="<run snapshot>")
    run = Run(
        id=uuid.uuid4(),
        composition_snapshot=snapshot,
        seeds=resolved,
        engine_version=ENGINE_VERSION,
        status=RunStatus.queued.value,
        progress={},
    )
    session.add(run)
    session.commit()
    return run


def get_run(session: Session, run_id: uuid.UUID) -> Run:
    """Return the run or raise LookupError (routers map it to 404)."""
    session.expire_all()  # always read committed state, never a stale identity-map copy
    run = session.get(Run, run_id)
    if run is None:
        raise LookupError(f"run {run_id} not found")
    return run


def request_cancel(session: Session, run_id: uuid.UUID) -> Run:
    """Set the cooperative cancel flag. A still-queued run is cancelled immediately (it never started); a running
    run keeps ``running`` until the worker sees the flag and records ``cancelled``; a finished run is unchanged."""
    run = get_run(session, run_id)
    if run.status == RunStatus.queued.value:
        run.cancel_requested = True
        run.status = RunStatus.cancelled.value
        run.finished_at = _now()
    elif run.status == RunStatus.running.value:
        run.cancel_requested = True
    session.commit()
    return run
