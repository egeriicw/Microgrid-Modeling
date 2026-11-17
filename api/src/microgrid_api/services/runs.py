"""Run lifecycle operations (FR-PLAT-2). Pure database logic; the worker and (later) the HTTP routers call these.

Cancellation design (decided in story 1.10): the cooperative cancel flag is the ``runs.cancel_requested`` column,
not a Redis key. Reasons: (1) the database is already the single source of truth for run state, so the flag
survives a worker restart, a Redis flush and a broker outage; (2) no second store to keep consistent; (3) the
worker polls it between engine runs with one tiny indexed read, which is negligible next to one ensemble run.
Cancel latency is therefore at most ONE engine run (the engine checks the flag before each run and never
interrupts a run in progress). Celery ``revoke`` is added by ``worker.cancel_run`` as a best-effort extra so a
task that is still waiting in the queue is dropped; it is never the only mechanism.

Trust boundary and Sprint 2 requirements for the HTTP layer (the `data` section of a snapshot holds filesystem
paths the engine reads, so whoever controls it controls which directories are read):
* NEVER take `data` (or any path, `data_dir`, `output_dir`) from the client request body.
* Build `data` server-side (``server_side_data_section``) from MICROGRID_DATA_DIR and the dataset catalog, and
  re-check that every resulting path is contained in the data root after symlink resolution.
* Store the server-built `data` in the snapshot (the snapshot alone must replay the run), and pass
  ``trusted_data_paths=True`` to ``create_run`` only for that server-built snapshot.
* Never let the client control `output_dir`; outputs go under MICROGRID_OUTPUT_DIR chosen by the server.
``create_run`` enforces the default: with ``trusted_data_paths=False`` any path key is rejected (key names only
are reported, never values).
"""

from __future__ import annotations

import copy
import uuid
from dataclasses import asdict, fields
from datetime import UTC, datetime
from typing import Any

from microgrid import __version__ as ENGINE_VERSION  # noqa: N812
from microgrid.community_load.synthetic.config import DataConfig, RNGConfig, config_from_dict
from microgrid.community_load.synthetic.rng import resolve_seeds
from sqlalchemy import update
from sqlalchemy.orm import Session

from microgrid_api.config import Settings
from microgrid_api.db.models import Run, RunStatus

SEED_KEYS = ("composition_seed", "archetype_seed", "perturbation_seed")
_PATH_SUFFIXES = ("_dir", "_dirs", "_file", "_path")
# Path-like fields of the engine's DataConfig, read from the engine rather than hard-coded.
DATA_PATH_KEYS = frozenset(f.name for f in fields(DataConfig) if f.name.endswith(_PATH_SUFFIXES))
RESULTS_FILENAME = "results.json"  # full results document, under the run's output.base_dir
FORBIDDEN_KEYS = frozenset({"data_dir", "data_dirs", "output_dir"})


def _path_keys_present(snapshot: dict[str, Any]) -> list[str]:
    found = {k for k in snapshot if k in FORBIDDEN_KEYS}
    data = snapshot.get("data")
    if isinstance(data, dict):
        found |= {k for k in data if k in DATA_PATH_KEYS or k in FORBIDDEN_KEYS}
    return sorted(found)


path_keys_present = _path_keys_present  # public alias for the HTTP layer


def server_side_data_section(settings: Settings) -> dict[str, str]:
    """Build the snapshot `data` paths from MICROGRID_DATA_DIR using the engine's default layout, verifying each
    stays under the data root after symlink resolution. Messages never include path values."""
    if settings.data_dir is None:
        raise ValueError("MICROGRID_DATA_DIR is not configured")
    root = settings.data_dir.resolve()
    layout = {
        "background_dir": "background",
        "resstock_timeseries_dir": "input/resstock/timeseries_individual_buildings",
        "comstock_timeseries_dir": "input/comstock/timeseries_individual_buildings",
    }
    out: dict[str, str] = {}
    for key, rel in layout.items():
        path = (root / rel).resolve()
        if not path.is_relative_to(root):
            raise ValueError(f"data path '{key}' resolves outside the data root")
        out[key] = str(path)
    return out


def _now() -> datetime:
    return datetime.now(UTC)


def _resolved_seeds(snapshot: dict[str, Any], seeds: dict[str, Any] | None) -> dict[str, int]:
    """Seeds from the snapshot's ``rng`` section, overridden by ``seeds``; unset ones are drawn fresh now."""
    merged: dict[str, Any] = dict(snapshot.get("rng") or {})
    merged.update(seeds or {})
    unknown = sorted(set(merged) - set(SEED_KEYS))
    if unknown:
        raise ValueError(f"unknown seed keys {unknown}; allowed: {list(SEED_KEYS)}")
    for key, value in merged.items():
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 2**32):
            raise ValueError(f"seed {key} must be an integer between 0 and {2**32 - 1}")
    resolved = asdict(resolve_seeds(RNGConfig(**merged)))
    return {k: int(resolved[k]) for k in SEED_KEYS}


def create_run(
    session: Session,
    composition_snapshot: dict[str, Any],
    seeds: dict[str, Any] | None = None,
    *,
    validate: bool = True,
    trusted_data_paths: bool = False,
    owner: str | None = None,
    run_id: uuid.UUID | None = None,
) -> Run:
    """Store a queued run. Unset seeds are resolved HERE, before execution, so the exact seeds are on the row and
    the stored snapshot alone replays the run. ``validate`` (default) checks the config with the engine's strict
    parser so a bad config fails at creation, not later in the worker. The caller's dict is not mutated.
    Path fields in the snapshot are rejected (keys only in the message) unless ``trusted_data_paths`` is True,
    which only server-side code that built them itself may pass."""
    if not trusted_data_paths:
        bad = _path_keys_present(composition_snapshot)
        if bad:
            raise ValueError(f"path fields are not allowed in a run snapshot from an untrusted source: {bad}")
    snapshot = copy.deepcopy(composition_snapshot)
    resolved = _resolved_seeds(snapshot, seeds)
    snapshot["rng"] = dict(resolved)
    if validate:
        config_from_dict(copy.deepcopy(snapshot), source="<run snapshot>")
    run = Run(
        id=run_id or uuid.uuid4(),
        composition_snapshot=snapshot,
        seeds=resolved,
        engine_version=ENGINE_VERSION,
        status=RunStatus.queued.value,
        progress={},
        owner=owner,
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
    """Set the cooperative cancel flag with two atomic statements (no read-then-write race with the worker claim).
    A still-queued run is cancelled immediately; a running run keeps ``running`` until the worker sees the flag
    and records ``cancelled``; a terminal run is unchanged. Unknown id raises LookupError."""
    cancelled = session.execute(
        update(Run)
        .where(Run.id == run_id, Run.status == RunStatus.queued.value)
        .values(status=RunStatus.cancelled.value, cancel_requested=True, finished_at=_now())
    )
    if getattr(cancelled, "rowcount", 0) == 0:
        session.execute(
            update(Run).where(Run.id == run_id, Run.status == RunStatus.running.value).values(cancel_requested=True)
        )
    session.commit()
    return get_run(session, run_id)
