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
from pathlib import Path
from typing import Any

from microgrid import __version__ as ENGINE_VERSION  # noqa: N812
from microgrid.community_load.synthetic.config import DataConfig, RNGConfig, config_from_dict
from microgrid.community_load.synthetic.rng import resolve_seeds
from sqlalchemy import update
from sqlalchemy.orm import Session

from microgrid_api.config import TEMPERATURE_OPTIONAL_FIELDS, Settings, is_plain_file_name, is_relative_data_path
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


class DataSectionError(ValueError):
    """The server cannot build a usable `data` section. ``public`` is safe to show any caller (no paths, no file
    names); ``str(exc)`` adds the setting involved, for the server log."""

    def __init__(self, detail: str, public: str = "the server data directory is not available") -> None:
        super().__init__(detail)
        self.public = public


def _contained(root: Path, rel: str, key: str) -> Path:
    path = (root / rel).resolve()
    if not path.is_relative_to(root):
        raise DataSectionError(f"data path '{key}' resolves outside the data root")
    return path


def server_side_data_section(settings: Settings) -> dict[str, str]:
    """Build the snapshot `data` section from settings: the directories under MICROGRID_DATA_DIR (the engine's default
    layout), the characteristics file NAMES (always written, so the engine's defaults are never relied on) and the
    declared temperature source. Every path, including each characteristics file and the weather file, must stay
    under the data root after symlink resolution. Existence is checked separately (``require_data_files``) so this
    stays usable for describing the section. Messages never include path values."""
    if settings.data_dir is None:
        raise DataSectionError("MICROGRID_DATA_DIR is not configured")
    root = settings.data_dir.resolve()
    # (tests/test_deploy_wave1.py reads this literal from the source to check the CI fixture layout)
    layout = {
        "background_dir": "background",
        "resstock_timeseries_dir": "input/resstock/timeseries_individual_buildings",
        "comstock_timeseries_dir": "input/comstock/timeseries_individual_buildings",
    }
    out: dict[str, str] = {key: str(_contained(root, rel, key)) for key, rel in layout.items()}
    for key in ("resstock_chars_file", "comstock_chars_file"):
        name: str = getattr(settings, key)
        if not is_plain_file_name(name):  # Settings already refuses this; checked again where the path is built
            raise DataSectionError(f"data file '{key}' is not a plain file name")
        _contained(root, f"{layout['background_dir']}/{name}", key)  # a symlinked file must not escape either
        out[key] = name  # the engine joins it to background_dir
    out["temperature_source"] = settings.temperature_source
    for key in TEMPERATURE_OPTIONAL_FIELDS:
        value = getattr(settings, key)
        if value is None:
            continue
        if key == "temperature_weather_file":
            if not is_relative_data_path(value):  # Settings already refuses this; checked again here
                raise DataSectionError(f"data file '{key}' is not a path relative to the data root")
            value = str(_contained(root, value, key))
        out[key] = value
    return out


_FILE_LABELS = {
    "resstock_chars_file": "ResStock characteristics file",
    "comstock_chars_file": "ComStock characteristics file",
    "temperature_weather_file": "weather file",
}


def require_data_files(data: dict[str, str]) -> None:
    """Every file a server-built section names must exist now, so a missing file is a clear error at run creation,
    not a late worker failure. DataSectionError names only the kind of file (public) and the key (log)."""
    files = {
        "resstock_chars_file": Path(data["background_dir"]) / data["resstock_chars_file"],
        "comstock_chars_file": Path(data["background_dir"]) / data["comstock_chars_file"],
    }
    if "temperature_weather_file" in data:
        files["temperature_weather_file"] = Path(data["temperature_weather_file"])
    for key, path in files.items():
        if not path.is_file():
            raise DataSectionError(
                f"data file '{key}' does not exist", public=f"the server's {_FILE_LABELS[key]} is not available"
            )


def data_files_provenance(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Which data files a stored run read, as file NAMES only (never paths), from its snapshot's `data` section.
    Runs stored before the names were recorded report None (the engine defaults of that time were used)."""
    data = snapshot.get("data")
    data = data if isinstance(data, dict) else {}

    def name(key: str) -> str | None:
        value = data.get(key)
        return Path(value).name if isinstance(value, str) and value else None

    def text(key: str) -> str | None:
        value = data.get(key)
        return value if isinstance(value, str) else None

    return {
        "resstock_characteristics_file": name("resstock_chars_file"),
        "comstock_characteristics_file": name("comstock_chars_file"),
        "temperature": {
            "source": text("temperature_source"),
            "file": name("temperature_weather_file"),
            "column": text("temperature_column"),
            "time_column": text("temperature_time_column"),
            "units": text("temperature_units"),
        },
    }


def public_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """A copy of a run snapshot without server paths (`data`, `output.base_dir`), for anything a caller can read."""
    out = {k: copy.deepcopy(v) for k, v in snapshot.items() if k != "data"}
    if isinstance(out.get("output"), dict):
        out["output"] = {k: v for k, v in out["output"].items() if k != "base_dir"}
    return out


def public_results_document(doc: dict[str, Any]) -> dict[str, Any]:
    """A results document (``RunResult.to_dict``) whose ``config_snapshot`` carries no server paths. Used both when
    the worker writes ``results.json`` and when the API serves one (files written before this existed)."""
    out = dict(doc)
    if isinstance(out.get("config_snapshot"), dict):
        out["config_snapshot"] = public_snapshot(out["config_snapshot"])
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
    filter_selections: dict[str, Any] | None = None,
) -> Run:
    """Store a queued run. Unset seeds are resolved HERE, before execution, so the exact seeds are on the row and
    the stored snapshot alone replays the run. ``validate`` (default) checks the config with the engine's strict
    parser so a bad config fails at creation, not later in the worker. The caller's dict is not mutated.
    Path fields in the snapshot are rejected (keys only in the message) unless ``trusted_data_paths`` is True,
    which only server-side code that built them itself may pass. ``filter_selections`` records what the user picked
    (exposed-attribute keys and configuration version); the effective filters themselves are in the snapshot."""
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
        filter_selections=copy.deepcopy(filter_selections),
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
