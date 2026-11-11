"""Engine facade (FR-ENG-8): the single entry point callers (worker, notebooks, CLI) use.

``run_scenario`` validates a config (dataclass or plain dict, with the same strict rules as YAML),
builds the archetype pool unless one is supplied, runs the ensemble, and reports typed progress
events. It adds no sampling or numerics of its own: results are exactly those of
``ensemble.run_ensemble``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from microgrid import __version__
from microgrid.community_load.synthetic import archetype, ensemble
from microgrid.community_load.synthetic._types import ArchetypePool, EnsembleResult, NeighborhoodResult
from microgrid.community_load.synthetic.config import ScenarioConfig, config_from_dict

Stage = Literal["building_pool", "running", "checking_convergence", "finished"]


@dataclass(frozen=True)
class ProgressEvent:
    """One progress report. ``run_index`` is the 0-based index of the latest run (-1 before any run)."""

    stage: Stage
    run_index: int
    n_attempted: int
    n_feasible: int
    max_runs: int
    converged: bool | None
    message: str


class RunCancelled(Exception):
    """Raised between runs when ``should_cancel`` returns True; carries the partial counts."""

    def __init__(self, n_attempted: int, n_feasible: int) -> None:
        super().__init__(f"run cancelled after {n_attempted} attempted runs ({n_feasible} feasible)")
        self.n_attempted = n_attempted
        self.n_feasible = n_feasible


def _metrics_row(r: NeighborhoodResult) -> dict[str, Any]:
    return {
        "run_index": int(r.run_index),
        "peak_kw": float(r.peak_kw),
        "annual_energy_kwh": float(r.annual_energy_kwh),
        "load_factor": float(r.load_factor),
        "soft_score": float(r.soft_score),
        "feasible": bool(r.feasible),
    }


@dataclass
class RunResult:
    """Typed result of one scenario run."""

    ensemble: EnsembleResult
    metrics: list[dict[str, Any]]  # one row per feasible ensemble member
    seeds: dict[str, int]  # seeds actually used; supply them as ``rng`` to replay the run
    engine_version: str
    config_snapshot: dict[str, Any] = field(default_factory=dict)

    @property
    def converged(self) -> bool:
        return self.ensemble.converged

    def to_dict(self) -> dict[str, Any]:
        """JSON-serializable summary (no time series)."""
        e = self.ensemble
        return {
            "engine_version": self.engine_version,
            "seeds": dict(self.seeds),
            "config_snapshot": self.config_snapshot,
            "metrics": [dict(m) for m in self.metrics],
            "converged": bool(e.converged),
            "n_attempted": int(e.n_attempted),
            "n_rejected": int(e.n_rejected),
            "diversity_metrics": {k: float(v) for k, v in e.diversity_metrics.items()},
        }


def run_scenario(
    config: ScenarioConfig | Mapping[str, Any],
    *,
    data_dirs: Mapping[str, Path | str] | None = None,
    pool: ArchetypePool | None = None,
    progress: Callable[[ProgressEvent], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> RunResult:
    """Run one scenario end to end.

    ``config``: a ScenarioConfig or a plain dict (validated strictly, like YAML).
    ``data_dirs``: ``{"resstock": dir, "comstock": dir}`` of timeseries; defaults come from ``cfg.data``.
    ``pool``: a pre-built ArchetypePool; otherwise it is built from the ``cfg.data`` characteristics files.
    ``progress``: receives a ProgressEvent after the pool is ready, after every run, after every convergence
    check, and at the end. ``should_cancel``: polled before each run; True raises RunCancelled.
    """
    if isinstance(config, ScenarioConfig):
        cfg = config
    elif isinstance(config, Mapping):
        cfg = config_from_dict(dict(config), source="<dict>")
    else:
        raise TypeError(f"config must be a ScenarioConfig or a dict, got {type(config).__name__}")

    max_runs = cfg.ensemble.max_runs

    def emit(stage: Stage, run_index: int, n_att: int, n_feas: int, converged: bool | None, message: str) -> None:
        if progress is not None:
            progress(ProgressEvent(stage, run_index, n_att, n_feas, max_runs, converged, message))

    if pool is None:
        bg = Path(cfg.data.background_dir)
        pool = archetype.build_archetype_pool(bg / cfg.data.resstock_chars_file, bg / cfg.data.comstock_chars_file, cfg)
    emit("building_pool", -1, 0, 0, None, "archetype pool ready")

    dirs: dict[str, Path] = (
        {k: Path(v) for k, v in data_dirs.items()}
        if data_dirs is not None
        else {
            "resstock": Path(cfg.data.resstock_timeseries_dir),
            "comstock": Path(cfg.data.comstock_timeseries_dir),
        }
    )

    def before_run(run_index: int, n_att: int, n_feas: int) -> None:
        if should_cancel is not None and should_cancel():
            raise RunCancelled(n_att, n_feas)

    def after_run(result: NeighborhoodResult, n_att: int, n_feas: int) -> None:
        verdict = "feasible" if result.feasible else "rejected"
        emit("running", result.run_index, n_att, n_feas, None, f"run {result.run_index + 1}/{max_runs} {verdict}")

    def on_check(converged: bool, n_att: int, n_feas: int) -> None:
        state = "converged" if converged else "not converged yet"
        emit("checking_convergence", n_att - 1, n_att, n_feas, converged, f"convergence check: {state}")

    ens = ensemble.run_ensemble(pool, cfg, dirs, before_run=before_run, after_run=after_run, on_check=on_check)

    snapshot = asdict(cfg)
    snapshot["rng"] = dict(ens.seeds)  # the seeds actually used, so the snapshot alone replays the run
    result = RunResult(
        ensemble=ens,
        metrics=[_metrics_row(r) for r in ens.runs],
        seeds=dict(ens.seeds),
        engine_version=__version__,
        config_snapshot=snapshot,
    )
    emit(
        "finished",
        ens.n_attempted - 1,
        ens.n_attempted,
        len(ens.runs),
        ens.converged,
        f"finished: {len(ens.runs)} feasible of {ens.n_attempted} attempted",
    )
    return result
