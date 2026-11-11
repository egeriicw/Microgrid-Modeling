"""Adaptive convergence, rule ``prd_default`` (FR-ENG-6; provisional, owner-approved 2026-11-06).

At each check (every ``ensemble.check_every`` feasible runs, from ``ensemble.min_runs``) the tracker compares the
ensemble now with the ensemble at the previous check:

* relative change of the mean annual energy,
* relative change of the mean peak kW,
* relative change of the mean hourly profile (L2 norm of the difference over the L2 norm of the previous mean),

and measures the precision of the P50 (median) profile:

* ``rel_se_p50`` = ||SE|| / ||P50|| (L2 norms over the hours), with SE_h = 1.2533 * s_h / sqrt(K), the large-sample
  standard error of a median (normal approximation; s_h is the sample standard deviation across runs at hour h).

A check *passes* when all three relative changes are strictly below ``epsilon`` and ``rel_se_p50`` is strictly
below ``se_target``. The first check has nothing to compare with, so it cannot pass. The ensemble is converged
once ``consecutive_checks`` (M) checks in a row pass; a failing check resets the count.

Everything here is a pure function of the feasible runs seen so far, so the same seeds replay the same stopping
point.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from microgrid.community_load.synthetic._types import NeighborhoodResult
from microgrid.community_load.synthetic.config import ConvergenceConfig

#: Large-sample ratio SE(median) / SE(mean) for a normal distribution: sqrt(pi / 2).
MEDIAN_SE_FACTOR = math.sqrt(math.pi / 2.0)
_TINY = 1e-12


@dataclass(frozen=True)
class ConvergenceCheck:
    """The outcome of one convergence check (plain data; ``to_dict`` is JSON-serializable)."""

    rule: str
    n_attempted: int
    n_feasible: int
    passed: bool
    consecutive_passes: int
    required_passes: int
    converged: bool
    rel_change: dict[str, float] = field(default_factory=dict)  # empty on the first check
    rel_se_p50: float | None = None
    epsilon: float = 0.0
    se_target: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "n_attempted": int(self.n_attempted),
            "n_feasible": int(self.n_feasible),
            "passed": bool(self.passed),
            "consecutive_passes": int(self.consecutive_passes),
            "required_passes": int(self.required_passes),
            "converged": bool(self.converged),
            "rel_change": {k: float(v) for k, v in self.rel_change.items()},
            "rel_se_p50": None if self.rel_se_p50 is None else float(self.rel_se_p50),
            "epsilon": float(self.epsilon),
            "se_target": None if self.se_target is None else float(self.se_target),
        }


def _rel(cur: float, prev: float) -> float:
    denom = abs(prev) if abs(prev) > _TINY else 1.0
    return abs(cur - prev) / denom


class ConvergenceTracker:
    """Stateful evaluator for the ``prd_default`` rule; call ``check`` once per check point, in order."""

    def __init__(self, cfg: ConvergenceConfig) -> None:
        if cfg.rule != "prd_default":
            raise ValueError(f"ConvergenceTracker implements rule 'prd_default', not {cfg.rule!r}")
        self._cfg = cfg
        self._prev: tuple[float, float, np.ndarray] | None = None
        self._consecutive = 0

    def check(self, feasible_runs: Sequence[NeighborhoodResult], n_attempted: int) -> ConvergenceCheck:
        cfg = self._cfg
        k = len(feasible_runs)
        if k == 0:
            raise ValueError("cannot check convergence without any feasible run")
        lengths = {len(r.total_profile) for r in feasible_runs}
        if len(lengths) != 1:
            raise ValueError(
                f"all runs must have profiles of the same length to check convergence, got {sorted(lengths)}"
            )
        profiles = np.vstack([r.total_profile.to_numpy(dtype=float) for r in feasible_runs])
        energy = float(np.mean([r.annual_energy_kwh for r in feasible_runs]))
        peak = float(np.mean([r.peak_kw for r in feasible_runs]))
        mean_profile = profiles.mean(axis=0)

        rel_se: float | None = None
        if k >= 2 and profiles.shape[1] > 0:
            se = MEDIAN_SE_FACTOR * profiles.std(axis=0, ddof=1) / math.sqrt(k)
            p50_norm = float(np.linalg.norm(np.median(profiles, axis=0)))
            se_norm = float(np.linalg.norm(se))
            rel_se = se_norm / p50_norm if p50_norm > _TINY else (0.0 if se_norm <= _TINY else math.inf)

        rel_change: dict[str, float] = {}
        passed = False
        if self._prev is not None:
            p_energy, p_peak, p_profile = self._prev
            prev_norm = float(np.linalg.norm(p_profile))
            diff_norm = float(np.linalg.norm(mean_profile - p_profile))
            rel_change = {
                "annual_energy_kwh": _rel(energy, p_energy),
                "peak_kw": _rel(peak, p_peak),
                "mean_profile": diff_norm / prev_norm if prev_norm > _TINY else diff_norm,
            }
            passed = all(v < cfg.epsilon for v in rel_change.values()) and rel_se is not None and rel_se < cfg.se_target
        self._prev = (energy, peak, mean_profile)
        self._consecutive = self._consecutive + 1 if passed else 0
        return ConvergenceCheck(
            rule="prd_default",
            n_attempted=n_attempted,
            n_feasible=k,
            passed=passed,
            consecutive_passes=self._consecutive,
            required_passes=cfg.consecutive_checks,
            converged=self._consecutive >= cfg.consecutive_checks,
            rel_change=rel_change,
            rel_se_p50=rel_se,
            epsilon=cfg.epsilon,
            se_target=cfg.se_target,
        )
