"""Distributional validation — Section 11 of the methodology.

Three validation levels:
  1. Per-sample: physical plausibility checks on each NeighborhoodResult
  2. Distributional: ensemble statistics vs. configurable benchmark ranges
  3. Structural: no diversity collapse, no pathological synchronization
"""
from __future__ import annotations

import numpy as np

from snlg._types import EnsembleResult, NeighborhoodResult
from snlg.config import ValidationConfig


# ---------------------------------------------------------------------------
# Per-sample validity
# ---------------------------------------------------------------------------

def validate_per_sample(result: NeighborhoodResult) -> list[str]:
    """Return warning strings for any implausible properties of one run.

    An empty list means the sample passes all checks.
    """
    warnings: list[str] = []

    if result.peak_kw <= 0:
        warnings.append(f"Run {result.run_index}: peak_kw={result.peak_kw:.3f} is non-positive.")

    if result.annual_energy_kwh <= 0:
        warnings.append(f"Run {result.run_index}: annual_energy_kwh is non-positive.")

    if not (0.0 < result.load_factor <= 1.0):
        warnings.append(
            f"Run {result.run_index}: load_factor={result.load_factor:.4f} outside (0, 1]."
        )

    if result.total_profile.isnull().any():
        n_null = int(result.total_profile.isnull().sum())
        warnings.append(f"Run {result.run_index}: total_profile has {n_null} NaN values.")

    if (result.total_profile < 0).any():
        n_neg = int((result.total_profile < 0).sum())
        warnings.append(f"Run {result.run_index}: total_profile has {n_neg} negative values.")

    return warnings


# ---------------------------------------------------------------------------
# Distributional validation
# ---------------------------------------------------------------------------

def validate_ensemble(
    ensemble: EnsembleResult,
    cfg: ValidationConfig,
) -> dict[str, object]:
    """Validate ensemble statistics against configurable benchmark ranges.

    Returns a report dict with pass/fail per check and warning messages.
    """
    report: dict[str, object] = {"passed": True, "warnings": [], "checks": {}}
    warnings: list[str] = report["warnings"]  # type: ignore[assignment]
    checks: dict[str, bool] = report["checks"]  # type: ignore[assignment]

    if not ensemble.runs:
        warnings.append("No feasible runs to validate.")
        report["passed"] = False
        return report

    peaks = np.array([r.peak_kw for r in ensemble.runs])
    lfs = np.array([r.load_factor for r in ensemble.runs])

    # --- Benchmark range checks ---
    for metric, bounds in cfg.benchmarks.items():
        if len(bounds) != 2:
            continue
        lo, hi = float(bounds[0]), float(bounds[1])
        if metric == "peak_demand_kw":
            mean_val = float(peaks.mean())
        elif metric == "load_factor":
            mean_val = float(lfs.mean())
        else:
            continue

        ok = lo <= mean_val <= hi
        checks[metric] = ok
        if not ok:
            msg = (
                f"Benchmark check FAILED for {metric}: "
                f"mean={mean_val:.4f} outside [{lo:.4f}, {hi:.4f}]"
            )
            warnings.append(msg)
            report["passed"] = False

    # --- Structural check: diversity collapse ---
    if peaks.mean() > 0:
        cv = float(peaks.std() / peaks.mean())
        checks["diversity_collapse"] = cv >= cfg.min_cv_peak
        if cv < cfg.min_cv_peak:
            warnings.append(
                f"Diversity collapse detected: CV of peak demand = {cv:.4f} "
                f"(threshold {cfg.min_cv_peak}). All runs may be nearly identical."
            )
            report["passed"] = False

    # --- Structural check: pathological synchronization ---
    if len(lfs) > 1:
        lf_std = float(lfs.std())
        checks["synchronization"] = lf_std > 1e-6
        if lf_std <= 1e-6:
            warnings.append(
                "Pathological synchronization detected: load factor variance is near zero. "
                "All buildings may be peaking simultaneously every run."
            )
            report["passed"] = False

    return report
