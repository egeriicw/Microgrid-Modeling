"""System-level feasibility evaluation — Section 6 of the methodology.

Hard constraints: binary accept/reject based on peak, energy, load factor.
Soft constraints: penalty score S(x) = Σ w_i * f_i(x) for ranking.
"""
from __future__ import annotations

from snlg._types import NeighborhoodResult
from snlg.config import FeasibilityConfig, SoftConstraintBound


def _hard_check(value: float, lo: float, hi: float, name: str) -> str | None:
    """Return a violation string if value is outside [lo, hi], else None."""
    if value < lo or value > hi:
        return f"{name}={value:.3f} outside [{lo:.3f}, {hi:.3f}]"
    return None


def passes_hard_constraints(
    peak_kw: float,
    annual_energy_kwh: float,
    load_factor: float,
    cfg: FeasibilityConfig,
) -> tuple[bool, list[str]]:
    """Check all hard constraints. Returns (feasible, list_of_violations)."""
    h = cfg.hard
    violations: list[str] = []
    for msg in [
        _hard_check(peak_kw, h.peak_kw_min, h.peak_kw_max, "peak_kw"),
        _hard_check(annual_energy_kwh, h.annual_energy_kwh_min, h.annual_energy_kwh_max, "annual_energy_kwh"),
        _hard_check(load_factor, h.load_factor_min, h.load_factor_max, "load_factor"),
    ]:
        if msg is not None:
            violations.append(msg)
    return len(violations) == 0, violations


def _penalty(value: float, bound: SoftConstraintBound) -> float:
    """Piecewise-linear penalty: 0 inside bounds, grows linearly outside."""
    penalty = 0.0
    if bound.min is not None and value < bound.min and bound.min != 0:
        penalty += (bound.min - value) / bound.min
    if bound.max is not None and value > bound.max and bound.max != 0:
        penalty += (value - bound.max) / bound.max
    return penalty


def compute_soft_score(
    peak_kw: float,
    annual_energy_kwh: float,
    load_factor: float,
    cfg: FeasibilityConfig,
) -> float:
    """S(x) = Σ w_i * f_i(x). Lower is better; 0 means all soft targets met."""
    metric_map = {
        "peak_kw": peak_kw,
        "annual_energy_kwh": annual_energy_kwh,
        "load_factor": load_factor,
    }
    score = 0.0
    for metric_name, bound in cfg.soft.bounds.items():
        weight = cfg.soft.weights.get(metric_name, 1.0)
        value = metric_map.get(metric_name, 0.0)
        score += weight * _penalty(value, bound)
    return score
