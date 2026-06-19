"""Tests for feasibility evaluation."""
import pytest

from snlg.config import FeasibilityConfig, HardConstraints, SoftConstraintBound, SoftConstraints
from snlg.feasibility import compute_soft_score, passes_hard_constraints


def _make_cfg(
    peak_min=10.0, peak_max=1000.0,
    energy_min=10000.0, energy_max=1e8,
    lf_min=0.1, lf_max=0.9,
) -> FeasibilityConfig:
    return FeasibilityConfig(
        hard=HardConstraints(
            peak_kw_min=peak_min, peak_kw_max=peak_max,
            annual_energy_kwh_min=energy_min, annual_energy_kwh_max=energy_max,
            load_factor_min=lf_min, load_factor_max=lf_max,
        ),
    )


# ---------------------------------------------------------------------------
# Hard constraints
# ---------------------------------------------------------------------------

def test_all_within_bounds_passes():
    cfg = _make_cfg()
    feasible, violations = passes_hard_constraints(500.0, 5_000_000.0, 0.5, cfg)
    assert feasible
    assert violations == []


def test_peak_too_low_rejected():
    cfg = _make_cfg(peak_min=100.0)
    feasible, violations = passes_hard_constraints(50.0, 5_000_000.0, 0.5, cfg)
    assert not feasible
    assert any("peak_kw" in v for v in violations)


def test_peak_too_high_rejected():
    cfg = _make_cfg(peak_max=200.0)
    feasible, violations = passes_hard_constraints(500.0, 5_000_000.0, 0.5, cfg)
    assert not feasible


def test_load_factor_too_low_rejected():
    cfg = _make_cfg(lf_min=0.5)
    feasible, violations = passes_hard_constraints(500.0, 5_000_000.0, 0.2, cfg)
    assert not feasible
    assert any("load_factor" in v for v in violations)


def test_multiple_violations_all_reported():
    cfg = _make_cfg(peak_min=1000.0, lf_max=0.1)
    feasible, violations = passes_hard_constraints(50.0, 5_000_000.0, 0.5, cfg)
    assert not feasible
    assert len(violations) == 2


# ---------------------------------------------------------------------------
# Soft score
# ---------------------------------------------------------------------------

def test_soft_score_zero_inside_bounds():
    cfg = FeasibilityConfig(
        hard=HardConstraints(),
        soft=SoftConstraints(
            weights={"peak_kw": 1.0, "load_factor": 2.0},
            bounds={
                "peak_kw": SoftConstraintBound(min=100.0, max=1000.0),
                "load_factor": SoftConstraintBound(min=0.3, max=0.7),
            },
        ),
    )
    score = compute_soft_score(500.0, 5_000_000.0, 0.5, cfg)
    assert score == 0.0


def test_soft_score_positive_outside_bounds():
    cfg = FeasibilityConfig(
        hard=HardConstraints(),
        soft=SoftConstraints(
            weights={"peak_kw": 1.0},
            bounds={"peak_kw": SoftConstraintBound(min=100.0, max=200.0)},
        ),
    )
    score = compute_soft_score(500.0, 5_000_000.0, 0.5, cfg)
    assert score > 0.0


def test_soft_score_monotonically_increases_away_from_bounds():
    cfg = FeasibilityConfig(
        hard=HardConstraints(),
        soft=SoftConstraints(
            weights={"peak_kw": 1.0},
            bounds={"peak_kw": SoftConstraintBound(min=100.0, max=200.0)},
        ),
    )
    s1 = compute_soft_score(250.0, 0.0, 0.5, cfg)
    s2 = compute_soft_score(400.0, 0.0, 0.5, cfg)
    assert s2 > s1
