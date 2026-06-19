"""Tests for distributional validation."""
import numpy as np
import pandas as pd
import pytest

from snlg._types import EnsembleResult, NeighborhoodResult, NeighborhoodComposition
from snlg.config import ValidationConfig
from snlg.validation import validate_ensemble, validate_per_sample


def _make_result(run_index: int, peak_kw: float, annual_kwh: float, load_factor: float) -> NeighborhoodResult:
    idx = pd.date_range("2018-01-01", periods=8760, freq="h")
    profile = pd.Series(np.ones(8760) * (annual_kwh / 8760), index=idx)
    return NeighborhoodResult(
        run_index=run_index,
        residential_profile=profile,
        commercial_profile=pd.Series(dtype=float),
        total_profile=profile,
        composition=NeighborhoodComposition(counts={}, proportions={}),
        selections=[],
        peak_kw=peak_kw,
        annual_energy_kwh=annual_kwh,
        load_factor=load_factor,
        feasible=True,
        soft_score=0.0,
    )


def _make_ensemble(n: int = 10, vary_peak: bool = True) -> EnsembleResult:
    rng = np.random.default_rng(0)
    runs = [
        _make_result(
            i,
            peak_kw=rng.uniform(100, 500) if vary_peak else 300.0,
            annual_kwh=rng.uniform(500_000, 2_000_000),
            load_factor=rng.uniform(0.3, 0.7),
        )
        for i in range(n)
    ]
    return EnsembleResult(
        runs=runs,
        converged=True,
        n_attempted=n,
        n_rejected=0,
    )


# ---------------------------------------------------------------------------
# Per-sample validation
# ---------------------------------------------------------------------------

def test_valid_result_produces_no_warnings():
    result = _make_result(0, peak_kw=300.0, annual_kwh=1_000_000.0, load_factor=0.38)
    warnings = validate_per_sample(result)
    assert warnings == []


def test_non_positive_peak_produces_warning():
    result = _make_result(0, peak_kw=0.0, annual_kwh=1_000_000.0, load_factor=0.0)
    warnings = validate_per_sample(result)
    assert any("peak_kw" in w for w in warnings)


def test_load_factor_above_one_produces_warning():
    result = _make_result(0, peak_kw=300.0, annual_kwh=1_000_000.0, load_factor=1.5)
    warnings = validate_per_sample(result)
    assert any("load_factor" in w for w in warnings)


# ---------------------------------------------------------------------------
# Distributional validation
# ---------------------------------------------------------------------------

def test_valid_ensemble_passes():
    ensemble = _make_ensemble(n=20)
    cfg = ValidationConfig(benchmarks={"peak_demand_kw": [50.0, 600.0]}, min_cv_peak=0.001)
    report = validate_ensemble(ensemble, cfg)
    assert report["passed"] is True
    assert report["warnings"] == []


def test_benchmark_violation_fails():
    ensemble = _make_ensemble(n=20)
    # Very tight bounds that will fail
    cfg = ValidationConfig(benchmarks={"peak_demand_kw": [1.0, 2.0]})
    report = validate_ensemble(ensemble, cfg)
    assert report["passed"] is False
    assert len(report["warnings"]) > 0


def test_diversity_collapse_detected():
    """When all runs have the same peak, CV=0 triggers collapse warning."""
    ensemble = _make_ensemble(n=10, vary_peak=False)
    cfg = ValidationConfig(min_cv_peak=0.01)
    report = validate_ensemble(ensemble, cfg)
    assert report["passed"] is False
    assert any("collapse" in w.lower() for w in report["warnings"])


def test_empty_ensemble_fails():
    ensemble = EnsembleResult(runs=[], converged=False, n_attempted=0, n_rejected=0)
    cfg = ValidationConfig()
    report = validate_ensemble(ensemble, cfg)
    assert report["passed"] is False
