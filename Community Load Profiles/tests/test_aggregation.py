"""Tests for neighborhood aggregation."""
import numpy as np
import pandas as pd
import pytest

from snlg._types import PerturbedProfile
from snlg.aggregation import aggregate_profiles, compute_coincidence_factor, compute_metrics


def _make_perturbed(values: np.ndarray, bldg_id: int, source: str = "resstock") -> PerturbedProfile:
    idx = pd.date_range("2018-01-01", periods=len(values), freq="h")
    series = pd.Series(values.astype(float), index=idx)
    return PerturbedProfile(
        bldg_id=bldg_id,
        category="mf_small",
        source=source,
        hourly_kwh=series,
        original_annual_kwh=float(series.sum()),
        perturbed_annual_kwh=float(series.sum()),
    )


def test_aggregate_two_profiles_sums_correctly():
    a = np.ones(8760) * 2.0
    b = np.ones(8760) * 3.0
    profiles = [_make_perturbed(a, 1), _make_perturbed(b, 2)]
    total = aggregate_profiles(profiles)
    assert abs(total.sum() - (a.sum() + b.sum())) < 1e-6
    assert abs(total.max() - 5.0) < 1e-6


def test_aggregate_empty_returns_empty():
    result = aggregate_profiles([])
    assert result.empty


def test_aggregate_source_filter():
    res = _make_perturbed(np.ones(8760) * 2.0, 1, source="resstock")
    com = _make_perturbed(np.ones(8760) * 3.0, 2, source="comstock")
    res_only = aggregate_profiles([res, com], source_filter="resstock")
    assert abs(res_only.max() - 2.0) < 1e-6


def test_compute_metrics_normal_profile():
    rng = np.random.default_rng(0)
    idx = pd.date_range("2018-01-01", periods=8760, freq="h")
    profile = pd.Series(rng.uniform(1.0, 10.0, 8760), index=idx)
    metrics = compute_metrics(profile)
    assert metrics["peak_kw"] == pytest.approx(float(profile.max()))
    assert metrics["annual_energy_kwh"] == pytest.approx(float(profile.sum()))
    assert 0.0 < metrics["load_factor"] < 1.0


def test_compute_metrics_zero_profile():
    idx = pd.date_range("2018-01-01", periods=8760, freq="h")
    profile = pd.Series(np.zeros(8760), index=idx)
    metrics = compute_metrics(profile)
    assert metrics["peak_kw"] == 0.0
    assert metrics["load_factor"] == 0.0


def test_coincidence_factor_all_same_peak():
    """When all buildings peak simultaneously, CF approaches 1."""
    values = np.zeros(8760)
    values[100] = 10.0  # single peak hour
    profiles = [_make_perturbed(values.copy(), i) for i in range(3)]
    total = aggregate_profiles(profiles)
    cf = compute_coincidence_factor(profiles, total)
    assert abs(cf - 1.0) < 1e-6


def test_coincidence_factor_staggered_peaks():
    """Staggered peaks produce CF < 1."""
    v1 = np.zeros(8760); v1[0] = 10.0
    v2 = np.zeros(8760); v2[1] = 10.0
    profiles = [_make_perturbed(v1, 1), _make_perturbed(v2, 2)]
    total = aggregate_profiles(profiles)
    cf = compute_coincidence_factor(profiles, total)
    # Total peak = 10.0, sum of individual peaks = 20.0 → CF = 0.5
    assert abs(cf - 0.5) < 1e-6
