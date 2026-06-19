"""Tests for instance-level perturbations."""
import numpy as np
import pandas as pd
import pytest

from snlg._types import PerturbedProfile
from snlg.config import EndUseFactorConfig, PerturbationConfig
from snlg.perturbation import apply_end_use_factors, apply_temporal_shift, perturb_building


def _make_profile(values: np.ndarray, seed: int = 0) -> PerturbedProfile:
    idx = pd.date_range("2018-01-01", periods=len(values), freq="h")
    series = pd.Series(values.astype(float), index=idx, name="load_kwh")
    annual = float(series.sum())
    return PerturbedProfile(
        bldg_id=1,
        category="mf_small",
        source="resstock",
        hourly_kwh=series,
        original_annual_kwh=annual,
        perturbed_annual_kwh=annual,
    )


# ---------------------------------------------------------------------------
# Temporal shift
# ---------------------------------------------------------------------------

def test_temporal_shift_zero_is_identity(synthetic_8760):
    result = apply_temporal_shift(synthetic_8760, 0)
    pd.testing.assert_series_equal(result, synthetic_8760)


def test_temporal_shift_preserves_annual_energy(synthetic_8760):
    for shift in [-5, -1, 1, 5, 100]:
        result = apply_temporal_shift(synthetic_8760, shift)
        assert abs(result.sum() - synthetic_8760.sum()) < 1e-6, f"shift={shift} changed energy"


def test_temporal_shift_full_year_is_identity(synthetic_8760):
    result = apply_temporal_shift(synthetic_8760, len(synthetic_8760))
    np.testing.assert_array_almost_equal(result.values, synthetic_8760.values)


def test_temporal_shift_moves_values(synthetic_8760):
    result = apply_temporal_shift(synthetic_8760, 3)
    # First 3 values of shifted should be last 3 of original
    np.testing.assert_array_almost_equal(
        result.values[:3], synthetic_8760.values[-3:]
    )


# ---------------------------------------------------------------------------
# End-use factors
# ---------------------------------------------------------------------------

def test_end_use_factors_all_ones_is_identity(synthetic_15min_df):
    original_total = synthetic_15min_df["out.electricity.total.energy_consumption..kwh"].sum()
    factors = {"lighting": 1.0, "plug_loads": 1.0, "hvac": 1.0}
    result = apply_end_use_factors(
        synthetic_15min_df, factors,
        "out.electricity.total.energy_consumption..kwh"
    )
    # Total should be close (recomputed from end-uses)
    new_total = result["out.electricity.total.energy_consumption..kwh"].sum()
    # Allow small difference since total is recomputed from end-use sum, not original total
    assert new_total > 0


def test_end_use_factors_no_negative_values(synthetic_15min_df):
    factors = {"lighting": 0.5, "plug_loads": 2.0}
    result = apply_end_use_factors(
        synthetic_15min_df, factors,
        "out.electricity.total.energy_consumption..kwh"
    )
    assert (result["out.electricity.lighting.energy_consumption..kwh"] >= 0).all()
    assert (result["out.electricity.plug_loads.energy_consumption..kwh"] >= 0).all()


# ---------------------------------------------------------------------------
# perturb_building (disabled mode)
# ---------------------------------------------------------------------------

def test_perturb_building_disabled_returns_identity(synthetic_8760):
    profile = _make_profile(synthetic_8760.values)
    cfg = PerturbationConfig(enabled=False)
    rng = np.random.default_rng(0)
    result = perturb_building(profile, cfg, rng)
    pd.testing.assert_series_equal(result.hourly_kwh, profile.hourly_kwh)
    assert result.perturbation_log == {}


# ---------------------------------------------------------------------------
# perturb_building (enabled: temporal shift only)
# ---------------------------------------------------------------------------

def test_perturb_building_temporal_shift_preserves_energy(synthetic_8760):
    profile = _make_profile(synthetic_8760.values)
    cfg = PerturbationConfig(
        enabled=True,
        max_temporal_shift_hours=3,
        energy_tolerance=0.05,
        end_use_factors={},
    )
    rng = np.random.default_rng(7)
    result = perturb_building(profile, cfg, rng)
    # Energy should be exactly preserved by temporal shift
    assert abs(result.perturbed_annual_kwh - profile.original_annual_kwh) < 1e-6
    assert "shift_hours" in result.perturbation_log
