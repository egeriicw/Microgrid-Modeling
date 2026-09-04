"""Shared pytest fixtures for the SNLG test suite."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from snlg.config import (
    ArchetypeFilterConfig,
    CategoryBounds,
    CompositionConfig,
    DataConfig,
    EnsembleConfig,
    FeasibilityConfig,
    HardConstraints,
    OutputConfig,
    PerturbationConfig,
    ResidentialFilterConfig,
    CommercialFilterConfig,
    RNGConfig,
    ScenarioConfig,
    ValidationConfig,
    ConvergenceConfig,
)
from snlg.rng import RNGBundle, make_rng_bundle


@pytest.fixture
def rng_bundle() -> RNGBundle:
    return make_rng_bundle(
        composition_seed=1,
        archetype_seed=2,
        perturbation_seed=3,
    )


@pytest.fixture
def minimal_cfg() -> ScenarioConfig:
    """Minimal valid ScenarioConfig with 2 residential categories, 10 buildings."""
    cfg = ScenarioConfig(
        name="test",
        rng=RNGConfig(composition_seed=1, archetype_seed=2, perturbation_seed=3),
        data=DataConfig(upgrade="0", state="DC"),
        composition=CompositionConfig(
            total_buildings=10,
            method="dirichlet",
            dirichlet_concentration=5.0,
            categories={
                "mf_small": CategoryBounds(min_fraction=0.5, max_fraction=1.0, source="resstock"),
                "mf_mid": CategoryBounds(min_fraction=0.0, max_fraction=0.5, source="resstock"),
            },
        ),
        archetype_filters=ArchetypeFilterConfig(
            resstock={
                "mf_small": ResidentialFilterConfig(
                    building_type_heights=["Multi-Family with 2 - 4 Units"],
                ),
                "mf_mid": ResidentialFilterConfig(
                    building_type_heights=["Multi-Family with 5+ Units, 4-7 Stories"],
                ),
            },
            comstock={},
        ),
        feasibility=FeasibilityConfig(
            hard=HardConstraints(
                peak_kw_min=0.0, peak_kw_max=1e9,
                annual_energy_kwh_min=0.0, annual_energy_kwh_max=1e12,
                load_factor_min=0.0, load_factor_max=1.0,
            ),
        ),
        perturbation=PerturbationConfig(enabled=False),
        ensemble=EnsembleConfig(mode="fixed", max_runs=5, min_runs=2, check_every=2),
        convergence=ConvergenceConfig(lookback_window=2, min_stable_window=2, epsilon=0.01),
        output=OutputConfig(per_run_csvs=False, representative_cases=False),
        validation=ValidationConfig(),
    )
    return cfg


@pytest.fixture
def synthetic_8760() -> pd.Series:
    """A synthetic full-year hourly load profile (8760 values)."""
    rng = np.random.default_rng(0)
    idx = pd.date_range("2018-01-01", periods=8760, freq="h")
    values = rng.uniform(1.0, 10.0, 8760)
    return pd.Series(values, index=idx, name="load_kwh")


@pytest.fixture
def synthetic_15min_df() -> pd.DataFrame:
    """A synthetic 15-min resolution building DataFrame (35040 rows)."""
    rng = np.random.default_rng(42)
    idx = pd.date_range("2018-01-01", periods=35040, freq="15min")
    return pd.DataFrame({
        "timestamp": idx,
        "bldg_id": 99999,
        "in.sqft": 1500.0,
        "out.electricity.total.energy_consumption..kwh": rng.uniform(0.1, 2.0, 35040).astype("float32"),
        "out.electricity.lighting.energy_consumption..kwh": rng.uniform(0.01, 0.3, 35040).astype("float32"),
        "out.electricity.plug_loads.energy_consumption..kwh": rng.uniform(0.01, 0.5, 35040).astype("float32"),
        "out.electricity.cooling.energy_consumption..kwh": rng.uniform(0.0, 1.0, 35040).astype("float32"),
        "out.electricity.heating.energy_consumption..kwh": rng.uniform(0.0, 0.8, 35040).astype("float32"),
    })
