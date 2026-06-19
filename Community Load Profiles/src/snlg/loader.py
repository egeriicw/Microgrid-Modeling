"""Parquet I/O, multifamily unit scaling, and 15-min → hourly resampling.

All disk access is concentrated here. The lru_cache on load_building_profile
prevents redundant reads when the same building appears in multiple runs.
"""
from __future__ import annotations

import functools
from pathlib import Path

import pandas as pd

from snlg._types import BuildingId, BuildingSelection, PerturbedProfile
from snlg.archetype import MF_BUILDING_TYPES
from snlg.config import ScenarioConfig

# ResStock total electricity column name
RESSTOCK_KWH_COL = "out.electricity.total.energy_consumption..kwh"
# ComStock total electricity column name
COMSTOCK_KWH_COL = "out.electricity.total.energy_consumption"


@functools.lru_cache(maxsize=None)
def _load_parquet_cached(path: str) -> pd.DataFrame:
    """Load a parquet file and cache it by path string."""
    return pd.read_parquet(path)


def _resolve_parquet_path(
    bldg_id: BuildingId,
    source: str,
    data_dirs: dict[str, Path],
    upgrade: str,
    state: str,
) -> Path:
    base = data_dirs[source] / f"upgrade={upgrade}" / f"state={state}"
    return base / f"{bldg_id}-0.parquet"


def _get_electricity_col(source: str) -> str:
    if source == "resstock":
        return RESSTOCK_KWH_COL
    return COMSTOCK_KWH_COL


def load_building_profile(
    bldg_id: BuildingId,
    source: str,
    data_dirs: dict[str, Path],
    upgrade: str,
    state: str,
) -> pd.DataFrame:
    """Load a single building's 15-min parquet, return as DataFrame.

    Result is cached in-process to avoid re-reading the same file when the
    same building is sampled across multiple runs.
    """
    path = _resolve_parquet_path(bldg_id, source, data_dirs, upgrade, state)
    return _load_parquet_cached(str(path))


def apply_mf_unit_multiplier(
    df: pd.DataFrame,
    unit_count: float,
    building_type: str,
    elec_col: str,
) -> pd.DataFrame:
    """Scale electricity consumption by unit_count for multifamily buildings.

    Only applies when building_type is in the multifamily list. Single-family
    and commercial buildings pass through unchanged (unit_count=1.0).
    """
    if building_type not in MF_BUILDING_TYPES or unit_count <= 1.0:
        return df
    df = df.copy()
    df[elec_col] = df[elec_col] * unit_count
    return df


def resample_to_hourly(df: pd.DataFrame, elec_col: str) -> pd.Series:
    """Resample 15-min timeseries to hourly by summing within each hour."""
    df = df.copy()
    df.index = pd.to_datetime(df["timestamp"])
    return df.resample("h")[elec_col].sum()


def _get_unit_multiplier(
    selection: BuildingSelection,
    resstock_chars: pd.DataFrame,
) -> float:
    """Look up the multifamily unit count for a building, defaulting to 1.0."""
    if selection.unit_multiplier != 1.0:
        return selection.unit_multiplier  # already resolved by caller

    if selection.source != "resstock":
        return 1.0

    match = resstock_chars.loc[
        resstock_chars["bldg_id"] == selection.bldg_id,
        "in.geometry_building_number_units_mf",
    ].values
    if len(match) > 0 and not pd.isna(match[0]):
        return float(match[0])
    return 1.0


def load_and_scale_profile(
    selection: BuildingSelection,
    data_dirs: dict[str, Path],
    cfg: ScenarioConfig,
    resstock_chars: pd.DataFrame | None = None,
    building_type: str = "",
) -> PerturbedProfile:
    """Load a building parquet, apply MF scaling, resample to hourly.

    Returns a PerturbedProfile with identity perturbation (no shifts,
    no end-use factors). The perturbation module replaces this with
    actual perturbations when enabled.
    """
    df = load_building_profile(
        selection.bldg_id,
        selection.source,
        data_dirs,
        cfg.data.upgrade,
        cfg.data.state,
    )
    elec_col = _get_electricity_col(selection.source)

    df = apply_mf_unit_multiplier(df, selection.unit_multiplier, building_type, elec_col)

    hourly = resample_to_hourly(df, elec_col)
    annual_kwh = float(hourly.sum())

    return PerturbedProfile(
        bldg_id=selection.bldg_id,
        category=selection.category,
        source=selection.source,
        hourly_kwh=hourly,
        original_annual_kwh=annual_kwh,
        perturbed_annual_kwh=annual_kwh,
        perturbation_log={},
    )


def clear_cache() -> None:
    """Clear the parquet file cache (useful between test runs)."""
    _load_parquet_cached.cache_clear()
