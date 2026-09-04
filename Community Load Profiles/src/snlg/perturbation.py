"""Instance-level perturbations — Section 4 of the methodology.

Two structured perturbation types (both off by default):
  1. Temporal shift  — circular roll of ±N hours (exact energy conservation)
  2. End-use scaling — lognormal multiplicative factors per end-use group

When config.perturbation.enabled is False, this module is a no-op and
returns the profile unchanged with an empty perturbation_log.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from snlg._types import PerturbedProfile
from snlg.config import PerturbationConfig

# Mapping from parquet column substrings to perturbation groups
_END_USE_GROUP_MAP: list[tuple[str, str]] = [
    ("lighting", "lighting"),
    ("plug_loads", "plug_loads"),
    ("cooling", "hvac"),
    ("heating", "hvac"),
    ("hot_water", "hot_water"),
    ("water_heater", "hot_water"),
]


def _classify_end_use_column(col: str) -> str | None:
    for substring, group in _END_USE_GROUP_MAP:
        if substring in col:
            return group
    return None


def apply_temporal_shift(profile: pd.Series, shift_hours: int) -> pd.Series:
    """Circularly shift an hourly load profile by shift_hours.

    Positive shift moves load later; negative shift moves it earlier.
    Annual energy is preserved exactly (circular shift = permutation).
    """
    if shift_hours == 0:
        return profile
    shifted = np.roll(profile.values, shift_hours)
    return pd.Series(shifted, index=profile.index, name=profile.name)


def apply_end_use_factors(
    df_15min: pd.DataFrame,
    factors: dict[str, float],
    total_col: str,
) -> pd.DataFrame:
    """Apply per-end-use multiplicative factors at 15-min resolution.

    Factors are keyed by group name (lighting, plug_loads, hvac, hot_water).
    The total electricity column is recomputed from the end-use columns after
    scaling so the aggregate is consistent.
    """
    if not factors:
        return df_15min

    df = df_15min.copy()
    end_use_cols = [c for c in df.columns if c != total_col and c != "timestamp" and c != "bldg_id"]

    affected_cols: list[str] = []
    for col in end_use_cols:
        group = _classify_end_use_column(col)
        if group and group in factors:
            df[col] = df[col] * factors[group]
            affected_cols.append(col)

    # Recompute total from end-use columns when any were scaled
    if affected_cols and total_col in df.columns:
        # Sum all end-use columns that are numeric and represent consumption
        kwh_cols = [
            c for c in end_use_cols
            if df[c].dtype in (float, "float32", "float64")
        ]
        if kwh_cols:
            df[total_col] = df[kwh_cols].sum(axis=1)

    return df


def perturb_building(
    profile: PerturbedProfile,
    cfg: PerturbationConfig,
    rng: np.random.Generator,
    df_15min: pd.DataFrame | None = None,
    total_col: str = "out.electricity.total.energy_consumption..kwh",
) -> PerturbedProfile:
    """Apply all configured perturbations to one building profile.

    When cfg.enabled is False, returns the profile unchanged.
    End-use perturbations require the raw 15-min DataFrame (df_15min).
    Temporal shifts are applied to the already-hourly profile.
    """
    if not cfg.enabled:
        return profile

    log: dict = {}
    hourly = profile.hourly_kwh.copy()

    # --- End-use factors (applied at 15-min resolution, before resampling) ---
    factors: dict[str, float] = {}
    if df_15min is not None and cfg.end_use_factors:
        for group, factor_cfg in cfg.end_use_factors.items():
            if factor_cfg.sigma > 0:
                factors[group] = float(rng.lognormal(mean=0.0, sigma=factor_cfg.sigma))

        if factors:
            df_scaled = apply_end_use_factors(df_15min, factors, total_col)
            df_scaled.index = pd.to_datetime(df_scaled["timestamp"])
            hourly = df_scaled.resample("h")[total_col].sum()
            log["end_use_factors"] = factors

    # --- Temporal shift (applied to hourly profile) ---
    max_shift = cfg.max_temporal_shift_hours
    shift_hours = int(rng.integers(-max_shift, max_shift + 1)) if max_shift > 0 else 0
    if shift_hours != 0:
        hourly = apply_temporal_shift(hourly, shift_hours)
        log["shift_hours"] = shift_hours

    perturbed_annual = float(hourly.sum())
    original_annual = profile.original_annual_kwh

    # Warn but do not reject if energy tolerance is breached
    if original_annual > 0:
        drift = abs(perturbed_annual - original_annual) / original_annual
        if drift > cfg.energy_tolerance:
            log["energy_drift_warning"] = f"{drift:.2%} (tolerance {cfg.energy_tolerance:.2%})"

    return PerturbedProfile(
        bldg_id=profile.bldg_id,
        category=profile.category,
        source=profile.source,
        hourly_kwh=hourly,
        original_annual_kwh=original_annual,
        perturbed_annual_kwh=perturbed_annual,
        perturbation_log=log,
    )
