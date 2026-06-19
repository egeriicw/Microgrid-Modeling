"""Neighborhood aggregation — Section 5 of the methodology.

Deterministic within a realization: L_k,t = Σ L_i,t
Also computes peak, annual energy, load factor, and diversity metrics.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from snlg._types import PerturbedProfile


def aggregate_profiles(
    profiles: list[PerturbedProfile],
    source_filter: str | None = None,
) -> pd.Series:
    """Sum hourly kWh across all (or filtered) profiles.

    Args:
        profiles: List of per-building PerturbedProfiles.
        source_filter: If given, only include profiles with matching .source.

    Returns:
        Hourly pd.Series with DatetimeIndex.
    """
    if source_filter is not None:
        profiles = [p for p in profiles if p.source == source_filter]

    if not profiles:
        # Return an empty series with a placeholder index
        return pd.Series(dtype=float)

    # Align on the common index (inner join handles any edge-case mismatches)
    total = profiles[0].hourly_kwh.copy()
    for p in profiles[1:]:
        total = total.add(p.hourly_kwh, fill_value=0.0)

    return total


def compute_metrics(total_profile: pd.Series) -> dict[str, float]:
    """Derive scalar system metrics from an hourly 8760 profile."""
    if total_profile.empty or total_profile.sum() == 0:
        return {
            "peak_kw": 0.0,
            "annual_energy_kwh": 0.0,
            "load_factor": 0.0,
            "coincidence_factor": float("nan"),
            "diversity_factor": float("nan"),
        }

    peak_kw = float(total_profile.max())
    annual_kwh = float(total_profile.sum())
    n_hours = len(total_profile)
    load_factor = annual_kwh / (peak_kw * n_hours) if peak_kw > 0 else 0.0

    return {
        "peak_kw": peak_kw,
        "annual_energy_kwh": annual_kwh,
        "load_factor": load_factor,
    }


def compute_coincidence_factor(
    profiles: list[PerturbedProfile],
    total_profile: pd.Series,
) -> float:
    """Ratio of coincident peak to sum of individual building peaks.

    Values < 1 indicate load diversity (peaks don't all occur simultaneously).
    """
    if not profiles or total_profile.empty:
        return float("nan")
    sum_individual_peaks = sum(float(p.hourly_kwh.max()) for p in profiles)
    if sum_individual_peaks == 0:
        return float("nan")
    return float(total_profile.max()) / sum_individual_peaks
