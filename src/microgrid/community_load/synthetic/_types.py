"""Shared dataclasses and type aliases for the SNLG package.

No computation lives here — only data contracts between modules.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypeAlias

import pandas as pd

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

LoadProfile: TypeAlias = pd.Series  # DatetimeIndex → kWh/h, length 8760
BuildingId: TypeAlias = int


# ---------------------------------------------------------------------------
# Archetype layer
# ---------------------------------------------------------------------------

@dataclass
class ArchetypePool:
    """Output of archetype.py: filtered bldg_id lists keyed by category."""
    residential: dict[str, list[BuildingId]]  # e.g. "mf_small" → [bldg_ids]
    commercial: dict[str, list[BuildingId]]   # e.g. "small_office" → [bldg_ids]
    resstock_chars: pd.DataFrame              # full characteristics table (for metadata lookups)
    comstock_chars: pd.DataFrame
    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Composition layer
# ---------------------------------------------------------------------------

@dataclass
class NeighborhoodComposition:
    """Sampled integer building counts per category for one run."""
    counts: dict[str, int]          # category → count
    proportions: dict[str, float]   # category → realized proportion


# ---------------------------------------------------------------------------
# Building selection
# ---------------------------------------------------------------------------

@dataclass
class BuildingSelection:
    """One selected building instance, post-composition sampling."""
    bldg_id: BuildingId
    category: str
    source: str              # "resstock" or "comstock"
    unit_multiplier: float   # 1.0 for non-MF; MF unit count otherwise


# ---------------------------------------------------------------------------
# Perturbation layer
# ---------------------------------------------------------------------------

@dataclass
class PerturbedProfile:
    """Hourly load profile for one building instance, post-perturbation."""
    bldg_id: BuildingId
    category: str
    source: str
    hourly_kwh: LoadProfile          # pd.Series, 8760 values
    original_annual_kwh: float
    perturbed_annual_kwh: float
    perturbation_log: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Neighborhood result (one Monte Carlo run)
# ---------------------------------------------------------------------------

@dataclass
class NeighborhoodResult:
    """Aggregated result of one complete Monte Carlo run."""
    run_index: int
    residential_profile: LoadProfile
    commercial_profile: LoadProfile
    total_profile: LoadProfile
    composition: NeighborhoodComposition
    selections: list[BuildingSelection]
    peak_kw: float
    annual_energy_kwh: float
    load_factor: float
    feasible: bool
    soft_score: float
    perturbation_logs: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Ensemble result (all runs)
# ---------------------------------------------------------------------------

@dataclass
class EnsembleResult:
    """Output of ensemble.run_ensemble(): all runs plus derived statistics."""
    runs: list[NeighborhoodResult]
    converged: bool
    n_attempted: int
    n_rejected: int
    convergence_history: dict[str, list[float]] = field(default_factory=dict)
    percentile_envelopes: dict[str, LoadProfile] = field(default_factory=dict)
    diversity_metrics: dict[str, float] = field(default_factory=dict)
    representative_cases: dict[str, NeighborhoodResult] = field(default_factory=dict)
