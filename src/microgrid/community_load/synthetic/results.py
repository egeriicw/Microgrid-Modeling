"""Structured results (FR-ENG-5, FR-ENG-7): everything a chart needs, computed in the engine.

``build_results(ensemble)`` turns the feasible runs of an ``EnsembleResult`` into a ``ResultsModel`` whose
``to_dict()`` is a strictly JSON-serializable document (no NaN/Infinity) described in
``docs/reference/results-schema.md``. The document carries ``schema_version``; any change to its shape bumps it.

Conventions
-----------
* Power is kW, energy is kWh, and every key says which (``mean_kw`` ...). A source value in kWh per 15-minute
  interval is multiplied by 4 to give kW; kWh per hour is already kW.
* Envelopes: ``mean_kw`` is the ensemble mean; ``p10_kw``/``p50_kw``/``p90_kw`` are the 10th/50th/90th percentiles
  across runs at each time step (linear interpolation, ``numpy.percentile`` default).
* Only feasible runs (the ensemble members) are summarized.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from microgrid.community_load.synthetic._types import EnsembleResult, NeighborhoodResult

SCHEMA_VERSION = "1.0"
PERCENTILES = (10, 50, 90)
#: 15-minute and hourly energy must agree to this relative tolerance (float32 source data summed in float64).
ENERGY_RTOL = 1e-5
SEASONS: dict[str, list[str]] = {
    "winter": ["Dec", "Jan", "Feb"],
    "spring": ["Mar", "Apr", "May"],
    "summer": ["Jun", "Jul", "Aug"],
    "fall": ["Sep", "Oct", "Nov"],
}
_SEASON_MONTHS = {"winter": (12, 1, 2), "spring": (3, 4, 5), "summer": (6, 7, 8), "fall": (9, 10, 11)}
_TINY = 1e-12


def _clean(arr: np.ndarray) -> list[float | None]:
    """A JSON-safe list: non-finite values (only possible for a season with no data) become null."""
    return [float(x) if math.isfinite(x) else None for x in np.asarray(arr, dtype=float).tolist()]


def _envelope(matrix: np.ndarray, axis_scale: float = 1.0) -> dict[str, list[float | None]]:
    """Mean and P10/P50/P90 across runs (rows) of a (runs x steps) matrix, times ``axis_scale``."""
    p10, p50, p90 = np.percentile(matrix, PERCENTILES, axis=0)
    return {
        "mean_kw": _clean(matrix.mean(axis=0) * axis_scale),
        "p10_kw": _clean(p10 * axis_scale),
        "p50_kw": _clean(p50 * axis_scale),
        "p90_kw": _clean(p90 * axis_scale),
    }


def _stats(values: np.ndarray) -> dict[str, float | int]:
    k = len(values)
    p10, p50, p90 = np.percentile(values, PERCENTILES)
    return {
        "mean": float(values.mean()),
        "std": float(values.std(ddof=1)) if k > 1 else 0.0,
        "p10": float(p10),
        "p50": float(p50),
        "p90": float(p90),
        "min": float(values.min()),
        "max": float(values.max()),
        "n": int(k),
    }


@dataclass
class ResultsModel:
    """The computed results; ``to_dict()`` is the documented, versioned JSON shape."""

    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.data


def _empty() -> ResultsModel:
    return ResultsModel(
        {
            "schema_version": SCHEMA_VERSION,
            "units": {"power": "kW", "energy": "kWh"},
            "n_runs": 0,
            "percentiles": list(PERCENTILES),
            "time": None,
            "envelopes": None,
            "by_building_type": {},
            "load_duration": None,
            "seasonal_daily": None,
            "scalars": None,
            "energy_check": {"checked": False, "max_relative_difference": None, "tolerance": ENERGY_RTOL},
        }
    )


def _axis(index: pd.Index, label: str) -> dict[str, Any]:
    if not isinstance(index, pd.DatetimeIndex) or len(index) == 0:
        raise ValueError(f"{label} profile needs a non-empty DatetimeIndex")
    steps: np.ndarray = (
        np.unique(np.diff(index.to_numpy()).astype("timedelta64[s]").astype(np.int64))
        if len(index) > 1
        else np.array([], dtype=np.int64)
    )
    if len(steps) > 1:
        raise ValueError(f"{label} time axis is not evenly spaced")
    step_min = int(steps[0] // 60) if len(steps) else (60 if label == "hourly" else 15)
    return {"start": index[0].isoformat(), "step_minutes": step_min, "count": int(len(index))}


def _stack(series: list[pd.Series], index: pd.Index, label: str) -> np.ndarray:
    """Stack aligned series into a (runs x steps) float matrix; an empty series counts as zeros."""
    rows = []
    for s in series:
        if len(s) == 0:
            rows.append(np.zeros(len(index)))
            continue
        if len(s) != len(index) or not s.index.equals(index):
            raise ValueError(f"{label}: every run must share the same time axis and length ({len(index)} steps)")
        rows.append(s.to_numpy(dtype=float))
    m = np.vstack(rows)
    if not np.isfinite(m).all():
        raise ValueError(f"{label}: profile contains NaN or non-finite values")
    return m


def _seasonal(matrix: np.ndarray, index: pd.DatetimeIndex) -> dict[str, dict[str, list[float | None]]]:
    """Per run, the mean load at each hour of day within each season; then the envelope across runs."""
    months = index.month.to_numpy()
    hours = index.hour.to_numpy()
    out: dict[str, dict[str, list[float | None]]] = {}
    for season, ms in _SEASON_MONTHS.items():
        in_season = np.isin(months, ms)
        per_run = np.full((matrix.shape[0], 24), np.nan)
        for h in range(24):
            mask = in_season & (hours == h)
            if mask.any():
                per_run[:, h] = matrix[:, mask].mean(axis=1)
        if np.isnan(per_run).all():
            out[season] = {k: [None] * 24 for k in ("mean_kw", "p10_kw", "p50_kw", "p90_kw")}
            continue
        with np.errstate(all="ignore"):
            out[season] = _envelope(per_run)
    return out


def build_results(ensemble: EnsembleResult) -> ResultsModel:
    """Compute the results document from the feasible runs of ``ensemble`` (see module docstring)."""
    runs: list[NeighborhoodResult] = list(ensemble.runs)
    if not runs:
        return _empty()

    index = runs[0].total_profile.index
    time = {"hourly": _axis(index, "hourly"), "15min": None}
    total_h = _stack([r.total_profile for r in runs], index, "hourly total")
    res_h = _stack([r.residential_profile for r in runs], index, "hourly residential")
    com_h = _stack([r.commercial_profile for r in runs], index, "hourly commercial")

    env: dict[str, Any] = {
        "hourly": {
            "total": _envelope(total_h),
            "residential": _envelope(res_h),
            "commercial": _envelope(com_h),
        },
        "15min": None,
    }

    # --- 15-minute resolution (only when every run carries it) and the energy conservation check ---
    energy_check: dict[str, Any] = {"checked": False, "max_relative_difference": None, "tolerance": ENERGY_RTOL}
    if all(r.residential_profile_15min is not None and r.commercial_profile_15min is not None for r in runs):
        qi = next(
            (
                s.index
                for r in runs
                for s in (r.residential_profile_15min, r.commercial_profile_15min)
                if s is not None and len(s) > 0
            ),
            None,
        )
        if qi is not None:
            time["15min"] = _axis(qi, "15min")
            res_q = _stack([r.residential_profile_15min for r in runs], qi, "15-minute residential")  # type: ignore[misc]
            com_q = _stack([r.commercial_profile_15min for r in runs], qi, "15-minute commercial")  # type: ignore[misc]
            total_q = res_q + com_q
            e_q = total_q.sum(axis=1)
            e_h = total_h.sum(axis=1)
            rel = np.abs(e_q - e_h) / np.maximum(np.abs(e_h), _TINY)
            worst = float(rel.max())
            if worst > ENERGY_RTOL:
                raise ValueError(
                    f"energy not conserved: 15-minute and hourly annual energy differ by {worst:.3e} "
                    f"(relative, run {runs[int(rel.argmax())].run_index}); tolerance {ENERGY_RTOL:g}"
                )
            energy_check = {"checked": True, "max_relative_difference": worst, "tolerance": ENERGY_RTOL}
            # kWh per 15-minute interval -> kW
            env["15min"] = {
                "total": _envelope(total_q, 4.0),
                "residential": _envelope(res_q, 4.0),
                "commercial": _envelope(com_q, 4.0),
            }

    # --- per building type (composition category), hourly ---
    sources: dict[str, str] = {}
    for r in runs:
        for sel in r.selections:
            sources.setdefault(sel.category, sel.source)
    categories = list(dict.fromkeys(c for r in runs for c in r.category_profiles))
    by_type: dict[str, Any] = {}
    for cat in categories:
        m = _stack([r.category_profiles.get(cat, pd.Series(dtype=float)) for r in runs], index, f"hourly {cat}")
        by_type[cat] = {
            "source": sources.get(cat),
            "mean_annual_energy_kwh": float(m.sum(axis=1).mean()),
            "hourly": _envelope(m),
        }

    # --- load-duration curves (hourly): each run sorted high to low, then the envelope across runs ---
    ldc = {
        name: _envelope(np.sort(mat, axis=1)[:, ::-1])
        for name, mat in (("total", total_h), ("residential", res_h), ("commercial", com_h))
    }

    # --- seasonal average daily profiles by load type ---
    seasonal = {
        "seasons": {k: list(v) for k, v in SEASONS.items()},
        "hours": list(range(24)),
        "profiles": {
            name: _seasonal(mat, index)  # type: ignore[arg-type]
            for name, mat in (("total", total_h), ("residential", res_h), ("commercial", com_h))
        },
    }

    # --- scalar metrics across runs ---
    scalars = {
        "peak_kw": _stats(np.array([r.peak_kw for r in runs], dtype=float)),
        "annual_energy_kwh": _stats(np.array([r.annual_energy_kwh for r in runs], dtype=float)),
        "load_factor": _stats(np.array([r.load_factor for r in runs], dtype=float)),
        "annual_energy_residential_kwh": _stats(res_h.sum(axis=1)),
        "annual_energy_commercial_kwh": _stats(com_h.sum(axis=1)),
    }

    return ResultsModel(
        {
            "schema_version": SCHEMA_VERSION,
            "units": {"power": "kW", "energy": "kWh"},
            "n_runs": len(runs),
            "percentiles": list(PERCENTILES),
            "time": time,
            "envelopes": env,
            "by_building_type": by_type,
            "load_duration": {"hourly": ldc},
            "seasonal_daily": seasonal,
            "scalars": scalars,
            "energy_check": energy_check,
        }
    )
