"""Outdoor temperature series that goes with a run's load (FR-ENG-10), so charts can show load and temperature together.

The source is DECLARED in the config (``data.temperature_source`` and friends), never guessed, because where the
real NREL ResStock/ComStock data keeps its temperature is not confirmed **[verify]**: it may be a column in each
building's timeseries parquet, or a separate weather file per dataset/location. Two declared kinds are supported:

``timeseries_column``
    ``data.temperature_column`` in every sampled building's parquet (timestamps from its ``timestamp`` column).
    Every distinct sampled building (of the feasible runs) is read and compared with the first. Within one DC
    dataset they are expected to share a weather location **[verify]**; if any two differ by more than
    ``CONSISTENCY_TOLERANCE_C`` the run FAILS with a message naming them. The engine never picks or averages one
    silently: choosing "the" temperature for buildings on different weather is a methodology decision.
``weather_file``
    One CSV or parquet file (``data.temperature_weather_file``) with ``data.temperature_time_column`` and
    ``data.temperature_column``. It is assumed to apply to every sampled building **[verify]**.

``none`` (the default) gives a results block with ``available: false`` and the reason, never a silent omission.

Alignment (exact time stamps, no shifting): values are converted to deg C (``F`` is converted, ``C`` kept), then

* a 15-minute source gives the 15-minute series directly and the hourly series as the mean of the four 15-minute
  values labelled within each hour (the same grouping that sums the hourly load);
* an hourly source gives the hourly series directly and the 15-minute series by holding each hour's value for its
  four intervals (no interpolation).

Any load time stamp the source lacks, duplicate or irregular source time stamps, time-zone-aware time stamps, and
missing or non-finite values all fail loudly. Whether NREL time stamps mark the start or end of an interval, and
whether load and weather use the same convention, is not confirmed **[verify]**.

Reading temperature never touches the load numbers: it runs after the ensemble, from the files only.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from microgrid.community_load.synthetic._types import NeighborhoodResult
from microgrid.community_load.synthetic.config import ScenarioConfig
from microgrid.community_load.synthetic.loader import load_building_profile

UNITS = "degC"
#: Two buildings' series closer than this (deg C, everywhere) count as the same weather. Placeholder chosen by the
#: engineer: float32 storage of one shared series gives exactly 0; a real disagreement is far larger.
CONSISTENCY_TOLERANCE_C = 0.01
_Q = pd.Timedelta(minutes=15)
_H = pd.Timedelta(hours=1)


def not_available(reason: str, source: dict[str, Any] | None = None) -> dict[str, Any]:
    """The temperature block for a result that has no temperature, saying why."""
    return {
        "available": False,
        "reason": reason,
        "units": UNITS,
        "source": source or {"kind": "none"},
        "source_resolution": None,
        "hourly": None,
        "15min": None,
        "derivation": None,
        "consistency": None,
    }


def _declared(cfg: ScenarioConfig) -> dict[str, Any]:
    d = cfg.data
    if d.temperature_source == "none":
        return {"kind": "none"}
    return {
        "kind": d.temperature_source,
        "column": d.temperature_column,
        "declared_units": d.temperature_units,
        # A file NAME only: results go to the browser and must not reveal server paths.
        "file": Path(d.temperature_weather_file).name if d.temperature_weather_file else None,
        "time_column": d.temperature_time_column,
    }


def _to_celsius(values: np.ndarray, units: str) -> np.ndarray:
    v = np.asarray(values, dtype="float64")
    return (v - 32.0) * 5.0 / 9.0 if units == "F" else v


def _series(times: Any, values: Any, units: str, where: str) -> pd.Series:
    idx = pd.DatetimeIndex(pd.to_datetime(times))
    if idx.tz is not None:
        raise ValueError(
            f"{where}: time stamps carry a time zone ({idx.tz}) but load time stamps do not; "
            "the time convention must be declared and matched first [verify]"
        )
    if idx.has_duplicates:
        raise ValueError(f"{where}: duplicate time stamps (first: {idx[idx.duplicated()][0]})")
    s = pd.Series(_to_celsius(np.asarray(values), units), index=idx).sort_index()
    return s


def _resolution(s: pd.Series, where: str) -> str:
    if len(s) < 2:
        raise ValueError(f"{where}: needs at least two time stamps to tell its resolution")
    steps = set(pd.Series(s.index).diff().dropna().unique())
    if steps == {_Q}:
        return "15min"
    if steps == {_H}:
        return "hourly"
    raise ValueError(
        f"{where}: time stamps must be evenly spaced at 15 minutes or 1 hour, found steps "
        f"{sorted(str(pd.Timedelta(x)) for x in steps)[:5]}"
    )


def _take(s: pd.Series, stamps: pd.DatetimeIndex, where: str) -> np.ndarray:
    missing = ~stamps.isin(s.index)
    if missing.any():
        raise ValueError(
            f"{where} does not cover the load period: {int(missing.sum())} of {len(stamps)} needed time stamps "
            f"missing (first: {stamps[missing][0]})"
        )
    v = s.reindex(stamps).to_numpy(dtype="float64")
    bad = ~np.isfinite(v)
    if bad.any():
        raise ValueError(
            f"{where}: missing or non-finite temperature values at {int(bad.sum())} needed time stamps "
            f"(first: {stamps[bad][0]})"
        )
    return v


def _align(
    s: pd.Series, hourly_idx: pd.DatetimeIndex, q_idx: pd.DatetimeIndex | None, where: str
) -> tuple[str, np.ndarray, np.ndarray | None, dict[str, str | None]]:
    res = _resolution(s, where)
    if res == "15min":
        offsets = np.tile(np.arange(4) * np.timedelta64(15, "m"), len(hourly_idx))
        quarters = pd.DatetimeIndex(np.repeat(hourly_idx.to_numpy(), 4) + offsets)
        hourly = _take(s, quarters, where).reshape(-1, 4).mean(axis=1)
        q = None if q_idx is None else _take(s, q_idx, where)
        deriv = {
            "hourly": "mean of the four 15-minute source values labelled within each hour (same grouping as load)",
            "15min": None if q is None else "source values",
        }
    else:
        hourly = _take(s, hourly_idx, where)
        q = None if q_idx is None else _take(s, q_idx.floor("h"), where)
        deriv = {
            "hourly": "source values",
            "15min": None
            if q is None
            else "hourly value held for each 15-minute interval of its hour (no interpolation)",
        }
    return res, hourly, q, deriv


def _load_axes(runs: list[NeighborhoodResult]) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex | None]:
    """The time axes the results document uses (``results.time``): hourly total, and 15-minute when present."""
    hourly = runs[0].total_profile.index
    if not isinstance(hourly, pd.DatetimeIndex) or len(hourly) == 0:
        raise ValueError("temperature: the hourly load profile needs a non-empty DatetimeIndex")
    if not all(r.residential_profile_15min is not None and r.commercial_profile_15min is not None for r in runs):
        return hourly, None
    q = next(
        (
            s.index
            for r in runs
            for s in (r.residential_profile_15min, r.commercial_profile_15min)
            if s is not None and len(s) > 0
        ),
        None,
    )
    if q is not None and not isinstance(q, pd.DatetimeIndex):
        raise ValueError("temperature: the 15-minute load profile needs a DatetimeIndex")
    return hourly, q


def _from_buildings(
    cfg: ScenarioConfig, runs: list[NeighborhoodResult], data_dirs: dict[str, Path]
) -> tuple[pd.Series, dict[str, Any]]:
    col = str(cfg.data.temperature_column)
    units = str(cfg.data.temperature_units)
    keys = list(dict.fromkeys((sel.source, sel.bldg_id) for r in runs for sel in r.selections))
    ref: pd.Series | None = None
    ref_key = ""
    worst = 0.0
    for source, bldg_id in keys:
        where = f"building {source}/{bldg_id}"
        df = load_building_profile(bldg_id, source, data_dirs, cfg.data.upgrade, cfg.data.state)
        if col not in df.columns:
            hints = [c for c in df.columns if "temp" in str(c).lower() or "drybulb" in str(c).lower()]
            raise ValueError(
                f"temperature column {col!r} not found in {where} timeseries (declared by data.temperature_column; "
                f"real NREL column names are not confirmed [verify]); columns that look like temperature: {hints}"
            )
        s = _series(df["timestamp"], df[col].to_numpy(), units, where)
        if ref is None:
            ref, ref_key = s, where
            continue
        if not s.index.equals(ref.index):
            raise ValueError(f"{where} and {ref_key} have different temperature time stamps")
        diff = float(np.nanmax(np.abs(s.to_numpy() - ref.to_numpy()))) if len(s) else 0.0
        if not math.isfinite(diff) or diff > CONSISTENCY_TOLERANCE_C:
            raise ValueError(
                f"sampled buildings carry different temperature series: {where} differs from {ref_key} by up to "
                f"{diff:.3f} deg C (tolerance {CONSISTENCY_TOLERANCE_C} deg C). The engine does not pick or average "
                "one silently; which temperature to report for buildings on different weather is a methodology "
                "decision for the owner"
            )
        worst = max(worst, diff)
    assert ref is not None  # runs are non-empty and every run has at least one building
    return ref, {
        "n_series_compared": len(keys),
        "max_abs_difference_c": worst,
        "tolerance_c": CONSISTENCY_TOLERANCE_C,
        "note": "every distinct sampled building's series compared with the first",
    }


def _from_file(cfg: ScenarioConfig) -> tuple[pd.Series, dict[str, Any]]:
    path = Path(str(cfg.data.temperature_weather_file))
    where = f"weather file {path.name}"
    if not path.is_file():
        raise FileNotFoundError(f"weather file {path} (data.temperature_weather_file) does not exist")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"{where}: unsupported format {suffix!r}; use .csv or .parquet")
    for fld in ("temperature_time_column", "temperature_column"):
        name = getattr(cfg.data, fld)
        if name not in df.columns:
            kind = "time" if fld == "temperature_time_column" else "temperature"
            raise ValueError(
                f"{kind} column {name!r} not found in {where} (declared by data.{fld}; real NREL weather file "
                f"columns are not confirmed [verify]); columns: {list(df.columns)[:20]}"
            )
    s = _series(
        df[cfg.data.temperature_time_column],
        df[cfg.data.temperature_column].to_numpy(),
        str(cfg.data.temperature_units),
        where,
    )
    return s, {
        "n_series_compared": 1,
        "max_abs_difference_c": 0.0,
        "tolerance_c": CONSISTENCY_TOLERANCE_C,
        "note": "one weather file declared for all sampled buildings; that it applies to all of them is assumed "
        "[verify]",
    }


def build_temperature(
    cfg: ScenarioConfig, runs: list[NeighborhoodResult], data_dirs: dict[str, Path]
) -> dict[str, Any]:
    """The ``results.temperature`` block for the feasible ``runs`` (see module docstring). Fails loudly."""
    declared = _declared(cfg)
    if cfg.data.temperature_source == "none":
        return not_available(
            "no temperature source declared (data.temperature_source is 'none'); old data, or the source is not "
            "yet confirmed",
            declared,
        )
    if not runs:
        return not_available("no feasible runs, so there is no load time axis to align temperature with", declared)
    hourly_idx, q_idx = _load_axes(runs)
    if cfg.data.temperature_source == "timeseries_column":
        s, consistency = _from_buildings(cfg, runs, data_dirs)
        where = f"temperature column {cfg.data.temperature_column!r}"
    else:
        s, consistency = _from_file(cfg)
        where = f"weather file {declared['file']}"
    resolution, hourly, q, derivation = _align(s, hourly_idx, q_idx, where)
    return {
        "available": True,
        "reason": None,
        "units": UNITS,
        "source": declared,
        "source_resolution": resolution,
        "hourly": [float(x) for x in hourly],
        "15min": None if q is None else [float(x) for x in q],
        "derivation": derivation,
        "consistency": consistency,
    }
