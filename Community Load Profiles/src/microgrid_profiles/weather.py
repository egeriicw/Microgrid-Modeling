"""Weather ingestion helpers.

The pipeline optionally joins hourly outdoor air temperature to the load profile
outputs. Input weather data can come from various sources with different column
names and units.

This module provides a small auto-detection routine that:

- Parses a datetime column (if present).
- Finds a temperature column among common candidates.
- Converts to the requested units (C/F) using a heuristic based on typical
  median values.
- Resamples to hourly mean.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class WeatherSeries:
    """Container for normalized weather data.

    Attributes:
        df: Hourly DataFrame indexed by datetime. Includes:
            - ``outdoor_air_temperature`` (float)
            - Optionally ``weather_source`` (str)
    """

    df: pd.DataFrame


def _first(df: pd.DataFrame, cands: list[str]) -> Optional[str]:
    """Return the first candidate column name found in a DataFrame."""

    for c in cands:
        if c in df.columns:
            return c
    return None


def read_weather_csv_auto(
    path: Path,
    preferred_units: str = "C",
    source_label: Optional[str] = None,
) -> WeatherSeries:
    """Read a weather CSV and normalize to hourly outdoor air temperature.

    Args:
        path: Path to a CSV file.
        preferred_units: Desired output units: ``"C"`` or ``"F"``.
        source_label: Optional label stored in a ``weather_source`` column.

    Returns:
        A :class:`WeatherSeries` containing an hourly DataFrame.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        ValueError: If no temperature column is detected or units are invalid.
    """

    if not path.exists():
        raise FileNotFoundError(f"Weather file not found: {path}")

    df = pd.read_csv(path)

    dt_col = _first(df, ["DATE", "Date", "datetime", "timestamp", "time"])
    if dt_col:
        df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
        df = df.dropna(subset=[dt_col]).set_index(dt_col)
    else:
        # Fall back to assuming the index is already a datetime-like column.
        df.index = pd.to_datetime(df.index)

    df = df.sort_index()

    tcol = _first(
        df,
        [
            "HourlyDryBulbTemperature",
            "TEMP",
            "Temp",
            "temperature",
            "Temperature",
            "TAVG",
            "DryBulbCelsius",
            "DryBulbFahrenheit",
        ],
    )
    if tcol is None:
        raise ValueError("No temperature column found in weather CSV.")

    s = pd.to_numeric(df[tcol], errors="coerce").dropna()
    out = pd.DataFrame({"outdoor_air_temperature_raw": s}, index=s.index)

    units = preferred_units.upper().strip()
    temp = out["outdoor_air_temperature_raw"]

    # Heuristic unit conversion: if values look implausible for the requested
    # units, assume the opposite unit system.
    if units == "C":
        if temp.median() > 45:
            temp = (temp - 32.0) * (5.0 / 9.0)
    elif units == "F":
        if temp.median() < 35:
            temp = temp * (9.0 / 5.0) + 32.0
    else:
        raise ValueError("preferred_units must be C or F")

    out = pd.DataFrame({"outdoor_air_temperature": temp}, index=out.index).resample("h").mean(numeric_only=True)

    if source_label:
        out["weather_source"] = source_label

    return WeatherSeries(df=out)
