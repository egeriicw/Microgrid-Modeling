from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import pandas as pd

@dataclass(frozen=True)
class WeatherSeries:
    df: pd.DataFrame

def _first(df: pd.DataFrame, cands: list[str]) -> Optional[str]:
    for c in cands:
        if c in df.columns:
            return c
    return None

def read_weather_csv_auto(path: Path, preferred_units: str = "C", source_label: Optional[str] = None) -> WeatherSeries:
    if not path.exists():
        raise FileNotFoundError(f"Weather file not found: {path}")
    df = pd.read_csv(path)
    dt_col = _first(df, ["DATE","Date","datetime","timestamp","time"])
    if dt_col:
        df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
        df = df.dropna(subset=[dt_col]).set_index(dt_col)
    else:
        df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    tcol = _first(df, ["HourlyDryBulbTemperature","TEMP","Temp","temperature","Temperature","TAVG","DryBulbCelsius","DryBulbFahrenheit"])
    if tcol is None:
        raise ValueError("No temperature column found in weather CSV.")
    s = pd.to_numeric(df[tcol], errors="coerce").dropna()
    out = pd.DataFrame({"outdoor_air_temperature_raw": s}, index=s.index)
    units = preferred_units.upper().strip()
    temp = out["outdoor_air_temperature_raw"]
    if units == "C":
        if temp.median() > 45:
            temp = (temp - 32.0) * (5.0/9.0)
    elif units == "F":
        if temp.median() < 35:
            temp = temp * (9.0/5.0) + 32.0
    else:
        raise ValueError("preferred_units must be C or F")
    out = pd.DataFrame({"outdoor_air_temperature": temp}, index=out.index).resample("h").mean(numeric_only=True)
    if source_label:
        out["weather_source"] = source_label
    return WeatherSeries(df=out)
