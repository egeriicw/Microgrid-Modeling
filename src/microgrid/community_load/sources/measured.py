"""Load from metered / utility-supplied data (the 'additional load data' path)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from microgrid.core.types import LoadResult, Neighborhood


class MeasuredLoadSource:
    """Reads a CSV with a timestamp column and one kW column; resamples to hourly mean.

    Measured data describes the whole feeder/area already, so the Neighborhood is
    recorded in metadata but does not change the profile.
    """

    def __init__(self, csv_path: Path | str, time_col: str = "timestamp", kw_col: str = "kw"):
        self.csv_path, self.time_col, self.kw_col = Path(csv_path), time_col, kw_col

    def profile(self, neighborhood: Neighborhood) -> LoadResult:
        df = pd.read_csv(self.csv_path, parse_dates=[self.time_col])
        if self.kw_col not in df.columns:
            raise ValueError(f"{self.csv_path}: missing column '{self.kw_col}'")
        series = df.set_index(self.time_col)[self.kw_col].astype(float).sort_index()
        if series.index.has_duplicates:
            raise ValueError(f"{self.csv_path}: duplicate timestamps")
        if series.isna().any():
            raise ValueError(f"{self.csv_path}: {int(series.isna().sum())} missing kW values")
        hourly = series.resample("1h").mean()
        return LoadResult(
            profile_kw=hourly,
            source="measured",
            metadata={"file": str(self.csv_path), "neighborhood": neighborhood.name},
        )
