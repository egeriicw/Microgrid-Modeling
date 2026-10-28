"""Cross-cutting contracts shared by every layer."""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class LoadResult:
    """Output of any load source: central hourly profile plus optional uncertainty band."""
    profile_kw: pd.Series                   # DatetimeIndex, hourly, central estimate
    source: str                             # "snlg-ensemble", "measured", ...
    p10_kw: pd.Series | None = None
    p90_kw: pd.Series | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def peak_kw(self) -> float:
        return float(self.profile_kw.max())

    @property
    def annual_kwh(self) -> float:
        return float(self.profile_kw.sum())  # hourly kW * 1 h
