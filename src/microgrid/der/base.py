from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

import pandas as pd

from microgrid.core.types import LoadResult, Neighborhood


@dataclass
class DERResult:
    """Hourly kW contribution: positive = added load (EV), negative = generation/discharge."""
    name: str
    net_kw: pd.Series
    metadata: dict[str, object] = field(default_factory=dict)


class DERModel(Protocol):
    name: str

    def simulate(self, neighborhood: Neighborhood, load: LoadResult) -> DERResult: ...
