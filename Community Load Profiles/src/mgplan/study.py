"""A Study chains Neighborhood -> load -> DER adjustments into one reproducible result."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

import pandas as pd

from mgplan.core.types import LoadResult, Neighborhood
from mgplan.der.base import DERModel, DERResult
from mgplan.load.base import LoadSource


@dataclass
class StudyResult:
    neighborhood: Neighborhood
    load: LoadResult
    der: list[DERResult]
    net_load_kw: pd.Series

    @property
    def net_peak_kw(self) -> float:
        return float(self.net_load_kw.max())


@dataclass
class Study:
    neighborhood: Neighborhood
    load_source: LoadSource
    der_models: Sequence[DERModel] = field(default_factory=tuple)

    def run(self) -> StudyResult:
        load = self.load_source.profile(self.neighborhood)
        results = [m.simulate(self.neighborhood, load) for m in self.der_models]
        net = load.profile_kw.copy()
        for r in results:
            if not r.net_kw.index.equals(net.index):
                raise ValueError(f"DER '{r.name}' index does not match the load profile index")
            net = net + r.net_kw
        return StudyResult(self.neighborhood, load, results, net)
