from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd
from .config import ScenarioConfig

@dataclass(frozen=True)
class ResidentialSelection:
    building_ids: list[Any]
    counts_by_type: pd.Series

@dataclass(frozen=True)
class CommercialSelection:
    building_ids: list[Any]
    counts_by_type: pd.Series

def _sample(rng: np.random.Generator, items: list[Any], k: int) -> list[Any]:
    if k <= 0 or not items:
        return []
    idx = rng.integers(0, len(items), size=k)
    return [items[i] for i in idx]

def construct_neighborhood_residential(cfg: ScenarioConfig, chars: pd.DataFrame, rng: np.random.Generator) -> ResidentialSelection:
    c = cfg.columns
    n = cfg.neighborhood
    total = n.total_buildings
    mf_count = int(round(total * n.multifamily_buildings_percent_of_total))
    chosen: list[Any] = []
    for name in n.multifamily_building_names:
        k = int(round(mf_count * float(n.multifamily_buildings_percentages.get(name, 0.0))))
        candidates = chars.loc[chars[c.resstock_building_type] == name, c.building_id_resstock].tolist()
        chosen.extend(_sample(rng, candidates, k))
    chosen_df = chars[chars[c.building_id_resstock].isin(chosen)]
    counts = chosen_df[c.resstock_building_type].value_counts()
    return ResidentialSelection(chosen, counts)

def construct_neighborhood_commercial(cfg: ScenarioConfig, chars: pd.DataFrame, rng: np.random.Generator) -> CommercialSelection:
    c = cfg.columns
    plan = [("LargeOffice",5),("MediumOffice",3),("RetailStandalone",6),("RetailStripmall",4),("Warehouse",7)]
    if cfg.include_public:
        plan += [("SmallHotel",1),("Hospital",1),("PrimarySchool",1),("SecondarySchool",1)]
    chosen: list[Any] = []
    for btype, k in plan:
        candidates = chars.loc[chars[c.comstock_building_type] == btype, c.building_id_comstock].tolist()
        chosen.extend(_sample(rng, candidates, k))
    chosen_df = chars[chars[c.building_id_comstock].isin(chosen)]
    counts = chosen_df[c.comstock_building_type].value_counts()
    return CommercialSelection(chosen, counts)

def building_parquet_path(timeseries_dir: Path, building_id: Any) -> Path:
    return timeseries_dir / f"{building_id}-0.parquet"
