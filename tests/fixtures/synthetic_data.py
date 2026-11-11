"""Synthetic ResStock/ComStock dataset generator (FR-DATA-2).

Writes a tiny, fully synthetic dataset in the REAL on-disk layout the engine reads. No real NREL data is used.
All values are made up; only the column names and file layout mimic the products.

Layout written under ``root``::

    background/chars_resstock.xlsx                  sheet "building_characteristics"
    background/chars_comstock.xlsx                  sheet "building_characteristics"
    input/resstock/timeseries_individual_buildings/upgrade=0/state=DC/<bldg_id>-0.parquet
    input/comstock/timeseries_individual_buildings/upgrade=0/state=DC/<bldg_id>-0.parquet

Deliberately bad ResStock rows (completed_status != "Success", applicability False) are included so that
``require_data_complete`` can be tested; their ids are in ``SyntheticDataset.bad_resstock_ids``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

RESSTOCK_KWH_COL = "out.electricity.total.energy_consumption..kwh"
COMSTOCK_KWH_COL = "out.electricity.total.energy_consumption"

DEFAULT_RESSTOCK_BY_HEIGHT: dict[str, int] = {
    "Single-Family Detached": 3,
    "Multi-Family with 2 - 4 Units": 3,
    "Multi-Family with 5+ Units, 4-7 Stories": 3,
}
DEFAULT_COMSTOCK_BY_TYPE: dict[str, int] = {"SmallOffice": 2, "MediumOffice": 1, "PrimarySchool": 1}

_MF_UNITS = {
    "Multi-Family with 2 - 4 Units": (2, 4),
    "Multi-Family with 5+ Units, 1-3 Stories": (5, 20),
    "Multi-Family with 5+ Units, 4-7 Stories": (8, 40),
    "Multi-Family with 5+ Units, 8+ Stories": (30, 120),
}
_STORIES = {
    "Single-Family Detached": (1, 2),
    "Single-Family Attached": (2, 3),
    "Multi-Family with 2 - 4 Units": (2, 3),
    "Multi-Family with 5+ Units, 1-3 Stories": (2, 3),
    "Multi-Family with 5+ Units, 4-7 Stories": (4, 7),
    "Multi-Family with 5+ Units, 8+ Stories": (8, 12),
}
_END_USES = ("cooling", "heating", "lighting", "plug_loads")
_RESSTOCK_ID0 = 1000
_COMSTOCK_ID0 = 500000


@dataclass
class SyntheticDataset:
    root: Path
    resstock_chars: Path
    comstock_chars: Path
    resstock_dir: Path  # .../timeseries_individual_buildings (contains upgrade=0/state=DC/)
    comstock_dir: Path
    background_dir: Path
    resstock_ids: list[int] = field(default_factory=list)  # good rows only
    comstock_ids: list[int] = field(default_factory=list)
    bad_resstock_ids: list[int] = field(default_factory=list)
    seed: int = 0
    year: int = 2018

    @property
    def data_dirs(self) -> dict[str, Path]:
        """The ``data_dirs`` argument the loader and ensemble expect."""
        return {"resstock": self.resstock_dir, "comstock": self.comstock_dir}

    @property
    def data(self) -> dict[str, Any]:
        """A ``data:`` section for ScenarioConfig (absolute paths, so no cwd assumptions)."""
        return {
            "upgrade": "0",
            "state": "DC",
            "resstock_chars_file": self.resstock_chars.name,
            "comstock_chars_file": self.comstock_chars.name,
            "resstock_timeseries_dir": str(self.resstock_dir),
            "comstock_timeseries_dir": str(self.comstock_dir),
            "background_dir": str(self.background_dir),
        }


def _profile(
    rng: np.random.Generator, index: pd.DatetimeIndex, scale: float, commercial: bool
) -> dict[str, np.ndarray]:
    hour = np.asarray(index.hour + index.minute / 60.0, dtype=float)
    doy = np.asarray(index.day_of_year, dtype=float)
    weekend = np.asarray(index.day_of_week >= 5)
    if commercial:
        diurnal = np.where((hour >= 8) & (hour < 18) & ~weekend, 1.0, 0.25)
    else:
        diurnal = 0.5 + 0.35 * np.exp(-((hour - 19.0) ** 2) / 8.0) + 0.15 * np.exp(-((hour - 7.5) ** 2) / 4.0)
    cool_season = np.clip(np.sin(np.pi * (doy - 120) / 160.0), 0, None)  # peaks mid-summer
    heat_season = (np.cos(2 * np.pi * (doy - 15) / 365.0) + 1) / 2
    noise = rng.uniform(0.9, 1.1, size=(len(index), 4))
    base = scale * 0.02
    cooling = base * 4 * cool_season * diurnal * noise[:, 0]
    heating = base * 3 * heat_season**2 * diurnal * noise[:, 1]
    lighting = base * 1.5 * diurnal * noise[:, 2]
    plug = base * 3 * diurnal * noise[:, 3]
    total = cooling + heating + lighting + plug + base * 2 + 1e-4
    return {"total": total, "cooling": cooling, "heating": heating, "lighting": lighting, "plug_loads": plug}


def _write_building(path: Path, rng: np.random.Generator, index: pd.DatetimeIndex, scale: float, product: str) -> None:
    p = _profile(rng, index, scale, commercial=product == "comstock")
    total_col = RESSTOCK_KWH_COL if product == "resstock" else COMSTOCK_KWH_COL
    # Rounded to 4 decimals so the files compress well (fixture stays tiny); values stay positive.
    df = pd.DataFrame({"timestamp": index, total_col: np.round(p["total"], 4).astype("float32")})
    for eu in _END_USES:
        name = f"out.electricity.{eu}.energy_consumption" + ("..kwh" if product == "resstock" else "")
        df[name] = np.round(p[eu] + 1e-4, 4).astype("float32")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, compression="zstd")


def build_synthetic_dataset(
    root: Path,
    *,
    resstock_by_height: dict[str, int] | None = None,
    comstock_by_type: dict[str, int] | None = None,
    seed: int = 0,
    year: int = 2018,
) -> SyntheticDataset:
    """Write the synthetic dataset under ``root`` and return its paths. Deterministic for a given ``seed``."""
    resstock_by_height = DEFAULT_RESSTOCK_BY_HEIGHT if resstock_by_height is None else resstock_by_height
    comstock_by_type = DEFAULT_COMSTOCK_BY_TYPE if comstock_by_type is None else comstock_by_type
    rng = np.random.default_rng(seed)
    index = pd.date_range(f"{year}-01-01", f"{year + 1}-01-01", freq="15min", inclusive="left")

    background = root / "background"
    res_dir = root / "input" / "resstock" / "timeseries_individual_buildings"
    com_dir = root / "input" / "comstock" / "timeseries_individual_buildings"
    background.mkdir(parents=True, exist_ok=True)

    res_rows: list[dict[str, Any]] = []
    bad_ids: list[int] = []
    next_id = _RESSTOCK_ID0
    heat_fuels = ["Electricity", "Natural Gas", "Fuel Oil"]
    vintages = ["<1940", "1940s", "1960s", "1980s", "2000s", "2010s"]
    hvacs = ["Ducted Heating", "Ducted Heat Pump", "Non-Ducted Heating"]

    def res_row(height: str, status: str, applicable: bool) -> dict[str, Any]:
        nonlocal next_id
        if height in _MF_UNITS:
            lo, hi = _MF_UNITS[height]
            units = float(rng.integers(lo, hi + 1))
        else:
            units = float("nan")
        s_lo, s_hi = _STORIES[height]
        hvac = str(rng.choice(hvacs))
        row = {
            "bldg_id": next_id,
            "completed_status": status,
            "applicability": applicable,
            "in.geometry_building_type_height": height,
            "in.geometry_building_number_units_mf": units,
            "in.sqft..ft2": float(rng.choice([650, 900, 1200, 1800, 2400, 3500])),
            "in.hvac_heating_type": hvac,
            "in.geometry_garage": "1 Car" if rng.random() < 0.3 else np.nan,
            "in.geometry_stories": int(rng.integers(s_lo, s_hi + 1)),
            "in.vintage": str(rng.choice(vintages)),
            "in.heating_fuel": str(rng.choice(heat_fuels)),
            "in.has_ev": bool(rng.random() < 0.15),
            "in.has_heat_pump": hvac == "Ducted Heat Pump",
        }
        next_id += 1
        return row

    for height, n in resstock_by_height.items():
        if height not in _STORIES:
            raise ValueError(f"unknown ResStock building type height {height!r}; known: {sorted(_STORIES)}")
        for _ in range(n):
            res_rows.append(res_row(height, "Success", True))
    if resstock_by_height:
        # Bad rows go in the 2-4 unit multifamily type when present (the type the standard tests filter on).
        first = (
            "Multi-Family with 2 - 4 Units"
            if "Multi-Family with 2 - 4 Units" in resstock_by_height
            else next(iter(resstock_by_height))
        )
        for status, applicable in (("Fail", True), ("Success", False)):
            row = res_row(first, status, applicable)
            bad_ids.append(int(row["bldg_id"]))
            res_rows.append(row)
    res_df = pd.DataFrame(res_rows)

    com_rows: list[dict[str, Any]] = []
    cid = _COMSTOCK_ID0
    for btype, n in comstock_by_type.items():
        for _ in range(n):
            com_rows.append(
                {
                    "bldg_id": cid,
                    "completed_status": "Success",
                    "applicability": True,
                    "in.comstock_building_type": btype,
                    "in.sqft..ft2": float(rng.choice([5000, 20000, 60000, 150000])),
                    "in.number_stories": int(rng.integers(1, 6)),
                    "in.vintage": str(rng.choice(vintages)),
                    "in.heating_fuel": str(rng.choice(heat_fuels)),
                }
            )
            cid += 1
    com_df = pd.DataFrame(com_rows)

    res_xlsx = background / "chars_resstock.xlsx"
    com_xlsx = background / "chars_comstock.xlsx"
    res_df.to_excel(res_xlsx, sheet_name="building_characteristics", index=False)
    com_df.to_excel(com_xlsx, sheet_name="building_characteristics", index=False)

    for _, r in res_df.iterrows():
        scale = float(r["in.sqft..ft2"]) / 1500.0 * 5.0
        path = res_dir / "upgrade=0" / "state=DC" / f"{int(r['bldg_id'])}-0.parquet"
        _write_building(path, rng, index, scale, "resstock")
    for _, r in com_df.iterrows():
        scale = float(r["in.sqft..ft2"]) / 5000.0 * 10.0
        path = com_dir / "upgrade=0" / "state=DC" / f"{int(r['bldg_id'])}-0.parquet"
        _write_building(path, rng, index, scale, "comstock")

    good = ~res_df["bldg_id"].isin(bad_ids) if len(res_df) else []
    return SyntheticDataset(
        root=root,
        resstock_chars=res_xlsx,
        comstock_chars=com_xlsx,
        resstock_dir=res_dir,
        comstock_dir=com_dir,
        background_dir=background,
        resstock_ids=[int(i) for i in res_df.loc[good, "bldg_id"]] if len(res_df) else [],
        comstock_ids=[int(i) for i in com_df["bldg_id"]] if len(com_df) else [],
        bad_resstock_ids=bad_ids,
        seed=seed,
        year=year,
    )
