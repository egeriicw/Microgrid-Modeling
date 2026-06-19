"""Archetype filtering — Section 1 of the methodology.

Produces ArchetypePool (A_filtered) from raw building characteristics
DataFrames. Does not touch parquet files; all I/O lives in loader.py.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from snlg._types import ArchetypePool, BuildingId
from snlg.config import (
    ArchetypeFilterConfig,
    CommercialFilterConfig,
    ResidentialFilterConfig,
    ScenarioConfig,
)

# Building types that use unit-count scaling
MF_BUILDING_TYPES = [
    "Multi-Family with 2 - 4 Units",
    "Multi-Family with 5+ Units, 1-3 Stories",
    "Multi-Family with 5+ Units, 4-7 Stories",
    "Multi-Family with 5+ Units, 8+ Stories",
]


def load_resstock_characteristics(path: Path) -> pd.DataFrame:
    """Read the ResStock building characteristics Excel file."""
    return pd.read_excel(path, sheet_name="building_characteristics")


def load_comstock_characteristics(path: Path) -> pd.DataFrame:
    """Read the ComStock building characteristics Excel file."""
    return pd.read_excel(path, sheet_name="building_characteristics")


def _filter_resstock_category(
    df: pd.DataFrame, cat_cfg: ResidentialFilterConfig
) -> list[BuildingId]:
    """Apply one residential category's hard-gate filters, return bldg_id list."""
    mask = pd.Series(True, index=df.index)

    if cat_cfg.building_type_heights:
        mask &= df["in.geometry_building_type_height"].isin(cat_cfg.building_type_heights)

    if cat_cfg.hvac_types is not None:
        mask &= df["in.hvac_heating_type"].isin(cat_cfg.hvac_types)

    if cat_cfg.hvac_types_exclude is not None:
        mask &= ~df["in.hvac_heating_type"].isin(cat_cfg.hvac_types_exclude)

    if cat_cfg.max_sqft is not None:
        mask &= df["in.sqft..ft2"] <= cat_cfg.max_sqft

    if cat_cfg.require_no_garage:
        mask &= df["in.geometry_garage"].isnull()

    return df.loc[mask, "bldg_id"].tolist()


def _filter_comstock_category(
    df: pd.DataFrame, cat_cfg: CommercialFilterConfig
) -> list[BuildingId]:
    """Apply one commercial category's hard-gate filters, return bldg_id list."""
    mask = pd.Series(True, index=df.index)

    if cat_cfg.building_types:
        mask &= df["in.comstock_building_type"].isin(cat_cfg.building_types)

    if not cat_cfg.include_public:
        public_types = ["PrimarySchool", "SecondarySchool"]
        mask &= ~df["in.comstock_building_type"].isin(public_types)

    return df.loc[mask, "bldg_id"].tolist()


def build_archetype_pool(
    resstock_chars_path: Path,
    comstock_chars_path: Path,
    cfg: ScenarioConfig,
) -> ArchetypePool:
    """Load characteristics and apply all filters to produce A_filtered.

    Returns an ArchetypePool with per-category bldg_id lists for both
    residential and commercial archetypes.
    """
    res_df = load_resstock_characteristics(resstock_chars_path)
    com_df = load_comstock_characteristics(comstock_chars_path)

    filters: ArchetypeFilterConfig = cfg.archetype_filters

    residential: dict[str, list[BuildingId]] = {}
    for cat_name, cat_cfg in filters.resstock.items():
        pool = _filter_resstock_category(res_df, cat_cfg)
        residential[cat_name] = pool

    commercial: dict[str, list[BuildingId]] = {}
    for cat_name, cat_cfg in filters.comstock.items():
        pool = _filter_comstock_category(com_df, cat_cfg)
        commercial[cat_name] = pool

    return ArchetypePool(
        residential=residential,
        commercial=commercial,
        resstock_chars=res_df,
        comstock_chars=com_df,
        metadata={
            "resstock_chars_path": str(resstock_chars_path),
            "comstock_chars_path": str(comstock_chars_path),
            "n_resstock_raw": len(res_df),
            "n_comstock_raw": len(com_df),
            "category_sizes": {
                **{k: len(v) for k, v in residential.items()},
                **{k: len(v) for k, v in commercial.items()},
            },
        },
    )
