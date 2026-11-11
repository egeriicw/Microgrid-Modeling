"""The synthetic dataset generator produces data the real engine functions can consume."""

from __future__ import annotations

import hashlib

import pandas as pd

from microgrid.community_load.synthetic import archetype, loader
from microgrid.community_load.synthetic._types import BuildingSelection
from microgrid.community_load.synthetic.config import (
    ArchetypeFilterConfig,
    CommercialFilterConfig,
    DataConfig,
    ResidentialFilterConfig,
    ScenarioConfig,
)
from tests.fixtures.synthetic_data import COMSTOCK_KWH_COL, RESSTOCK_KWH_COL, build_synthetic_dataset

MF = "Multi-Family with 2 - 4 Units"


def _cfg(ds, require_complete=True):
    return ScenarioConfig(
        data=DataConfig(**ds.data),
        archetype_filters=ArchetypeFilterConfig(
            resstock={
                "mf_small": ResidentialFilterConfig(building_type_heights=[MF], require_data_complete=require_complete)
            },
            comstock={"office": CommercialFilterConfig(building_types=["SmallOffice", "MediumOffice"])},
        ),
    )


def test_archetype_pool_non_empty(synthetic_dataset):
    pool = archetype.build_archetype_pool(
        synthetic_dataset.resstock_chars, synthetic_dataset.comstock_chars, _cfg(synthetic_dataset)
    )
    assert len(pool.residential["mf_small"]) == 3
    assert len(pool.commercial["office"]) == 3


def test_require_data_complete_excludes_bad_rows(synthetic_dataset):
    ds = synthetic_dataset
    assert len(ds.bad_resstock_ids) == 2
    strict = archetype.build_archetype_pool(ds.resstock_chars, ds.comstock_chars, _cfg(ds, True))
    loose = archetype.build_archetype_pool(ds.resstock_chars, ds.comstock_chars, _cfg(ds, False))
    assert not set(ds.bad_resstock_ids) & set(strict.residential["mf_small"])
    assert set(ds.bad_resstock_ids) <= set(loose.residential["mf_small"])


def test_load_and_scale_profile_resstock_and_comstock(synthetic_dataset):
    ds = synthetic_dataset
    cfg = _cfg(ds)
    chars = pd.read_excel(ds.resstock_chars, sheet_name="building_characteristics")
    pool = archetype.build_archetype_pool(ds.resstock_chars, ds.comstock_chars, cfg)
    rid = pool.residential["mf_small"][0]
    units = float(chars.loc[chars["bldg_id"] == rid, "in.geometry_building_number_units_mf"].iloc[0])
    prof = loader.load_and_scale_profile(
        BuildingSelection(bldg_id=rid, category="mf_small", source="resstock", unit_multiplier=units),
        ds.data_dirs,
        cfg,
        chars,
        MF,
    )
    assert len(prof.hourly_kwh) == 8760
    assert (prof.hourly_kwh > 0).all()
    cprof = loader.load_and_scale_profile(
        BuildingSelection(bldg_id=ds.comstock_ids[0], category="office", source="comstock", unit_multiplier=1.0),
        ds.data_dirs,
        cfg,
    )
    assert len(cprof.hourly_kwh) == 8760 and cprof.original_annual_kwh > 0


def test_parquet_columns_and_resolution(synthetic_dataset):
    ds = synthetic_dataset
    r = pd.read_parquet(ds.resstock_dir / "upgrade=0" / "state=DC" / f"{ds.resstock_ids[0]}-0.parquet")
    c = pd.read_parquet(ds.comstock_dir / "upgrade=0" / "state=DC" / f"{ds.comstock_ids[0]}-0.parquet")
    assert RESSTOCK_KWH_COL in r.columns and COMSTOCK_KWH_COL in c.columns
    assert len(r) == 35040
    assert (r["timestamp"].diff().dropna() == pd.Timedelta("15min")).all()
    assert sum(col.startswith("out.electricity.") for col in r.columns) >= 4


def test_chars_contain_extra_filter_columns(synthetic_dataset):
    df = pd.read_excel(synthetic_dataset.resstock_chars, sheet_name="building_characteristics")
    for col in (
        "completed_status",
        "applicability",
        "in.geometry_stories",
        "in.vintage",
        "in.heating_fuel",
        "in.has_ev",
        "in.has_heat_pump",
    ):
        assert col in df.columns


def _digest(root):
    h = hashlib.sha256()
    for p in sorted((root / "input").rglob("*.parquet")):
        h.update(p.read_bytes())
    return h.hexdigest()


def test_deterministic_by_seed(tmp_path):
    a = build_synthetic_dataset(tmp_path / "a", seed=5)
    b = build_synthetic_dataset(tmp_path / "b", seed=5)
    c = build_synthetic_dataset(tmp_path / "c", seed=6)
    assert _digest(a.root) == _digest(b.root)
    assert _digest(a.root) != _digest(c.root)


def test_fixture_is_small(synthetic_dataset):
    total = sum(p.stat().st_size for p in synthetic_dataset.root.rglob("*") if p.is_file())
    assert total < 15 * 1024 * 1024
