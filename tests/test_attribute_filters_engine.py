"""Attribute filters inside the engine: config parsing, archetype pool, preview, run record (FR-ENG-11)."""

from __future__ import annotations

import copy
import json

import pytest

from microgrid.community_load.synthetic import archetype, run
from microgrid.community_load.synthetic.attribute_filters import AttributeFilter
from microgrid.community_load.synthetic.config import config_from_dict
from microgrid.community_load.synthetic.pool_preview import PoolPreview, preview_pool, preview_pools
from tests.test_run_facade import build_tiny_config

MF_UNITS = "Multi-Family with 2 - 4 Units"


def _with_attrs(cfg: dict, category: str, attrs: list | None) -> dict:
    out = copy.deepcopy(cfg)
    out["archetype_filters"]["resstock"][category]["attributes"] = attrs
    return out


# ---- config parsing ----------------------------------------------------------------------------------------


def test_attributes_parse_into_typed_filters(synthetic_dataset):
    raw = _with_attrs(
        build_tiny_config(synthetic_dataset),
        "mf_small",
        [{"column": "in.geometry_stories", "op": "range", "min": 2}, {"column": "in.has_ev", "op": "is_true"}],
    )
    cfg = config_from_dict(raw)
    attrs = cfg.archetype_filters.resstock["mf_small"].attributes
    assert attrs == [AttributeFilter("in.geometry_stories", "range", min=2), AttributeFilter("in.has_ev", "is_true")]
    assert cfg.archetype_filters.resstock["mf_mid"].attributes == []


@pytest.mark.parametrize(
    "bad, msg",
    [
        ([{"colum": "x", "op": "not_null"}], r"archetype_filters\.resstock\.mf_small\.attributes\[0\]\.colum.*unknown"),
        ([{"column": "x", "op": "like"}], r"attributes\[0\].*unknown operator"),
        ([{"column": "x", "op": "range"}], r"attributes\[0\].*min or max"),
        ("stories>2", r"attributes.*list"),
        (["stories>2"], r"attributes\[0\].*mapping"),
    ],
)
def test_bad_attribute_config_is_loud_and_names_the_path(synthetic_dataset, bad, msg):
    with pytest.raises(ValueError, match=msg):
        config_from_dict(_with_attrs(build_tiny_config(synthetic_dataset), "mf_small", bad))


# ---- archetype pool ----------------------------------------------------------------------------------------


def _chars(ds):
    return archetype.load_resstock_characteristics(ds.resstock_chars)


def test_filter_narrows_pool_and_never_widens(synthetic_dataset):
    df = _chars(synthetic_dataset)
    base = archetype.ResidentialFilterConfig(building_type_heights=[MF_UNITS])
    ids = archetype._filter_resstock_category(df, base)
    narrowed = archetype.ResidentialFilterConfig(
        building_type_heights=[MF_UNITS], attributes=[AttributeFilter("in.geometry_stories", "range", min=3)]
    )
    got = archetype._filter_resstock_category(df, narrowed)
    assert set(got) <= set(ids)
    expected = df[(df["bldg_id"].isin(ids)) & (df["in.geometry_stories"] >= 3)]["bldg_id"].tolist()
    assert got == expected


def test_filter_on_missing_column_fails_naming_it(synthetic_dataset):
    df = _chars(synthetic_dataset)
    cfg = archetype.ResidentialFilterConfig(
        building_type_heights=[MF_UNITS], attributes=[AttributeFilter("in.no_such_column", "not_null")]
    )
    with pytest.raises(ValueError, match="in.no_such_column"):
        archetype._filter_resstock_category(df, cfg)


def test_comstock_attribute_filter(synthetic_dataset):
    df = archetype.load_comstock_characteristics(synthetic_dataset.comstock_chars)
    cfg = archetype.CommercialFilterConfig(
        building_types=["SmallOffice", "MediumOffice"], attributes=[AttributeFilter("in.number_stories", "range", min=3)]
    )
    got = archetype._filter_comstock_category(df, cfg)
    assert got == df[df["in.comstock_building_type"].isin(cfg.building_types) & (df["in.number_stories"] >= 3)][
        "bldg_id"
    ].tolist()


# ---- pool preview ------------------------------------------------------------------------------------------


def test_preview_counts_and_warnings(synthetic_dataset):
    df = _chars(synthetic_dataset)
    base = archetype.ResidentialFilterConfig(building_type_heights=[MF_UNITS])
    p = preview_pool(df, base, min_pool=2)
    assert isinstance(p, PoolPreview)
    assert p.matched == len(archetype._filter_resstock_category(df, base))
    assert p.total == len(df)
    assert p.warnings == []

    small = preview_pool(df, base, min_pool=10)
    assert any("fewer than 10" in w for w in small.warnings)  # tiny fixture pool

    zero = archetype.ResidentialFilterConfig(
        building_type_heights=[MF_UNITS], attributes=[AttributeFilter("in.geometry_stories", "range", min=99)]
    )
    z = preview_pool(df, zero)
    assert z.matched == 0
    assert any("no buildings" in w for w in z.warnings)


def test_preview_pools_per_category_default_minimum_is_ten(synthetic_dataset):
    cfg = config_from_dict(build_tiny_config(synthetic_dataset))
    out = preview_pools(_chars(synthetic_dataset), None, cfg.archetype_filters)
    assert set(out) == {"resstock/mf_small", "resstock/mf_mid"}
    assert cfg.archetype_filters.min_pool_size == 10


def test_min_pool_size_is_configurable_and_validated(synthetic_dataset):
    raw = build_tiny_config(synthetic_dataset)
    raw["archetype_filters"]["min_pool_size"] = 2
    assert config_from_dict(raw).archetype_filters.min_pool_size == 2
    raw["archetype_filters"]["min_pool_size"] = -1
    with pytest.raises(ValueError, match="min_pool_size"):
        config_from_dict(raw)


# ---- run wiring and no-methodology-change proof ------------------------------------------------------------


def _metrics(raw):
    return run.run_scenario(raw).metrics


def test_empty_attribute_list_reproduces_the_same_numbers(synthetic_dataset):
    base = build_tiny_config(synthetic_dataset)
    assert _metrics(base) == _metrics(_with_attrs(base, "mf_small", []))
    assert _metrics(base) == _metrics(_with_attrs(base, "mf_small", None))


def test_run_records_pool_summary_filters_and_warns(synthetic_dataset):
    raw = _with_attrs(
        build_tiny_config(synthetic_dataset), "mf_small", [{"column": "in.geometry_stories", "op": "range", "min": 2}]
    )
    events = []
    result = run.run_scenario(raw, progress=events.append)
    s = result.pool_summary
    assert set(s) == {"resstock/mf_small", "resstock/mf_mid"}
    assert s["resstock/mf_small"]["matched"] > 0
    assert s["resstock/mf_small"]["total"] > s["resstock/mf_small"]["matched"]
    assert s["resstock/mf_small"]["warnings"]  # fixture pool is below the default minimum of 10
    pool_event = next(e for e in events if e.stage == "building_pool")
    assert "fewer than 10" in pool_event.message
    snap_attrs = result.config_snapshot["archetype_filters"]["resstock"]["mf_small"]["attributes"]
    assert snap_attrs[0]["column"] == "in.geometry_stories"
    assert result.to_dict()["pool_summary"] == s
    json.dumps(result.to_dict())


def test_filter_that_keeps_every_building_changes_no_number(synthetic_dataset):
    base = build_tiny_config(synthetic_dataset)
    keep_all = _with_attrs(base, "mf_small", [{"column": "bldg_id", "op": "not_null"}])
    assert _metrics(base) == _metrics(keep_all)
