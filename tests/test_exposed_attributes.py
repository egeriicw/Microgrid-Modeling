"""Admin-configured exposed attributes and discovery (FR-ENG-11 layers 1 and 2)."""

from __future__ import annotations

import copy

import pytest
import yaml

from microgrid.community_load.exposed_attributes import (
    ExposedAttributes,
    check_against_data,
    default_exposed_attributes,
    discover_candidates,
)
from microgrid.community_load.registry import default_registry
from microgrid.community_load.synthetic.archetype import load_comstock_characteristics, load_resstock_characteristics
from microgrid.community_load.synthetic.attribute_filters import AttributeFilter

STARTER_KEYS = {
    "stories",
    "dwelling_units",
    "has_ev",
    "vintage",
    "floor_area",
    "heating_fuel",
    "has_heat_pump",
}


def _doc(**over):
    entry = {
        "key": "stories",
        "label": "Stories",
        "help": "Floors above ground.",
        "kind": "numeric",
        "columns": {"resstock": "in.geometry_stories"},
        "operators": ["equals", "range"],
        "min": 1,
        "max": 100,
        "order": 10,
    }
    entry.update(over)
    return {"version": 1, "attributes": [entry]}


# ---- shipped starter set ---------------------------------------------------------------------------------


def test_shipped_starter_set_is_valid_and_ordered():
    ex = default_exposed_attributes()
    assert {a.key for a in ex.attributes} == STARTER_KEYS
    orders = [a.order for a in ex.active_for("resstock")]
    assert orders == sorted(orders)
    # registry type keys named in the file must exist (attached vs detached stays a registry type, not an attribute)
    registry = default_registry()
    for a in ex.attributes:
        for key in a.building_types:
            assert key in registry
    assert "attached" not in " ".join(a.key for a in ex.attributes)


def test_starter_columns_exist_in_the_generated_fixture(synthetic_dataset):
    res = load_resstock_characteristics(synthetic_dataset.resstock_chars)
    com = load_comstock_characteristics(synthetic_dataset.comstock_chars)
    ex = default_exposed_attributes()
    assert check_against_data(ex, res, "resstock") == []
    # commercial columns the starter set names that the fixture does not have are reported, never ignored
    problems = check_against_data(ex, com, "comstock")
    assert all("comstock" in p for p in problems)


def test_multifamily_only_attribute_applies_to_mf_types():
    ex = default_exposed_attributes()
    mf = {a.key for a in ex.active_for("resstock", "mf_mid")}
    sf = {a.key for a in ex.active_for("resstock", "sf_detached")}
    assert "dwelling_units" in mf and "dwelling_units" not in sf
    assert "dwelling_units" not in {a.key for a in ex.active_for("comstock")}


def test_inactive_attributes_are_not_offered():
    raw = _doc(active=False)
    ex = ExposedAttributes.from_dict(raw)
    assert ex.active_for("resstock") == []
    assert [a.key for a in ex.attributes] == ["stories"]


# ---- loader strictness -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "over, msg",
    [
        ({"colour": "red"}, "unknown field.*colour"),
        ({"kind": "text"}, "kind"),
        ({"operators": ["range", "like"]}, "like"),
        ({"operators": ["is_true"]}, "not valid for numeric"),
        ({"operators": []}, "operators"),
        ({"columns": {}}, "columns"),
        ({"columns": {"resstoc": "x"}}, "resstoc"),
        ({"min": 5, "max": 1}, "min.*max"),
        ({"building_types": ["no_such_type"]}, "no_such_type"),
        ({"label": ""}, "label"),
        ({"order": "first"}, "order"),
        ({"active": "yes"}, "active"),
        ({"choices": ["a"]}, "choices"),
    ],
)
def test_bad_entries_fail_loudly(over, msg):
    with pytest.raises(ValueError, match=msg):
        ExposedAttributes.from_dict(_doc(**over), registry=default_registry())


def test_missing_field_duplicate_key_and_top_level_unknowns():
    raw = _doc()
    del raw["attributes"][0]["label"]
    with pytest.raises(ValueError, match="missing.*label"):
        ExposedAttributes.from_dict(raw)
    dup = _doc()
    dup["attributes"].append(copy.deepcopy(dup["attributes"][0]))
    with pytest.raises(ValueError, match="duplicate"):
        ExposedAttributes.from_dict(dup)
    with pytest.raises(ValueError, match="unknown top-level"):
        ExposedAttributes.from_dict({"version": 1, "attributes": [], "extra": 1})
    with pytest.raises(ValueError, match="version"):
        ExposedAttributes.from_dict({"version": 2, "attributes": []})


def test_from_yaml_roundtrip(tmp_path):
    p = tmp_path / "x.yaml"
    p.write_text(yaml.safe_dump(_doc()))
    assert ExposedAttributes.from_yaml(p).get("stories").label == "Stories"
    with pytest.raises(KeyError, match="unknown exposed attribute"):
        ExposedAttributes.from_yaml(p).get("nope")


# ---- turning form selections into engine filters ---------------------------------------------------------


def test_selections_become_filters_using_the_source_column():
    ex = default_exposed_attributes()
    got = ex.to_filters(
        [
            {"attribute": "stories", "op": "range", "min": 3},
            {"attribute": "has_ev", "op": "is_true"},
        ],
        source="resstock",
        building_type="mf_mid",
    )
    assert got[0] == AttributeFilter(ex.get("stories").columns["resstock"], "range", min=3)
    assert got[1].op == "is_true"
    comm = ex.to_filters([{"attribute": "stories", "op": "range", "min": 3}], source="comstock")
    assert comm[0].column == ex.get("stories").columns["comstock"]


@pytest.mark.parametrize(
    "selection, kwargs, msg",
    [
        ({"attribute": "nope", "op": "not_null"}, {}, "unknown exposed attribute"),
        ({"attribute": "stories", "op": "not_null"}, {}, "not allowed"),
        ({"attribute": "stories", "op": "range", "min": 0}, {}, "below"),
        ({"attribute": "stories", "op": "range", "max": 10_000}, {}, "above"),
        ({"attribute": "heating_fuel", "op": "equals", "value": "Coal"}, {}, "not one of the allowed choices"),
        ({"attribute": "dwelling_units", "op": "range", "min": 2}, {"building_type": "sf_detached"}, "does not apply"),
        ({"attribute": "dwelling_units", "op": "range", "min": 2}, {"source": "comstock"}, "does not apply"),
        ({"attribute": "has_heat_pump"}, {}, "op"),
        ({"attribute": "stories", "op": "range", "min": 1, "bogus": 1}, {}, "bogus"),
    ],
)
def test_invalid_selections_are_errors(selection, kwargs, msg):
    ex = default_exposed_attributes()
    args = {"source": "resstock", "building_type": None, **kwargs}
    with pytest.raises(ValueError, match=msg):
        ex.to_filters([selection], **args)


def test_same_attribute_twice_is_an_error():
    ex = default_exposed_attributes()
    sel = {"attribute": "stories", "op": "range", "min": 2}
    with pytest.raises(ValueError, match="more than once"):
        ex.to_filters([sel, sel], source="resstock")


# ---- discovery -------------------------------------------------------------------------------------------


def test_discover_candidates_lists_filterable_columns(synthetic_dataset):
    res = load_resstock_characteristics(synthetic_dataset.resstock_chars)
    found = {c["name"]: c for c in discover_candidates(res)}
    assert {"in.geometry_stories", "in.vintage", "in.heating_fuel", "in.has_ev", "in.sqft..ft2"} <= set(found)
    # identifiers, completion flags and result columns are never candidates
    assert not {"bldg_id", "completed_status", "applicability"} & set(found)
    assert found["in.geometry_stories"]["kind"] == "numeric"
    assert "range" in found["in.geometry_stories"]["suggested_operators"]
    assert found["in.has_ev"]["suggested_operators"] == ["is_true", "is_false", "not_null", "is_null"]
    assert found["in.vintage"]["values"]


def test_discover_marks_already_exposed_columns(synthetic_dataset):
    res = load_resstock_characteristics(synthetic_dataset.resstock_chars)
    found = {c["name"]: c for c in discover_candidates(res, exposed=default_exposed_attributes(), source="resstock")}
    assert found["in.geometry_stories"]["exposed_as"] == "stories"
    assert found["in.hvac_heating_type"]["exposed_as"] is None


def test_discover_skips_constant_and_empty_columns():
    import pandas as pd

    df = pd.DataFrame({"bldg_id": [1, 2], "in.same": ["a", "a"], "in.empty": [None, None], "in.varies": [1, 2]})
    assert [c["name"] for c in discover_candidates(df)] == ["in.varies"]
