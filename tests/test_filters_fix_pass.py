"""Post-QA fix pass for stories 1.6b / 1.11b: strict loading, selection-time type checks, data consistency."""

from __future__ import annotations

import pandas as pd
import pytest

from microgrid.community_load.exposed_attributes import (
    ExposedAttributes,
    check_against_data,
    default_exposed_attributes,
)
from microgrid.community_load.registry import BuildingTypeRegistry
from microgrid.community_load.synthetic.config import load_config
from microgrid.community_load.synthetic.strict_yaml import load_yaml_strict


def _attr(**over) -> dict:
    return {
        "key": "a",
        "label": "A",
        "kind": "numeric",
        "columns": {"resstock": "c"},
        "operators": ["range", "equals", "in"],
        **over,
    }


def _ea(*attrs: dict) -> ExposedAttributes:
    return ExposedAttributes.from_dict({"version": 1, "attributes": list(attrs)})


# --- strict YAML -----------------------------------------------------------------------------------------


def test_duplicate_yaml_keys_are_an_error_with_the_key_and_line():
    with pytest.raises(ValueError, match=r"duplicate key 'a' at line 2"):
        load_yaml_strict("a: 1\na: 2\n")
    with pytest.raises(ValueError, match="duplicate key 'x'"):
        load_yaml_strict("top:\n  x: 1\n  y: 2\n  x: 3\n")
    assert load_yaml_strict("a: {b: 1}\nc: {b: 2}\n") == {"a": {"b": 1}, "c": {"b": 2}}  # same key, other maps: fine


def test_every_yaml_loader_refuses_duplicate_keys(tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text("scenario: {name: a}\nscenario: {name: b}\n")
    with pytest.raises(ValueError, match="duplicate key 'scenario'"):
        load_config(cfg)
    ex = tmp_path / "e.yaml"
    ex.write_text("version: 1\nversion: 1\nattributes: []\n")
    with pytest.raises(ValueError, match="duplicate key 'version'"):
        ExposedAttributes.from_yaml(ex)
    reg = tmp_path / "r.yaml"
    reg.write_text("building_types: []\nbuilding_types: []\n")
    with pytest.raises(ValueError, match="duplicate key 'building_types'"):
        BuildingTypeRegistry.from_yaml(reg)


# --- minor strictness ------------------------------------------------------------------------------------


def test_version_true_is_not_version_1():
    with pytest.raises(ValueError, match="version"):
        ExposedAttributes.from_dict({"version": True, "attributes": []})


def test_duplicate_operators_in_one_attribute_are_an_error():
    with pytest.raises(ValueError, match="duplicate operator 'range'"):
        _ea(_attr(operators=["range", "range"]))


def test_to_filters_none_and_non_list_selections_are_clear_errors():
    d = default_exposed_attributes()
    for bad in (None, "stories", {"attribute": "stories"}, 5):
        with pytest.raises(ValueError, match="selections must be a list"):
            d.to_filters(bad, source="resstock")  # type: ignore[arg-type]
    assert d.to_filters([], source="resstock") == []


def test_active_for_unknown_building_type_is_an_error():
    d = default_exposed_attributes()
    with pytest.raises(ValueError, match="unknown building type 'castle'"):
        d.active_for("resstock", "castle")
    with pytest.raises(ValueError, match="unknown building type"):
        d.to_filters([], source="resstock", building_type="castle")
    assert d.active_for("resstock", "sf_detached") is not None


# --- F10: value types at selection time --------------------------------------------------------------------


@pytest.mark.parametrize(
    "sel",
    [
        {"attribute": "a", "op": "equals", "value": "3"},
        {"attribute": "a", "op": "equals", "value": True},
        {"attribute": "a", "op": "in", "values": [1, "2"]},
        {"attribute": "a", "op": "range", "min": float("nan")},
        {"attribute": "a", "op": "range", "min": "1"},
    ],
)
def test_numeric_attributes_reject_text_bool_and_nan(sel):
    with pytest.raises(ValueError, match=r"selection\[0\]"):
        _ea(_attr()).to_filters([sel], source="resstock")


def test_categorical_attributes_reject_numbers_and_boolean_attributes_reject_any_value():
    cat = _ea(_attr(kind="categorical", operators=["equals", "in"]))
    with pytest.raises(ValueError, match="text"):
        cat.to_filters([{"attribute": "a", "op": "equals", "value": 3}], source="resstock")
    with pytest.raises(ValueError, match="text"):
        cat.to_filters([{"attribute": "a", "op": "in", "values": ["x", 2]}], source="resstock")
    boo = _ea(_attr(kind="boolean", operators=["is_true"]))
    with pytest.raises(ValueError):
        boo.to_filters([{"attribute": "a", "op": "is_true", "value": 1}], source="resstock")
    assert boo.to_filters([{"attribute": "a", "op": "is_true"}], source="resstock")[0].op == "is_true"
    num = _ea(_attr())
    assert num.to_filters([{"attribute": "a", "op": "equals", "value": 3}], source="resstock")[0].value == 3


# --- F9: check_against_data compares choices and kinds -----------------------------------------------------


def test_check_against_data_reports_missing_choices_per_attribute_with_structure():
    d = _ea(
        _attr(key="v", kind="categorical", columns={"resstock": "v"}, operators=["equals"], choices=["1950s", "ghost"]),
        _attr(key="w", kind="categorical", columns={"resstock": "w"}, operators=["equals"], choices=["x", "y"]),
    )
    df = pd.DataFrame({"v": ["1950s", "1960s", None], "w": ["x", "y", "x"]})
    problems = check_against_data(d, df, "resstock")
    assert len(problems) == 1 and isinstance(problems[0], str)
    p = problems[0]
    assert "'v'" in p and "ghost" in p
    assert (p.attribute, p.column, p.problem, p.details) == ("v", "v", "choices_not_in_data", ("ghost",))


def test_check_against_data_flags_kind_that_does_not_fit_the_column_dtype():
    d = _ea(
        _attr(key="n", columns={"resstock": "n"}),
        _attr(key="c", kind="categorical", columns={"resstock": "c"}, operators=["equals"]),
        _attr(key="b", kind="boolean", columns={"resstock": "b"}, operators=["is_true"]),
        _attr(key="ok", columns={"resstock": "ok"}),
    )
    df = pd.DataFrame({"n": ["a", "b"], "c": [1, 2], "b": [1, 0], "ok": [1.5, None]})
    found = {p.attribute: p for p in check_against_data(d, df, "resstock")}
    assert set(found) == {"n", "c", "b"}
    assert all(p.problem == "kind_mismatch" for p in found.values())
    assert "numeric" in found["n"] and "'n'" in found["n"]


def test_check_against_data_accepts_text_booleans_and_missing_column_is_still_reported():
    d = _ea(_attr(key="b", kind="boolean", columns={"resstock": "b"}, operators=["is_true"]), _attr(key="gone"))
    df = pd.DataFrame({"b": ["True", "false", None]})
    (p,) = check_against_data(d, df, "resstock")
    assert p.problem == "missing_column" and "not in the data" in p
