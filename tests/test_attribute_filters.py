"""Attribute-filter language (FR-ENG-11 a, d): strict, AND-combined, loud on bad input."""

from __future__ import annotations

import pandas as pd
import pytest

from microgrid.community_load.synthetic.attribute_filters import (
    AttributeFilter,
    apply_attribute_filters,
    parse_attribute_filter,
)


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5],
            "stories": [1, 2, 3, 4, 8],
            "fuel": ["Gas", "Electricity", "Gas", "Oil", None],
            "garage": ["1 Car", None, None, "2 Car", None],
            "ev": [True, False, False, True, False],
            "ev_text": ["True", "False", "false", "TRUE", "False"],
        }
    )


def _ids(df, *filters):
    return df.loc[apply_attribute_filters(df, list(filters)), "bldg_id"].tolist()


def test_equals_and_in(df):
    assert _ids(df, AttributeFilter("fuel", "equals", value="Gas")) == [1, 3]
    assert _ids(df, AttributeFilter("fuel", "in", values=("Gas", "Oil"))) == [1, 3, 4]
    assert _ids(df, AttributeFilter("stories", "equals", value=2)) == [2]


def test_range_inclusive_flags(df):
    f = {"column": "stories", "op": "range"}
    assert _ids(df, AttributeFilter(**f, min=2, max=4)) == [2, 3, 4]
    assert _ids(df, AttributeFilter(**f, min=2, max=4, min_inclusive=False)) == [3, 4]
    assert _ids(df, AttributeFilter(**f, min=2, max=4, max_inclusive=False)) == [2, 3]
    assert _ids(df, AttributeFilter(**f, min=4)) == [4, 5]
    assert _ids(df, AttributeFilter(**f, max=1)) == [1]


def test_not_null_and_boolean(df):
    assert _ids(df, AttributeFilter("garage", "not_null")) == [1, 4]
    assert _ids(df, AttributeFilter("garage", "is_null")) == [2, 3, 5]
    assert _ids(df, AttributeFilter("ev", "is_true")) == [1, 4]
    assert _ids(df, AttributeFilter("ev", "is_false")) == [2, 3, 5]
    assert _ids(df, AttributeFilter("ev_text", "is_true")) == [1, 4]  # spreadsheet text booleans


def test_and_across_attributes(df):
    got = _ids(df, AttributeFilter("fuel", "equals", value="Gas"), AttributeFilter("stories", "range", min=2))
    assert got == [3]


def test_empty_list_keeps_everything(df):
    assert _ids(df) == [1, 2, 3, 4, 5]


def test_missing_column_is_a_loud_error_naming_it(df):
    with pytest.raises(ValueError, match="no_such_col"):
        apply_attribute_filters(df, [AttributeFilter("no_such_col", "not_null")])


def test_type_mismatches_are_errors(df):
    with pytest.raises(ValueError, match="fuel.*numeric"):
        apply_attribute_filters(df, [AttributeFilter("fuel", "range", min=1)])
    with pytest.raises(ValueError, match="stories.*boolean"):
        apply_attribute_filters(df, [AttributeFilter("stories", "is_true")])


@pytest.mark.parametrize(
    "kwargs, msg",
    [
        ({"column": "", "op": "equals", "value": 1}, "column"),
        ({"column": "a", "op": "between", "min": 1, "max": 2}, "unknown operator"),
        ({"column": "a", "op": "equals"}, "needs `value`"),
        ({"column": "a", "op": "equals", "value": [1, 2]}, "single"),
        ({"column": "a", "op": "in", "values": ()}, "non-empty"),
        ({"column": "a", "op": "range"}, "min or max"),
        ({"column": "a", "op": "range", "min": 5, "max": 1}, "exceeds"),
        ({"column": "a", "op": "range", "min": True}, "number"),
        ({"column": "a", "op": "not_null", "value": 1}, "does not take"),
        ({"column": "a", "op": "equals", "value": 1, "min": 1}, "does not take"),
    ],
)
def test_construction_is_strictly_validated(kwargs, msg):
    with pytest.raises(ValueError, match=msg):
        AttributeFilter(**kwargs)


def test_parse_names_path_and_unknown_keys():
    f = parse_attribute_filter({"column": "stories", "op": "range", "min": 2, "max_inclusive": False}, "p[0]")
    assert f == AttributeFilter("stories", "range", min=2, max_inclusive=False)
    with pytest.raises(ValueError, match=r"p\[0\].*unknown key.*colum"):
        parse_attribute_filter({"colum": "x", "op": "not_null"}, "p[0]")
    with pytest.raises(ValueError, match=r"p\[1\].*unknown operator"):
        parse_attribute_filter({"column": "x", "op": "like"}, "p[1]")
    with pytest.raises(ValueError, match=r"p\[2\].*mapping"):
        parse_attribute_filter("stories>2", "p[2]")
