"""Characteristics dictionary: attribute discovery groundwork for filters (FR-DATA-7, FR-ENG-11)."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from microgrid.datasets import describe_characteristics


def _by_name(rows):
    return {r["name"]: r for r in rows}


def test_kinds_counts_and_ranges_on_fixture(synthetic_dataset):
    df = pd.read_excel(synthetic_dataset.resstock_chars, sheet_name="building_characteristics")
    rows = describe_characteristics(df)
    d = _by_name(rows)
    assert [r["name"] for r in rows] == list(df.columns)

    assert d["in.geometry_building_type_height"]["kind"] == "categorical"
    assert set(d["in.geometry_building_type_height"]["values"]) == set(df["in.geometry_building_type_height"])
    assert d["in.geometry_building_type_height"]["n_distinct"] == 3

    assert d["in.sqft..ft2"]["kind"] == "numeric"
    assert d["in.sqft..ft2"]["min"] == df["in.sqft..ft2"].min()
    assert d["in.sqft..ft2"]["max"] == df["in.sqft..ft2"].max()
    assert "values" not in d["in.sqft..ft2"]

    assert d["applicability"]["kind"] == "boolean"
    assert d["in.has_ev"]["kind"] == "boolean"
    assert d["in.geometry_stories"]["kind"] == "numeric"

    # SF rows have no unit count and some rows have no garage value
    assert d["in.geometry_building_number_units_mf"]["n_missing"] == int(
        df["in.geometry_building_number_units_mf"].isna().sum()
    )
    assert d["in.geometry_garage"]["n_missing"] > 0
    assert d["bldg_id"]["n_distinct"] == len(df)


def test_categorical_values_capped_at_20_most_frequent_first():
    values = [f"v{i:02d}" for i in range(30)] + ["v29"] * 5 + ["v28"] * 3
    row = describe_characteristics(pd.DataFrame({"c": values}))[0]
    assert row["n_distinct"] == 30
    assert len(row["values"]) == 20
    assert row["values"][:2] == ["v29", "v28"]
    assert row["values_truncated"] is True


def test_missing_excluded_from_distinct_and_all_missing_column():
    df = pd.DataFrame({"a": ["x", None, "x", "y"], "b": [np.nan] * 4, "n": [1.0, np.nan, 3.0, 2.0]})
    d = _by_name(describe_characteristics(df))
    assert d["a"]["n_distinct"] == 2 and d["a"]["n_missing"] == 1
    assert d["b"]["n_distinct"] == 0 and d["b"]["n_missing"] == 4
    assert d["n"]["min"] == 1.0 and d["n"]["max"] == 3.0


def test_output_is_json_serialisable(synthetic_dataset):
    df = pd.read_excel(synthetic_dataset.comstock_chars, sheet_name="building_characteristics")
    json.dumps(describe_characteristics(df))
