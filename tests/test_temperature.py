"""FR-ENG-10: every result carries the outdoor temperature series that goes with its load, or says why not.

The temperature source is DECLARED in the config (``data.temperature_*``), never guessed. Where the real NREL data
keeps its temperature is not confirmed [verify]; these tests use the synthetic fixture's two kinds of source: a
column in every building parquet, and a separate hourly weather file.
"""

from __future__ import annotations

import copy
import json

import numpy as np
import pandas as pd
import pytest

from microgrid.community_load.synthetic import config as cfg_mod
from microgrid.community_load.synthetic import loader, run
from microgrid.community_load.synthetic.results import SCHEMA_VERSION
from tests.fixtures.synthetic_data import (
    WEATHER_TEMP_COL_C,
    WEATHER_TEMP_COL_F,
    WEATHER_TIME_COL,
    build_synthetic_dataset,
    temperature_c,
)
from tests.test_run_facade import build_tiny_config

TEMP_COL = "synthetic.outdoor_air_temperature_c"  # fixture-only name; real column name [verify]
H_IDX = pd.date_range("2018-01-01", periods=8760, freq="h")
Q_IDX = pd.date_range("2018-01-01", periods=35040, freq="15min")


@pytest.fixture(autouse=True)
def _fresh_cache():
    loader.clear_cache()  # datasets in different temp dirs never share paths, but keep reads honest
    yield
    loader.clear_cache()


@pytest.fixture(scope="module")
def ds_column(tmp_path_factory):
    return build_synthetic_dataset(tmp_path_factory.mktemp("temp_col"), temperature_column=TEMP_COL)


@pytest.fixture(scope="module")
def ds_weather_f(tmp_path_factory):
    return build_synthetic_dataset(tmp_path_factory.mktemp("temp_wf"), weather_file=True, weather_units="F")


@pytest.fixture(scope="module")
def ds_mismatch(tmp_path_factory):
    return build_synthetic_dataset(
        tmp_path_factory.mktemp("temp_mismatch"), temperature_column=TEMP_COL, comstock_temperature_offset_c=2.0
    )


def _column_cfg(ds) -> dict:
    cfg = build_tiny_config(ds)
    cfg["data"].update(temperature_source="timeseries_column", temperature_column=TEMP_COL, temperature_units="C")
    return cfg


def _weather_cfg(ds, units: str = "F", column: str = WEATHER_TEMP_COL_F) -> dict:
    cfg = build_tiny_config(ds)
    cfg["data"].update(
        temperature_source="weather_file",
        temperature_weather_file=str(ds.weather_file),
        temperature_time_column=WEATHER_TIME_COL,
        temperature_column=column,
        temperature_units=units,
    )
    return cfg


def _mixed_cfg(ds) -> dict:
    """Residential and commercial buildings in one run (needed to see a resstock/comstock disagreement)."""
    cfg = _column_cfg(ds)
    cfg["composition"] = {
        "total_buildings": 4,
        "method": "dirichlet",
        "categories": {
            "mf_small": {"min_fraction": 0.25, "max_fraction": 0.75, "source": "resstock"},
            "office": {"min_fraction": 0.25, "max_fraction": 0.75, "source": "comstock"},
        },
    }
    cfg["archetype_filters"]["comstock"] = {"office": {"building_types": ["SmallOffice", "MediumOffice"]}}
    cfg["archetype_filters"]["resstock"].pop("mf_mid")
    return cfg


# ---- schema version -------------------------------------------------------------------------------------------


def test_schema_version_is_1_1():
    assert SCHEMA_VERSION == "1.1"


# ---- the switch: no declared source means an explicit "not available" ------------------------------------------


def test_default_config_declares_no_temperature_source():
    assert cfg_mod.DataConfig().temperature_source == "none"


def test_no_source_is_reported_explicitly(synthetic_dataset):
    d = run.run_scenario(build_tiny_config(synthetic_dataset)).to_dict()
    assert d["schema_version"] == "1.1" and d["results"]["schema_version"] == "1.1"
    t = d["results"]["temperature"]
    assert t["available"] is False
    assert "temperature_source" in t["reason"]
    assert t["hourly"] is None and t["15min"] is None
    assert t["source"] == {"kind": "none"}
    assert t["units"] == "degC"


# ---- source: a column in each building's timeseries -----------------------------------------------------------


def test_column_source_known_answer_hourly_and_15min(ds_column):
    d = run.run_scenario(_column_cfg(ds_column)).to_dict()
    res = d["results"]
    t = res["temperature"]
    assert t["available"] is True and t["units"] == "degC"
    assert len(t["hourly"]) == res["time"]["hourly"]["count"] == 8760
    assert len(t["15min"]) == res["time"]["15min"]["count"] == 35040
    # 15-minute values are the source values (float32 in the parquet); hourly is the mean of the hour's four.
    q = temperature_c(Q_IDX).astype("float32").astype(float)
    np.testing.assert_allclose(t["15min"], q, rtol=0, atol=1e-9)
    np.testing.assert_allclose(t["hourly"], q.reshape(-1, 4).mean(axis=1), rtol=0, atol=1e-9)
    src = t["source"]
    assert src["kind"] == "timeseries_column" and src["column"] == TEMP_COL and src["declared_units"] == "C"
    assert "mean" in t["derivation"]["hourly"]
    c = t["consistency"]
    assert c["n_series_compared"] >= 2 and c["max_abs_difference_c"] == 0.0
    json.dumps(d, allow_nan=False)


def test_column_source_missing_column_fails_loudly(synthetic_dataset):
    cfg = build_tiny_config(synthetic_dataset)  # the default fixture has NO temperature column
    cfg["data"].update(temperature_source="timeseries_column", temperature_column=TEMP_COL, temperature_units="C")
    with pytest.raises(ValueError, match=r"temperature column .*synthetic.outdoor_air_temperature_c.* not found"):
        run.run_scenario(cfg)


def test_buildings_with_different_temperature_fail_loudly(ds_mismatch):
    with pytest.raises(ValueError, match=r"different temperature series"):
        run.run_scenario(_mixed_cfg(ds_mismatch))


def test_buildings_with_same_temperature_pass_in_a_mixed_run(ds_column):
    d = run.run_scenario(_mixed_cfg(ds_column)).to_dict()
    sources = {s for r in d["results"]["by_building_type"].values() for s in [r["source"]]}
    assert sources == {"resstock", "comstock"}  # both products really were compared
    assert d["results"]["temperature"]["consistency"]["max_abs_difference_c"] == 0.0


# ---- source: a separate weather file ----------------------------------------------------------------------------


def test_weather_file_in_fahrenheit_is_converted_and_held_for_15min(ds_weather_f):
    t = run.run_scenario(_weather_cfg(ds_weather_f)).to_dict()["results"]["temperature"]
    expect_h = temperature_c(H_IDX)
    np.testing.assert_allclose(t["hourly"], expect_h, rtol=0, atol=1e-9)
    np.testing.assert_allclose(t["15min"], np.repeat(expect_h, 4), rtol=0, atol=1e-9)
    assert t["source"]["kind"] == "weather_file"
    assert t["source"]["file"] == ds_weather_f.weather_file.name  # a file NAME only: no server paths in results
    assert t["source"]["declared_units"] == "F"
    assert "held" in t["derivation"]["15min"]


def test_weather_file_missing_fails_loudly(ds_weather_f, tmp_path):
    cfg = _weather_cfg(ds_weather_f)
    cfg["data"]["temperature_weather_file"] = str(tmp_path / "nope.csv")
    with pytest.raises(FileNotFoundError, match="weather file"):
        run.run_scenario(cfg)


def test_missing_weather_file_fails_before_any_run_even_with_zero_feasible_runs(ds_weather_f, tmp_path):
    cfg = _weather_cfg(ds_weather_f)
    cfg["data"]["temperature_weather_file"] = str(tmp_path / "nope.csv")
    cfg["feasibility"] = {"hard": {"annual_energy_kwh": {"max": 1.0}}}  # every run rejected
    events: list[run.ProgressEvent] = []
    with pytest.raises(FileNotFoundError, match="weather file"):
        run.run_scenario(cfg, progress=events.append)
    assert events == []  # failed up front: no pool built, no run attempted


def test_without_a_declared_source_no_file_check_happens(synthetic_dataset):
    cfg = cfg_mod.config_from_dict(build_tiny_config(synthetic_dataset))
    assert cfg.data.temperature_source == "none"
    from microgrid.community_load.synthetic.temperature import check_declared_source

    check_declared_source(cfg)  # nothing declared: nothing to check, no error


def test_weather_file_missing_column_fails_loudly(ds_weather_f):
    cfg = _weather_cfg(ds_weather_f, column=WEATHER_TEMP_COL_C)  # the file is in F, so this column is absent
    with pytest.raises(ValueError, match=r"temperature column .* not found"):
        run.run_scenario(cfg)


def test_weather_file_not_covering_the_load_period_fails_loudly(ds_weather_f, tmp_path):
    short = tmp_path / "short.csv"
    pd.read_csv(ds_weather_f.weather_file).iloc[:-24].to_csv(short, index=False)
    cfg = _weather_cfg(ds_weather_f)
    cfg["data"]["temperature_weather_file"] = str(short)
    with pytest.raises(ValueError, match=r"does not cover"):
        run.run_scenario(cfg)


def test_weather_file_with_duplicate_timestamps_fails_loudly(ds_weather_f, tmp_path):
    df = pd.read_csv(ds_weather_f.weather_file)
    dup = tmp_path / "dup.csv"
    pd.concat([df, df.iloc[:1]]).to_csv(dup, index=False)
    cfg = _weather_cfg(ds_weather_f)
    cfg["data"]["temperature_weather_file"] = str(dup)
    with pytest.raises(ValueError, match=r"duplicate"):
        run.run_scenario(cfg)


def test_weather_file_with_missing_values_fails_loudly(ds_weather_f, tmp_path):
    df = pd.read_csv(ds_weather_f.weather_file)
    df.loc[100, WEATHER_TEMP_COL_F] = np.nan
    bad = tmp_path / "nan.csv"
    df.to_csv(bad, index=False)
    cfg = _weather_cfg(ds_weather_f)
    cfg["data"]["temperature_weather_file"] = str(bad)
    with pytest.raises(ValueError, match=r"missing or non-finite"):
        run.run_scenario(cfg)


# ---- temperature never changes the load numbers ---------------------------------------------------------------


def _without_temperature(doc: dict) -> dict:
    d = copy.deepcopy(doc)
    d["results"].pop("temperature")
    d.pop("config_snapshot")  # differs only in the declared temperature fields
    return d


def test_temperature_does_not_change_any_load_output(ds_column, ds_weather_f):
    for ds, with_temp in ((ds_column, _column_cfg(ds_column)), (ds_weather_f, _weather_cfg(ds_weather_f))):
        plain = run.run_scenario(build_tiny_config(ds)).to_dict()
        temp = run.run_scenario(with_temp).to_dict()
        assert _without_temperature(plain) == _without_temperature(temp)


def test_adding_a_temperature_column_does_not_change_the_load_data(synthetic_dataset, ds_column):
    """The fixture's temperature column is opt-in and draws no random numbers: loads are byte-identical."""
    a = run.run_scenario(build_tiny_config(synthetic_dataset)).to_dict()
    b = run.run_scenario(build_tiny_config(ds_column)).to_dict()
    assert a["metrics"] == b["metrics"]
    assert a["results"]["envelopes"] == b["results"]["envelopes"]


# ---- config validation: declared, complete, never silently ignored -----------------------------------------------


@pytest.mark.parametrize(
    ("data", "msg"),
    [
        ({"temperature_source": "guess"}, "temperature_source"),
        ({"temperature_source": "timeseries_column", "temperature_units": "C"}, "temperature_column"),
        ({"temperature_source": "timeseries_column", "temperature_column": "t"}, "temperature_units"),
        ({"temperature_source": "timeseries_column", "temperature_column": "t", "temperature_units": "K"}, "units"),
        (
            {"temperature_source": "timeseries_column", "temperature_column": "t", "temperature_units": "C",
             "temperature_weather_file": "w.csv"},
            "temperature_weather_file",
        ),
        (
            {"temperature_source": "weather_file", "temperature_column": "t", "temperature_units": "C",
             "temperature_time_column": "date_time"},
            "temperature_weather_file",
        ),
        (
            {"temperature_source": "weather_file", "temperature_column": "t", "temperature_units": "C",
             "temperature_weather_file": "w.csv"},
            "temperature_time_column",
        ),
        ({"temperature_column": "t"}, "temperature_source"),  # a stray field with source none is not ignored
        ({"temperature_units": "C"}, "temperature_source"),
    ],
)  # fmt: skip
def test_invalid_temperature_declarations_fail_loudly(data, msg):
    with pytest.raises(ValueError, match=msg):
        cfg_mod.config_from_dict({"data": data})


def test_valid_declarations_round_trip_through_the_snapshot():
    raw = {
        "data": {
            "temperature_source": "weather_file",
            "temperature_weather_file": "w.parquet",
            "temperature_time_column": "date_time",
            "temperature_column": "t",
            "temperature_units": "F",
        }
    }
    cfg = cfg_mod.config_from_dict(raw)
    again = cfg_mod.config_from_dict(cfg_mod.config_to_dict(cfg))
    assert again.data == cfg.data
