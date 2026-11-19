"""W1-COMPOSE (D): the server names every data file it uses, checks each exists at run creation, and records the
file NAMES (never paths) with the run.

* The characteristics file names come from settings (MICROGRID_RESSTOCK_CHARS_FILE / MICROGRID_COMSTOCK_CHARS_FILE)
  and are always written into the snapshot, so the engine's hard-coded defaults are never relied on silently.
* The temperature source is declared from settings (MICROGRID_TEMPERATURE_*), `none` by default.
* Every file resolves under MICROGRID_DATA_DIR (symlink escape refused) and must exist at creation (503, no paths in
  the message, nothing stored or enqueued).
* Run detail carries a path-free `data_files` provenance block.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from microgrid.community_load.synthetic.config import config_from_dict
from microgrid.community_load.synthetic.run import run_scenario

from microgrid_api import worker
from microgrid_api.config import ConfigurationError, Settings
from microgrid_api.db.models import Run
from microgrid_api.main import create_app
from microgrid_api.services import runs as svc

from .conftest import REPO_ROOT
from .http_support import bearer, client_config, make_settings

SEEDS = {"composition_seed": 11, "archetype_seed": 22, "perturbation_seed": 33}


def _fixture_module() -> Any:
    name = "_root_synthetic_data"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "tests" / "fixtures" / "synthetic_data.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _client(settings: Settings) -> TestClient:
    app = create_app(settings)
    app.state.enqueued = []
    app.state.enqueue = lambda run_id: app.state.enqueued.append(str(run_id))
    return TestClient(app)


def _rows(api: TestClient) -> list[Run]:
    with api.app.state.session_factory() as s:
        return list(s.query(Run).all())


# --- settings: names are validated at startup, the variable is named, the value is not echoed ---------------------


@pytest.mark.parametrize("field", ["resstock_chars_file", "comstock_chars_file"])
@pytest.mark.parametrize(
    "value", ["../escape.xlsx", "/etc/passwd.xlsx", "sub/dir.xlsx", "..", ".", "", "a\\b.xlsx", "x\x00.xlsx"]
)
def test_characteristics_file_names_must_be_plain_names(field, value):
    with pytest.raises(ConfigurationError) as exc:
        Settings(environment="test", **{field: value})
    msg = str(exc.value)
    assert f"MICROGRID_{field.upper()}" in msg
    if value not in ("", ".", ".."):
        assert value not in msg


@pytest.mark.parametrize("value", ["../w.csv", "/etc/w.csv", "weather/../../w.csv", "", "a\\..\\w.csv", "w\x00.csv"])
def test_weather_file_must_be_relative_inside_the_data_root(value):
    with pytest.raises(ConfigurationError) as exc:
        Settings(
            environment="test",
            temperature_source="weather_file",
            temperature_weather_file=value,
            temperature_time_column="t",
            temperature_column="v",
            temperature_units="F",
        )
    assert "MICROGRID_TEMPERATURE_WEATHER_FILE" in str(exc.value)
    if value:
        assert value not in str(exc.value)


@pytest.mark.parametrize(
    "kwargs, variable",
    [
        ({"temperature_source": "guess"}, "MICROGRID_TEMPERATURE_SOURCE"),
        ({"temperature_column": "t"}, "MICROGRID_TEMPERATURE_COLUMN"),  # set but source none: would be ignored
        ({"temperature_units": "C"}, "MICROGRID_TEMPERATURE_UNITS"),
        ({"temperature_weather_file": "w.csv"}, "MICROGRID_TEMPERATURE_WEATHER_FILE"),
        ({"temperature_source": "timeseries_column", "temperature_units": "C"}, "MICROGRID_TEMPERATURE_COLUMN"),
        ({"temperature_source": "timeseries_column", "temperature_column": "t"}, "MICROGRID_TEMPERATURE_UNITS"),
        (
            {"temperature_source": "timeseries_column", "temperature_column": "t", "temperature_units": "K"},
            "MICROGRID_TEMPERATURE_UNITS",
        ),
        (
            {
                "temperature_source": "timeseries_column",
                "temperature_column": "t",
                "temperature_units": "C",
                "temperature_time_column": "ts",
            },
            "MICROGRID_TEMPERATURE_TIME_COLUMN",
        ),
        (
            {
                "temperature_source": "weather_file",
                "temperature_weather_file": "w.csv",
                "temperature_column": "t",
                "temperature_units": "C",
            },
            "MICROGRID_TEMPERATURE_TIME_COLUMN",
        ),
    ],
)
def test_temperature_settings_are_complete_and_nothing_is_ignored(kwargs, variable):
    with pytest.raises(ConfigurationError) as exc:
        Settings(environment="test", **kwargs)
    assert variable in str(exc.value)


def test_defaults_are_explicit_and_documented():
    s = Settings(environment="test")
    assert s.resstock_chars_file == "DC_upgrade0.xlsx"
    assert s.comstock_chars_file == "DC_upgrade0_agg.xlsx"
    assert s.temperature_source == "none"


# --- the data section ---------------------------------------------------------------------------------------------


def test_data_section_names_the_characteristics_files_and_declares_no_temperature_by_default(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    data = svc.server_side_data_section(
        Settings(environment="test", data_dir=root, resstock_chars_file="r.xlsx", comstock_chars_file="c.xlsx")
    )
    assert data["resstock_chars_file"] == "r.xlsx"
    assert data["comstock_chars_file"] == "c.xlsx"
    assert data["temperature_source"] == "none"
    assert not any(k.startswith("temperature_") and k != "temperature_source" for k in data)
    # what the server builds is a valid engine data section
    config_from_dict({"data": data}, source="<test>")


def test_data_section_declares_a_weather_file_under_the_root(tmp_path):
    root = tmp_path / "root"
    (root / "weather").mkdir(parents=True)
    s = Settings(
        environment="test",
        data_dir=root,
        temperature_source="weather_file",
        temperature_weather_file="weather/w.csv",
        temperature_time_column="date_time",
        temperature_column="Dry Bulb Temperature [F]",
        temperature_units="F",
    )
    data = svc.server_side_data_section(s)
    assert data["temperature_source"] == "weather_file"
    assert Path(data["temperature_weather_file"]) == (root / "weather" / "w.csv").resolve()
    assert data["temperature_time_column"] == "date_time"
    assert data["temperature_column"] == "Dry Bulb Temperature [F]"
    assert data["temperature_units"] == "F"
    config_from_dict({"data": data}, source="<test>")


def test_data_section_declares_a_timeseries_column(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    s = Settings(
        environment="test",
        data_dir=root,
        temperature_source="timeseries_column",
        temperature_column="out.site_outdoor_air_drybulb_temp",
        temperature_units="C",
    )
    data = svc.server_side_data_section(s)
    assert {k: v for k, v in data.items() if k.startswith("temperature_")} == {
        "temperature_source": "timeseries_column",
        "temperature_column": "out.site_outdoor_air_drybulb_temp",
        "temperature_units": "C",
    }
    config_from_dict({"data": data}, source="<test>")


@pytest.mark.parametrize(
    "field, value",
    [
        ("resstock_chars_file", "sub/x.xlsx"),
        ("comstock_chars_file", "../x.xlsx"),
        ("temperature_weather_file", "/etc/w.csv"),
        ("temperature_weather_file", "a/../../w.csv"),
    ],
)
def test_data_section_rechecks_names_even_if_settings_were_bypassed(tmp_path, field, value):
    root = tmp_path / "root"
    (root / "background" / "sub").mkdir(parents=True)
    s = Settings(environment="test", data_dir=root)
    setattr(s, field, value)  # no validation on assignment: the builder must not trust the object blindly
    with pytest.raises(ValueError) as exc:
        svc.server_side_data_section(s)
    assert value not in str(exc.value)


def test_symlinked_characteristics_file_out_of_the_root_is_refused(tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    (root / "background").mkdir(parents=True)
    outside.mkdir()
    (outside / "secret.xlsx").write_text("x")
    (root / "background" / "r.xlsx").symlink_to(outside / "secret.xlsx")
    with pytest.raises(ValueError) as exc:
        svc.server_side_data_section(Settings(environment="test", data_dir=root, resstock_chars_file="r.xlsx"))
    assert str(outside) not in str(exc.value) and str(root) not in str(exc.value)


def test_symlinked_weather_file_out_of_the_root_is_refused(tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (outside / "w.csv").write_text("x")
    (root / "w.csv").symlink_to(outside / "w.csv")
    s = Settings(
        environment="test",
        data_dir=root,
        temperature_source="weather_file",
        temperature_weather_file="w.csv",
        temperature_time_column="t",
        temperature_column="v",
        temperature_units="C",
    )
    with pytest.raises(ValueError) as exc:
        svc.server_side_data_section(s)
    assert str(outside) not in str(exc.value) and str(root) not in str(exc.value)


# --- run creation: missing or escaping files are a clear 503, nothing stored or enqueued ----------------------------


def _copy_root(synthetic_dataset, tmp_path) -> Path:
    root = tmp_path / "data"
    shutil.copytree(synthetic_dataset.root, root)
    return root


@pytest.mark.parametrize("which", ["resstock_chars_file", "comstock_chars_file"])
def test_missing_characteristics_file_is_503_at_creation(tmp_path, synthetic_dataset, tiny_config, which):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, **{which: "absent.xlsx"})
    with _client(s) as api:
        r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": client_config(tiny_config)})
        assert r.status_code == 503, r.text
        assert "characteristics" in r.json()["detail"]
        assert "absent.xlsx" not in r.text and str(synthetic_dataset.root) not in r.text
        assert api.app.state.enqueued == [] and _rows(api) == []


def test_missing_weather_file_is_503_at_creation(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(
        tmp_path,
        synthetic_dataset,
        tiny_config,
        temperature_source="weather_file",
        temperature_weather_file="weather/none.csv",
        temperature_time_column="t",
        temperature_column="v",
        temperature_units="F",
    )
    with _client(s) as api:
        r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": client_config(tiny_config)})
        assert r.status_code == 503, r.text
        assert "weather" in r.json()["detail"]
        assert "none.csv" not in r.text and str(synthetic_dataset.root) not in r.text
        assert api.app.state.enqueued == [] and _rows(api) == []


def test_characteristics_symlink_escape_is_503_without_paths(tmp_path, synthetic_dataset, tiny_config):
    root = _copy_root(synthetic_dataset, tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    shutil.copy(synthetic_dataset.resstock_chars, outside / "r.xlsx")
    (root / "background" / "linked.xlsx").symlink_to(outside / "r.xlsx")
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=root, resstock_chars_file="linked.xlsx")
    with _client(s) as api:
        r = api.post("/api/v1/runs", headers=bearer("admin"), json={"config": client_config(tiny_config)})
        assert r.status_code == 503
        assert str(outside) not in r.text and str(root) not in r.text and "linked.xlsx" not in r.text
        assert api.app.state.enqueued == [] and _rows(api) == []
        pv = api.post(
            "/api/v1/exposed-attributes/preview", headers=bearer("admin"), json={"config": client_config(tiny_config)}
        )
        assert pv.status_code == 503 and str(outside) not in pv.text


def test_weather_symlink_escape_is_503_without_paths(tmp_path, synthetic_dataset, tiny_config):
    root = _copy_root(synthetic_dataset, tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "w.csv").write_text("t,v\n")
    (root / "w.csv").symlink_to(outside / "w.csv")
    s = make_settings(
        tmp_path,
        synthetic_dataset,
        tiny_config,
        data_dir=root,
        temperature_source="weather_file",
        temperature_weather_file="w.csv",
        temperature_time_column="t",
        temperature_column="v",
        temperature_units="C",
    )
    with _client(s) as api:
        r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": client_config(tiny_config)})
        assert r.status_code == 503
        assert str(outside) not in r.text and str(root) not in r.text
        assert api.app.state.enqueued == [] and _rows(api) == []


# --- provenance and end-to-end execution ---------------------------------------------------------------------------


def test_run_detail_records_file_names_only(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config)
    with _client(s) as api:
        r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": client_config(tiny_config)})
        assert r.status_code == 202, r.text
        detail = api.get(f"/api/v1/runs/{r.json()['id']}", headers=bearer("alice")).json()
    assert detail["data_files"] == {
        "resstock_characteristics_file": "chars_resstock.xlsx",
        "comstock_characteristics_file": "chars_comstock.xlsx",
        "temperature": {"source": "none", "file": None, "column": None, "time_column": None, "units": None},
    }
    assert "data" not in detail["composition_snapshot"]
    assert str(synthetic_dataset.root) not in json.dumps(detail)


def test_old_snapshot_without_names_reports_them_as_unrecorded(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config)
    with _client(s) as api:
        with api.app.state.session_factory() as session:
            snap = client_config(tiny_config)
            snap["data"] = {"background_dir": str(synthetic_dataset.background_dir)}
            run = svc.create_run(session, snap, SEEDS, trusted_data_paths=True, owner="alice")
            rid = run.id
        detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("alice")).json()
    assert detail["data_files"] == {
        "resstock_characteristics_file": None,
        "comstock_characteristics_file": None,
        "temperature": {"source": None, "file": None, "column": None, "time_column": None, "units": None},
    }


def _execute(api: TestClient, settings: Settings, rid: str, monkeypatch) -> dict[str, Any]:
    monkeypatch.setattr(worker, "_settings_override", settings)
    worker.execute_run(rid, api.app.state.session_factory)
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("alice")).json()
    assert detail["status"] == "succeeded", detail["error_message"]
    return detail


def test_weather_file_temperature_reaches_the_results_without_paths(tmp_path, tiny_config, monkeypatch):
    mod = _fixture_module()
    ds = mod.build_synthetic_dataset(tmp_path / "wx", weather_file=True, weather_units="F")
    rel = ds.weather_file.relative_to(ds.root).as_posix()
    s = make_settings(
        tmp_path,
        ds,
        tiny_config,
        temperature_source="weather_file",
        temperature_weather_file=rel,
        temperature_time_column=mod.WEATHER_TIME_COL,
        temperature_column=mod.WEATHER_TEMP_COL_F,
        temperature_units="F",
    )
    with _client(s) as api:
        cfg = client_config(tiny_config)
        cfg["ensemble"] = {"mode": "fixed", "max_runs": 2}
        r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": cfg, "seeds": SEEDS})
        assert r.status_code == 202, r.text
        rid = r.json()["id"]
        detail = _execute(api, s, rid, monkeypatch)
        assert detail["data_files"]["temperature"] == {
            "source": "weather_file",
            "file": ds.weather_file.name,
            "column": mod.WEATHER_TEMP_COL_F,
            "time_column": mod.WEATHER_TIME_COL,
            "units": "F",
        }
        results = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice"))
        assert results.status_code == 200
        assert results.json()["results"]["temperature"]["available"] is True
        for text in (json.dumps(detail), results.text):
            assert str(ds.root) not in text and str(ds.root.resolve()) not in text
            assert str(s.output_dir) not in text and str(s.output_dir.resolve()) not in text
            assert rel not in text or rel == ds.weather_file.name


def test_timeseries_column_temperature_reaches_the_results(tmp_path, tiny_config, monkeypatch):
    mod = _fixture_module()
    ds = mod.build_synthetic_dataset(tmp_path / "tc", temperature_column="out.outdoor_temp_test")
    s = make_settings(
        tmp_path,
        ds,
        tiny_config,
        temperature_source="timeseries_column",
        temperature_column="out.outdoor_temp_test",
        temperature_units="C",
    )
    with _client(s) as api:
        cfg = client_config(tiny_config)
        cfg["ensemble"] = {"mode": "fixed", "max_runs": 2}
        r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": cfg, "seeds": SEEDS})
        assert r.status_code == 202, r.text
        rid = r.json()["id"]
        _execute(api, s, rid, monkeypatch)
        results = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice")).json()
    assert results["results"]["temperature"]["available"] is True


def _comparable(doc: dict[str, Any]) -> dict[str, Any]:
    """The numbers of a results document: everything except the config copy (which now names the files)."""
    return json.loads(json.dumps({k: v for k, v in doc.items() if k != "config_snapshot"}, default=str))


def test_config_run_results_are_unchanged_by_naming_the_files(tmp_path, synthetic_dataset, tiny_config, monkeypatch):
    """Same seeds, same numbers: a run through the new server-built data section equals the engine run on the data
    section the server built before this story (three directories, engine-default characteristics file names)."""
    root = _copy_root(synthetic_dataset, tmp_path)
    shutil.copy(synthetic_dataset.resstock_chars, root / "background" / "DC_upgrade0.xlsx")
    shutil.copy(synthetic_dataset.comstock_chars, root / "background" / "DC_upgrade0_agg.xlsx")
    s = make_settings(
        tmp_path,
        synthetic_dataset,
        tiny_config,
        data_dir=root,
        resstock_chars_file="DC_upgrade0.xlsx",
        comstock_chars_file="DC_upgrade0_agg.xlsx",
    )
    cfg = client_config(tiny_config)
    with _client(s) as api:
        r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": cfg, "seeds": SEEDS})
        assert r.status_code == 202, r.text
        rid = r.json()["id"]
        _execute(api, s, rid, monkeypatch)
        new = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice")).json()

    old_snapshot = copy.deepcopy(cfg)
    old_snapshot["rng"] = dict(SEEDS)
    old_snapshot["data"] = {
        "background_dir": str(root / "background"),
        "resstock_timeseries_dir": str(root / "input/resstock/timeseries_individual_buildings"),
        "comstock_timeseries_dir": str(root / "input/comstock/timeseries_individual_buildings"),
    }
    old_snapshot["output"] = {"base_dir": str(tmp_path / "old-out" / str(uuid.uuid4()))}
    old = run_scenario(config_from_dict(old_snapshot, source="<old>")).to_dict()
    assert _comparable(new) == _comparable(old)
