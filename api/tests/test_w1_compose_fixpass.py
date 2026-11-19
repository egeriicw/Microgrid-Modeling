"""W1-COMPOSE fix pass (QA and security conditions A-J).

A count rules on every run input · B pool check at creation for every run · C unknown MICROGRID_TEMPERATURE_* /
*_CHARS_FILE variables fail at startup · D composition names: no blank, C1 or bidi characters · E Cache-Control:
no-store on API responses · F /ready reports the data files · G the worker re-checks stored data paths · H a plain
message for convergence with max_runs below min_runs, and the default min_runs in /me · I /me says whether the caller
may set seeds · J late data-file failures store a safe, path-free reason.
"""

from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from microgrid.community_load.synthetic.config import EnsembleConfig

from microgrid_api import worker
from microgrid_api.config import ConfigurationError, Settings
from microgrid_api.db.models import Run
from microgrid_api.main import create_app
from microgrid_api.services import runs as svc

from .conftest import REPO_ROOT
from .http_support import bearer, client_config, make_settings

SEEDS = {"composition_seed": 5, "archetype_seed": 6, "perturbation_seed": 7}


def _fixture_module() -> Any:
    import importlib.util
    import sys

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


@pytest.fixture
def http_settings(tmp_path, synthetic_dataset, tiny_config, monkeypatch):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, max_ensemble_runs=50)
    monkeypatch.setattr(worker, "_settings_override", s)
    return s


@pytest.fixture
def api(http_settings):
    with _client(http_settings) as c:
        yield c


def _post(api: TestClient, body: dict[str, Any], who: str = "alice"):
    return api.post("/api/v1/runs", headers=bearer(who), json=body)


def _comp(total: int, *entries: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {"name": "fix", "total_buildings": total, "building_types": list(entries), "max_runs": 2, **extra}


def _rows(api: TestClient) -> int:
    with api.app.state.session_factory() as s:
        return s.query(Run).count()


def _detail(api: TestClient, rid: str, who: str = "alice") -> dict[str, Any]:
    return api.get(f"/api/v1/runs/{rid}", headers=bearer(who)).json()


# --- A: count rules on every run input -----------------------------------------------------------------------------


def test_predefined_composition_with_bad_counts_is_refused(tmp_path, synthetic_dataset, tiny_config):
    bad = client_config(tiny_config)
    bad["composition"]["categories"] = {
        "mf_small": {"fixed_count": 1, "source": "resstock"},
        "mf_mid": {"fixed_count": 1, "source": "resstock"},
    }
    demo = tmp_path / "demo-bad-counts.yaml"
    demo.write_text(yaml.safe_dump({"compositions": {"short": bad}}))
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, demo_compositions_file=demo)
    with _client(s) as api:
        r = _post(api, {"composition_id": "short"})
        assert r.status_code == 422, r.text
        assert "add up to 2" in r.json()["detail"]
        assert api.app.state.enqueued == [] and _rows(api) == 0


def test_raw_config_count_message_names_the_numbers(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["composition"]["categories"]["mf_small"] = {"fixed_count": 6, "source": "resstock"}
    cfg["composition"]["categories"]["mf_mid"]["max_fraction"] = 1.0  # so only the count rule is broken
    r = _post(api, {"config": cfg})
    assert r.status_code == 422 and "add up to 6" in r.json()["detail"] and "(4)" in r.json()["detail"]


# --- B: pool check at creation for every run -----------------------------------------------------------------------


def test_config_run_with_an_empty_pool_is_refused_naming_the_category(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["composition"]["categories"]["mf_large"] = {"fixed_count": 1, "source": "resstock"}
    cfg["composition"]["categories"]["mf_small"]["min_fraction"] = 0.0
    cfg["archetype_filters"]["resstock"]["mf_large"] = {
        "building_type_heights": ["Multi-Family with 5+ Units, 8+ Stories"]
    }
    r = _post(api, {"config": cfg})
    assert r.status_code == 422, r.text
    assert "'mf_large'" in r.json()["detail"] and "no buildings" in r.json()["detail"]
    assert api.app.state.enqueued == [] and _rows(api) == 0


def test_composition_empty_pool_message_names_the_building_type(api):
    r = _post(api, {"composition": _comp(4, {"building_type": "mf_large", "fixed_count": 1},
                                         {"building_type": "mf_small", "min_fraction": 0, "max_fraction": 1})})  # fmt: skip
    assert r.status_code == 422 and "'mf_large'" in r.json()["detail"]
    assert "Multi-Family" not in r.text and "in.geometry" not in r.text  # no filter internals


def test_empty_pool_is_fine_when_the_category_can_receive_nothing(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["composition"]["categories"]["mf_large"] = {"min_fraction": 0.0, "max_fraction": 0.0, "source": "resstock"}
    cfg["archetype_filters"]["resstock"]["mf_large"] = {
        "building_type_heights": ["Multi-Family with 5+ Units, 8+ Stories"]
    }
    assert _post(api, {"config": cfg}).status_code == 202


def test_unfiltered_runs_are_not_blocked_by_an_invalid_active_filter_configuration(api, tiny_config):
    """The pool check for runs without selections must not depend on the stored exposed-attribute configuration."""
    from microgrid_api.db.models import ExposedAttributeActivation, ExposedAttributeVersion
    from microgrid_api.services import exposed_attributes as ea_svc

    bad = ea_svc.packaged_document()
    bad["attributes"][0]["building_types"] = ["retired_type"]
    with api.app.state.session_factory() as s:
        s.add(ExposedAttributeVersion(version=1, document=bad, based_on=0, created_by="admin"))
        s.flush()
        s.add(ExposedAttributeActivation(version=1, activated_by="admin"))
        s.commit()
    assert _post(api, {"config": client_config(tiny_config)}).status_code == 202


# --- C: unknown data-file variables fail at startup ----------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["MICROGRID_TEMPERATURE_SOURCES", "MICROGRID_TEMPERATURE_UNIT", "MICROGRID_RESSTOCK_CHAR_FILE",
     "MICROGRID_RESTOCK_CHARS_FILE", "microgrid_temperature_colum"],
)  # fmt: skip
def test_unknown_data_file_variables_fail_at_startup_naming_the_variable(tmp_path, monkeypatch, name):
    monkeypatch.setenv("MICROGRID_DATABASE_URL", f"sqlite:///{tmp_path / 'x.db'}")
    monkeypatch.setenv("MICROGRID_ENVIRONMENT", "test")
    monkeypatch.setenv(name, "secret-value-4711")
    with pytest.raises(ConfigurationError) as exc:
        Settings()
    assert name.upper() in str(exc.value)
    assert "secret-value-4711" not in str(exc.value)


def test_known_data_file_variables_still_work(tmp_path, monkeypatch):
    monkeypatch.setenv("MICROGRID_DATABASE_URL", f"sqlite:///{tmp_path / 'x.db'}")
    monkeypatch.setenv("MICROGRID_ENVIRONMENT", "test")
    monkeypatch.setenv("MICROGRID_COMSTOCK_CHARS_FILE", "c.xlsx")
    monkeypatch.setenv("MICROGRID_TEMPERATURE_SOURCE", "none")
    assert Settings().comstock_chars_file == "c.xlsx"


# --- D: composition names -----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["   ", "\t", " ", "a\x85b", "a\x9fb", "a‮b", "a‪b", "a⁦b", "a⁩b", "a b", "a b"],
)
def test_composition_name_refuses_blank_c1_and_bidi_characters(api, name):
    r = _post(api, {"composition": _comp(3, {"building_type": "mf_small", "fixed_count": 3}, name=name)})
    assert r.status_code == 422, (repr(name), r.text)
    assert api.app.state.enqueued == []


@pytest.mark.parametrize("name", ["Ward 6 – block A", "Bloc résidentiel", "東京 test", " padded "])
def test_ordinary_names_are_accepted(api, name):
    r = _post(api, {"composition": _comp(3, {"building_type": "mf_small", "fixed_count": 3}, name=name)})
    assert r.status_code == 202, r.text


# --- E: Cache-Control: no-store ------------------------------------------------------------------------------------


def test_api_responses_are_not_cacheable(api, tiny_config):
    def no_store(r: Any) -> None:
        assert r.headers.get("cache-control") == "no-store", (r.status_code, r.headers.get("cache-control"))

    no_store(api.get("/api/v1/me", headers=bearer("alice")))
    no_store(api.get("/api/v1/me"))  # 401
    no_store(_post(api, {"composition": {"bogus": 1}}))  # 422
    r = _post(api, {"config": client_config(tiny_config), "seeds": SEEDS})
    no_store(r)
    rid = r.json()["id"]
    worker.execute_run(rid, api.app.state.session_factory)
    no_store(api.get(f"/api/v1/runs/{rid}", headers=bearer("alice")))
    results = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice"))
    assert results.status_code == 200
    no_store(results)
    events = api.get(f"/api/v1/runs/{rid}/events", headers=bearer("alice"))
    assert events.status_code == 200 and "event: status" in events.text  # the stream still works
    no_store(events)
    no_store(api.get(f"/api/v1/runs/{uuid.uuid4()}", headers=bearer("alice")))  # 404


def test_a_413_is_not_cacheable_either(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, max_request_bytes=100)
    with _client(s) as api:
        r = api.post("/api/v1/runs", headers=bearer("alice"), content=b"x" * 500)
        assert r.status_code == 413 and r.headers.get("cache-control") == "no-store"


# --- F: /ready reports the data files ------------------------------------------------------------------------------


def test_ready_reports_data_files_ok(api):
    r = api.get("/api/v1/ready")
    assert r.status_code == 200, r.text
    assert r.json()["checks"]["data_files"] == "ok"
    assert {"db", "schema"} <= set(r.json()["checks"])


@pytest.mark.parametrize("which", ["resstock_chars_file", "comstock_chars_file"])
def test_ready_is_not_ready_when_a_data_file_is_missing(tmp_path, synthetic_dataset, tiny_config, which):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, **{which: "absent-file.xlsx"})
    with _client(s) as api:
        r = api.get("/api/v1/ready")
    assert r.status_code == 503 and r.json()["ready"] is False
    checks = r.json()["checks"]
    assert checks["db"] == "ok" and checks["schema"] == "ok"
    assert which in checks["data_files"]
    assert "absent-file" not in r.text and str(synthetic_dataset.root) not in r.text


def test_ready_is_not_ready_when_a_data_file_escapes_the_root(tmp_path, synthetic_dataset, tiny_config):
    root = tmp_path / "data"
    shutil.copytree(synthetic_dataset.root, root)
    outside = tmp_path / "outside"
    outside.mkdir()
    shutil.copy(synthetic_dataset.resstock_chars, outside / "r.xlsx")
    (root / "background" / "linked.xlsx").symlink_to(outside / "r.xlsx")
    s = make_settings(
        tmp_path, synthetic_dataset, tiny_config, data_dir=root, resstock_chars_file="linked.xlsx",
        comstock_chars_file=synthetic_dataset.comstock_chars.name,
    )  # fmt: skip
    with _client(s) as api:
        r = api.get("/api/v1/ready")
    assert r.status_code == 503 and "resstock_chars_file" in r.json()["checks"]["data_files"]
    assert str(outside) not in r.text and str(root) not in r.text


def test_ready_without_a_data_dir_is_unchanged(app):
    r = TestClient(app).get("/api/v1/ready")
    assert r.status_code == 200 and "data_files" not in r.json()["checks"]


# --- G: the worker re-checks stored data paths ---------------------------------------------------------------------


def _stored_run(api: TestClient, tiny_config: dict[str, Any], data: dict[str, str]) -> uuid.UUID:
    snap = client_config(tiny_config)
    snap["data"] = data
    snap["output"] = {"base_dir": str(Path(api.app.state.settings.output_dir).resolve() / "g")}
    with api.app.state.session_factory() as s:
        return svc.create_run(s, snap, SEEDS, trusted_data_paths=True, owner="alice").id


@pytest.mark.parametrize("key", ["background_dir", "resstock_timeseries_dir", "comstock_timeseries_dir"])
def test_worker_refuses_a_stored_directory_outside_the_data_root(api, tiny_config, synthetic_dataset, tmp_path, key):
    outside = tmp_path / "elsewhere"
    shutil.copytree(synthetic_dataset.root, outside)
    data = svc.server_side_data_section(api.app.state.settings)
    rel = Path(data[key]).relative_to(Path(api.app.state.settings.data_dir).resolve())
    data[key] = str(outside / rel)
    rid = _stored_run(api, tiny_config, data)
    worker.execute_run(rid, api.app.state.session_factory)
    d = _detail(api, str(rid))
    assert d["status"] == "failed"
    assert d["error_message"] == "the server data directory is not available"
    assert str(outside) not in json.dumps(d)


def test_worker_refuses_a_characteristics_file_swapped_for_a_symlink_out(api, tiny_config, synthetic_dataset, tmp_path):
    root = tmp_path / "data"
    shutil.copytree(synthetic_dataset.root, root)
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=root,
                      resstock_chars_file="chars_resstock.xlsx", comstock_chars_file="chars_comstock.xlsx")  # fmt: skip
    with _client(s) as c:
        r = _post(c, {"config": client_config(tiny_config), "seeds": SEEDS})
        assert r.status_code == 202, r.text
        outside = tmp_path / "outside"
        outside.mkdir()
        target = root / "background" / "chars_resstock.xlsx"
        shutil.move(target, outside / "r.xlsx")
        target.symlink_to(outside / "r.xlsx")
        worker._settings_override = s  # the fixture's monkeypatch restores the original afterwards
        worker.execute_run(r.json()["id"], c.app.state.session_factory)
        d = _detail(c, r.json()["id"])
    assert d["status"] == "failed" and d["error_message"] == "the server data directory is not available"
    assert str(outside) not in json.dumps(d)


def test_worker_without_a_data_dir_refuses_stored_paths(api, tiny_config, monkeypatch, http_settings):
    data = svc.server_side_data_section(http_settings)
    rid = _stored_run(api, tiny_config, data)
    monkeypatch.setattr(worker, "_settings_override", http_settings.model_copy(update={"data_dir": None}))
    worker.execute_run(rid, api.app.state.session_factory)
    d = _detail(api, str(rid))
    assert d["status"] == "failed" and d["error_message"] == "the server data directory is not available"


# --- H: convergence with max_runs below min_runs; default min_runs in /me -------------------------------------------


def test_convergence_with_max_runs_below_the_default_min_runs_has_a_plain_message(api):
    r = _post(api, {"composition": _comp(3, {"building_type": "mf_small", "fixed_count": 3}, convergence=True)})
    assert r.status_code == 422
    default = EnsembleConfig().min_runs
    assert r.json()["detail"] == (
        f"with convergence on, max_runs must be at least min_runs ({default} unless you set it); got max_runs 2"
    )


def test_convergence_with_an_explicit_min_runs_above_max_runs_has_the_same_message(api):
    comp = _comp(3, {"building_type": "mf_small", "fixed_count": 3}, convergence=True, min_runs=5)
    r = _post(api, {"composition": comp})
    assert r.status_code == 422 and "max_runs must be at least min_runs" in r.json()["detail"]


def test_me_exposes_the_engine_default_min_runs(api):
    assert api.get("/api/v1/me", headers=bearer("alice")).json()["limits"]["default_min_runs"] == 20
    assert EnsembleConfig().min_runs == 20


# --- I: /me says whether the caller may set seeds -------------------------------------------------------------------


@pytest.mark.parametrize("who", ["admin", "alice", "demo"])
def test_me_may_set_seeds_matches_enforcement(api, tiny_config, who):
    me = api.get("/api/v1/me", headers=bearer(who)).json()
    body = {"composition_id": "small-mixed", "seeds": SEEDS}
    status = _post(api, body, who).status_code
    assert me["may_set_seeds"] is (who != "demo")
    assert (status == 202) is me["may_set_seeds"], status


# --- J: late data-file failures store a safe, path-free reason -----------------------------------------------------


def _weather_settings(tmp_path, tiny_config, ds, mod, **over: Any) -> Settings:
    wx = {
        "temperature_source": "weather_file",
        "temperature_weather_file": ds.weather_file.relative_to(ds.root).as_posix(),
        "temperature_time_column": mod.WEATHER_TIME_COL,
        "temperature_column": mod.WEATHER_TEMP_COL_F,
        "temperature_units": "F",
    }
    return make_settings(tmp_path, ds, tiny_config, max_ensemble_runs=50, **{**wx, **over})


def _create_then(tmp_path, tiny_config, monkeypatch, damage, **over: Any) -> tuple[dict[str, Any], Any]:
    mod = _fixture_module()
    ds = mod.build_synthetic_dataset(tmp_path / "j", weather_file=True)
    s = _weather_settings(tmp_path, tiny_config, ds, mod, **over)
    monkeypatch.setattr(worker, "_settings_override", s)
    with _client(s) as api:
        body = {
            "composition": _comp(4, {"building_type": "commercial_private", "fixed_count": 1},
                                 {"building_type": "mf_small", "min_fraction": 0, "max_fraction": 1}),
            "seeds": SEEDS,
        }  # fmt: skip
        r = _post(api, body)
        assert r.status_code == 202, r.text
        damage(ds)
        worker.execute_run(r.json()["id"], api.app.state.session_factory)
        d = _detail(api, r.json()["id"])
    assert d["status"] == "failed", d
    assert str(ds.root) not in json.dumps(d) and str(ds.root.resolve()) not in json.dumps(d)
    return d, ds


def test_wrong_weather_column_stores_a_safe_reason(tmp_path, tiny_config, monkeypatch):
    d, _ = _create_then(tmp_path, tiny_config, monkeypatch, lambda ds: None, temperature_column="no such column")
    assert d["error_message"].startswith("the server's weather or temperature data could not be read or used")
    assert "no such column" not in d["error_message"]


@pytest.mark.parametrize(
    "which, expected",
    [
        ("resstock", "the server's ResStock characteristics file could not be read"),
        ("comstock", "the server's ComStock characteristics file could not be read"),
        ("weather", "the server's weather or temperature data could not be read or used"),
    ],
)
def test_zero_byte_file_after_creation_stores_a_safe_reason(tmp_path, tiny_config, monkeypatch, which, expected):
    def damage(ds: Any) -> None:
        {"resstock": ds.resstock_chars, "comstock": ds.comstock_chars, "weather": ds.weather_file}[which].write_bytes(
            b""
        )

    d, _ = _create_then(tmp_path, tiny_config, monkeypatch, damage)
    assert d["error_message"].startswith(expected), d["error_message"]


@pytest.mark.parametrize(
    "which, expected",
    [
        ("resstock", "the server's ResStock characteristics file is not available"),
        ("comstock", "the server's ComStock characteristics file is not available"),
        ("weather", "the server's weather file is not available"),
    ],
)
def test_file_deleted_after_creation_stores_a_safe_reason(tmp_path, tiny_config, monkeypatch, which, expected):
    def damage(ds: Any) -> None:
        {"resstock": ds.resstock_chars, "comstock": ds.comstock_chars, "weather": ds.weather_file}[which].unlink()

    d, _ = _create_then(tmp_path, tiny_config, monkeypatch, damage)
    assert d["error_message"] == expected


def test_building_time_series_failure_stores_a_safe_reason(tmp_path, tiny_config, monkeypatch):
    def damage(ds: Any) -> None:
        for p in ds.resstock_dir.rglob("*.parquet"):
            p.write_bytes(b"not parquet")

    d, _ = _create_then(tmp_path, tiny_config, monkeypatch, damage)
    assert d["error_message"].startswith("the server's building time-series data could not be read"), d


def test_other_engine_failures_keep_the_class_only_message(session_factory, session, monkeypatch):
    def boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("/srv/secret/path exploded")

    monkeypatch.setattr(worker, "run_scenario", boom)
    rid = svc.create_run(session, {"scenario": {"name": "x"}}, validate=False).id
    worker.execute_run(rid, session_factory)
    session.expire_all()
    msg = session.get(Run, rid).error_message
    assert msg == "engine run failed (RuntimeError); see worker logs"
