"""Fix pass: server-set output dir, caps, seed validation, compact stored result + results file endpoint."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from microgrid_api import worker
from microgrid_api.db.models import Run, RunStatus
from microgrid_api.main import create_app
from microgrid_api.services import runs as svc

from .http_support import bearer, client_config, make_settings


@pytest.fixture
def http_settings(tmp_path, synthetic_dataset, tiny_config):
    return make_settings(tmp_path, synthetic_dataset, tiny_config)


@pytest.fixture
def api(http_settings):
    app = create_app(http_settings)
    app.state.enqueue = lambda run_id: None
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


def _post(api, cfg, who="alice", **extra):
    return api.post("/api/v1/runs", headers=bearer(who), json={"config": cfg, **extra})


def _snapshot(api, rid):
    with api.app.state.session_factory() as s:
        return s.get(Run, uuid.UUID(rid)).composition_snapshot


def test_server_sets_per_run_output_dir(api, tiny_config, http_settings):
    r = _post(api, client_config(tiny_config))
    rid = r.json()["id"]
    base = Path(_snapshot(api, rid)["output"]["base_dir"])
    assert base == http_settings.output_dir.resolve() / rid
    assert str(http_settings.output_dir) not in r.text
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("alice"))
    assert "base_dir" not in detail.text


def test_client_output_section_other_keys_kept(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["output"] = {"per_run_csvs": False}
    rid = _post(api, cfg).json()["id"]
    out = _snapshot(api, rid)["output"]
    assert out["per_run_csvs"] is False and "base_dir" in out


def test_unconfigured_output_dir_is_503(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, output_dir=None)
    with TestClient(create_app(s)) as c:
        r = c.post("/api/v1/runs", headers=bearer("alice"), json={"config": client_config(tiny_config)})
    assert r.status_code == 503


@pytest.mark.parametrize("seed", [True, "5", 1.5, 2**32])
def test_bad_seed_types_and_range(api, tiny_config, seed):
    assert _post(api, client_config(tiny_config), seeds={"archetype_seed": seed}).status_code == 422


def test_max_seed_accepted(api, tiny_config):
    assert _post(api, client_config(tiny_config), seeds={"archetype_seed": 2**32 - 1}).status_code == 202


def test_building_cap_for_clients_and_admin(api, tiny_config):
    for who in ("alice", "admin"):
        cfg = client_config(tiny_config)
        cfg["composition"]["total_buildings"] = 5001
        r = _post(api, cfg, who=who)
        assert r.status_code == 422 and "total_buildings" in r.json()["detail"]


def test_run_cap_configurable(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, max_ensemble_runs=6)
    with TestClient(create_app(s)) as c:
        c.app.state.enqueue = lambda rid: None
        cfg = client_config(tiny_config)
        assert c.post("/api/v1/runs", headers=bearer("alice"), json={"config": cfg}).status_code == 202
        cfg["ensemble"]["max_runs"] = 7
        assert c.post("/api/v1/runs", headers=bearer("alice"), json={"config": cfg}).status_code == 422


def test_last_event_id_negative_422(api, tiny_config):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    r = api.get(f"/api/v1/runs/{rid}/events", headers={**bearer("alice"), "Last-Event-ID": "-5"})
    assert r.status_code == 422


def test_limit_and_offset_bounds(api):
    assert api.get("/api/v1/runs?limit=201", headers=bearer("alice")).status_code == 422
    assert api.get("/api/v1/runs?offset=10000000", headers=bearer("alice")).status_code == 422


# ---- worker stores compact summary + results file --------------------------------------------------------------


class _Result:
    def to_dict(self, *, include_results: bool = True):
        doc = {
            "engine_version": "9.9.9",
            "seeds": {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3},
            "metrics": [],
        }
        if include_results:
            doc["results"] = {"time": [1, 2, 3]}
        return doc


def _queued_with_output(session, base: Path):
    cfg = {"scenario": {"name": "x"}, "output": {"base_dir": str(base)}}
    return svc.create_run(session, cfg, validate=False).id


def test_worker_writes_results_file_and_compact_row(session_factory, session, settings, tmp_path, monkeypatch):
    out = settings.model_copy(update={"output_dir": tmp_path / "out"})
    monkeypatch.setattr(worker, "_settings_override", out)
    monkeypatch.setattr(worker, "run_scenario", lambda *a, **k: _Result())
    rid = _queued_with_output(session, tmp_path / "out" / "r1")
    worker.execute_run(rid, session_factory)
    session.expire_all()
    row = session.get(Run, rid)
    assert row.status == "succeeded" and "results" not in row.result
    doc = json.loads((tmp_path / "out" / "r1" / "results.json").read_text())
    assert doc["results"] == {"time": [1, 2, 3]} and doc["engine_version"] == "9.9.9"


def test_worker_fails_loudly_when_results_have_nowhere_to_go(session_factory, session, tmp_path, monkeypatch):
    monkeypatch.setattr(worker, "_settings_override", None)
    monkeypatch.setattr(worker, "run_scenario", lambda *a, **k: _Result())
    rid = svc.create_run(session, {"scenario": {"name": "x"}}, validate=False).id
    worker.execute_run(rid, session_factory)
    session.expire_all()
    assert session.get(Run, rid).status == "failed"


def test_worker_refuses_base_dir_outside_output_dir(session_factory, session, settings, tmp_path, monkeypatch):
    out = settings.model_copy(update={"output_dir": tmp_path / "out"})
    monkeypatch.setattr(worker, "_settings_override", out)
    monkeypatch.setattr(worker, "run_scenario", lambda *a, **k: _Result())
    rid = _queued_with_output(session, tmp_path / "elsewhere")
    worker.execute_run(rid, session_factory)
    session.expire_all()
    assert session.get(Run, rid).status == "failed"
    assert not (tmp_path / "elsewhere").exists()


# ---- results endpoint -------------------------------------------------------------------------------------------


def _finish_with_file(api, rid, write=True):
    base = Path(_snapshot(api, rid)["output"]["base_dir"])
    if write:
        base.mkdir(parents=True, exist_ok=True)
        (base / "results.json").write_text(json.dumps({"results": {"time": [1, 2]}}))
    with api.app.state.session_factory() as s:
        s.get(Run, uuid.UUID(rid)).status = RunStatus.succeeded.value
        s.commit()


def test_results_endpoint_streams_file(api, tiny_config):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    assert api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice")).status_code == 404  # not ready
    _finish_with_file(api, rid)
    r = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice"))
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/json")
    assert r.json() == {"results": {"time": [1, 2]}}
    assert api.get(f"/api/v1/runs/{rid}/results", headers=bearer("admin")).status_code == 200


def test_results_ownership_auth_and_missing_file(api, tiny_config):
    rid = _post(api, client_config(tiny_config), who="bob").json()["id"]
    _finish_with_file(api, rid)
    assert api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice")).status_code == 404
    assert api.get(f"/api/v1/runs/{rid}/results").status_code == 401
    rid2 = _post(api, client_config(tiny_config)).json()["id"]
    _finish_with_file(api, rid2, write=False)
    assert api.get(f"/api/v1/runs/{rid2}/results", headers=bearer("alice")).status_code == 404


def test_results_path_outside_output_dir_is_refused(api, tiny_config, tmp_path):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    evil = tmp_path / "evil"
    evil.mkdir()
    (evil / "results.json").write_text("{}")
    with api.app.state.session_factory() as s:
        row = s.get(Run, uuid.UUID(rid))
        row.composition_snapshot = {**row.composition_snapshot, "output": {"base_dir": str(evil)}}
        row.status = "succeeded"
        s.commit()
    assert api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice")).status_code == 404
