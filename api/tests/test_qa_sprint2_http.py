"""Independent QA of the Sprint 2 HTTP layer (auth, run endpoints, demo, reaper, migration, openapi).

Regression guards found during review; the "defect" tests were xfail until the fix pass and now pass."""

from __future__ import annotations

import copy
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
import yaml
from fastapi.testclient import TestClient

from microgrid_api import openapi_export, worker
from microgrid_api.config import ApiKey, ConfigurationError, Settings
from microgrid_api.db.migrate import upgrade_to_head
from microgrid_api.db.models import Run, RunStatus
from microgrid_api.main import create_app

from .http_support import KEYS, bearer, client_config, make_settings


@pytest.fixture
def http_settings(tmp_path, synthetic_dataset, tiny_config):
    return make_settings(tmp_path, synthetic_dataset, tiny_config)


@pytest.fixture
def api(http_settings):
    app = create_app(http_settings)
    app.state.enqueued = []
    app.state.enqueue = lambda run_id: app.state.enqueued.append(str(run_id))
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


def _post(api, cfg, who="alice", **extra):
    return api.post("/api/v1/runs", headers=bearer(who), json={"config": cfg, **extra})


def _bare_run(**kw):
    base = {"id": uuid.uuid4(), "composition_snapshot": {}, "seeds": {}, "engine_version": "x", "progress": {}}
    base.update(kw)
    return Run(**base)


# ---- defects ----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("base", ["/etc/cron.d", "../../escape", "/tmp/anything"])
def test_defect_client_output_base_dir_is_rejected(api, tiny_config, base):
    cfg = client_config(tiny_config)
    cfg["output"] = {"base_dir": base}
    assert _post(api, cfg).status_code == 422


def test_defect_snapshot_output_under_server_output_dir(api, tiny_config, http_settings):
    r = _post(api, client_config(tiny_config))
    assert r.status_code == 202
    with api.app.state.session_factory() as s:
        snap = s.get(Run, uuid.UUID(r.json()["id"])).composition_snapshot
    assert snap.get("output", {}).get("base_dir", "").startswith(str(http_settings.output_dir))


def test_defect_huge_last_event_id(api, tiny_config):
    # Today: HTTP 200 with an empty body (the generator dies on the DB OverflowError); expected: 422 up front.
    rid = _post(api, client_config(tiny_config)).json()["id"]
    r = api.get(f"/api/v1/runs/{rid}/events", headers={**bearer("alice"), "Last-Event-ID": str(10**30)})
    assert r.status_code == 422


def test_defect_huge_offset(api):
    r = api.get("/api/v1/runs?offset=" + str(10**30), headers=bearer("alice"))
    assert r.status_code in (200, 422)


@pytest.mark.parametrize("seed", [-1, 2**70])
def test_defect_bad_seed_rejected_at_creation(api, tiny_config, seed):
    r = _post(api, client_config(tiny_config), seeds={"composition_seed": seed})
    assert r.status_code == 422


def test_defect_client_resource_cap(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["ensemble"] = {"mode": "fixed", "max_runs": 10**9}
    assert _post(api, cfg).status_code == 422


def test_defect_demo_composition_with_output_base_dir_refused(tmp_path, synthetic_dataset, tiny_config):
    cfg = client_config(tiny_config)
    cfg["output"] = {"base_dir": "/tmp/elsewhere"}
    s = make_settings(tmp_path, synthetic_dataset, tiny_config)
    assert s.demo_compositions_file is not None
    s.demo_compositions_file.write_text(yaml.safe_dump({"compositions": {"x": cfg}}))
    with pytest.raises(ConfigurationError):
        create_app(s)


# ---- auth guards (regression, passing) -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "header",
    [
        "Bearer",
        "Bearer ",
        "Basic " + KEYS["admin"],
        "Bearer  " + KEYS["admin"] + "x",
        "Token " + KEYS["admin"],
        "Bearer " + KEYS["admin"][:-1],
        "Bearer " + KEYS["admin"].upper(),
        "Bearer é" + KEYS["admin"],
    ],
)
def test_bad_authorization_headers_are_401(api, header):
    raw = header.encode("utf-8")  # bytes: httpx refuses a non-ASCII str header
    assert api.get("/api/v1/runs", headers={b"Authorization": raw}).status_code == 401


def test_lowercase_scheme_and_header_name_work(api):
    assert api.get("/api/v1/runs", headers={"authorization": "bearer " + KEYS["alice"]}).status_code == 200


def test_query_string_token_and_role_headers_ignored(api):
    assert api.get(f"/api/v1/runs?token={KEYS['admin']}&api_key={KEYS['admin']}").status_code == 401
    r = api.get("/api/v1/runs", headers={**bearer("alice"), "X-Role": "admin", "X-User": "bob"})
    assert r.status_code == 200 and r.json()["total"] == 0


def test_role_in_body_cannot_escalate(api, tiny_config):
    r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": client_config(tiny_config), "role": "admin"})
    assert r.status_code == 422


def test_short_and_duplicate_keys_refused():
    with pytest.raises(ValueError):
        ApiKey(subject="x", role="client", key="short")
    a = ApiKey(subject="a", role="client", key="k" * 16)
    b = ApiKey(subject="b", role="admin", key="k" * 16)
    with pytest.raises(ConfigurationError):
        Settings(environment="test", database_url="sqlite://", api_keys=[a, b], _env_file=None)
    c = ApiKey(subject="a", role="client", key="z" * 16)
    with pytest.raises(ConfigurationError):
        Settings(environment="test", database_url="sqlite://", api_keys=[a, c], _env_file=None)


def test_no_keys_configured_means_everything_401(tmp_path):
    s = Settings(environment="test", database_url=f"sqlite:///{tmp_path / 'n.db'}", api_keys=[], _env_file=None)
    upgrade_to_head(s.require_database_url())
    with TestClient(create_app(s)) as c:
        assert c.get("/api/v1/runs", headers={"Authorization": "Bearer "}).status_code == 401
        assert c.get("/api/v1/runs", headers={"Authorization": "Bearer anything-at-all-123456"}).status_code == 401


def test_secrets_not_in_repr_or_openapi(http_settings):
    text = repr(http_settings) + str(http_settings) + openapi_export.OPENAPI_PATH.read_text()
    for k in KEYS.values():
        assert k not in text


# ---- cross-user (passing) ---------------------------------------------------------------------------------------


def test_cross_user_all_endpoints_404_and_unprobeable(api, tiny_config):
    rid = _post(api, client_config(tiny_config), who="bob").json()["id"]
    ghost = str(uuid.uuid4())
    for path, method in [
        ("/api/v1/runs/{}", "get"),
        ("/api/v1/runs/{}/events", "get"),
        ("/api/v1/runs/{}/cancel", "post"),
    ]:
        a = getattr(api, method)(path.format(rid), headers=bearer("alice"))
        b = getattr(api, method)(path.format(ghost), headers=bearer("alice"))
        assert a.status_code == b.status_code == 404
        assert a.json() == b.json()
    assert api.get("/api/v1/runs", headers=bearer("alice")).json()["total"] == 0
    assert api.get("/api/v1/runs?status=queued", headers=bearer("demo")).json()["total"] == 0
    assert api.get("/api/v1/runs", headers=bearer("admin")).json()["total"] == 1


def test_legacy_owner_null_run_visible_only_to_admin(api):
    with api.app.state.session_factory() as s:
        r = _bare_run(status="queued")
        s.add(r)
        s.commit()
        rid = str(r.id)
    assert api.get(f"/api/v1/runs/{rid}", headers=bearer("alice")).status_code == 404
    assert api.get(f"/api/v1/runs/{rid}", headers=bearer("admin")).status_code == 200


# ---- smuggling / nested keys (passing) --------------------------------------------------------------------------


@pytest.mark.parametrize("key", ["data", "data_dir", "output_dir", "data_dirs"])
def test_top_level_path_smuggling_rejected(api, tiny_config, key):
    cfg = client_config(tiny_config)
    cfg[key] = {"background_dir": "/etc"} if key == "data" else "/etc"
    assert _post(api, cfg).status_code == 422
    assert api.app.state.enqueued == []


def test_unknown_top_level_section_rejected(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["Data"] = {"background_dir": "/etc"}
    assert _post(api, cfg).status_code == 422


def test_null_data_rejected(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["data"] = None
    assert _post(api, cfg).status_code == 422


# ---- demo caps (passing) ----------------------------------------------------------------------------------------


def test_demo_cannot_send_config_seeds_or_empty_seeds(api, tiny_config):
    assert _post(api, client_config(tiny_config), who="demo").status_code == 403
    for seeds in ({}, {"composition_seed": 1}):
        r = api.post("/api/v1/runs", headers=bearer("demo"), json={"composition_id": "small-mixed", "seeds": seeds})
        assert r.status_code == 403


def test_demo_composition_uses_fixed_seed(api, http_settings):
    r = api.post("/api/v1/runs", headers=bearer("demo"), json={"composition_id": "small-mixed"})
    assert r.status_code == 202
    assert set(r.json()["seeds"].values()) == {http_settings.demo_seed}


def test_demo_unknown_composition_and_body_variants(api):
    for body in ({"composition_id": "nope"}, {"composition_id": "../x"}, {}, {"composition_id": None, "config": None}):
        assert api.post("/api/v1/runs", headers=bearer("demo"), json=body).status_code in (404, 422)


def test_demo_library_not_mutated_by_runs(api):
    before = copy.deepcopy(api.app.state.demo_compositions)
    api.post("/api/v1/runs", headers=bearer("demo"), json={"composition_id": "small-mixed"})
    assert api.app.state.demo_compositions == before


def test_demo_file_over_caps_fails_loudly(tmp_path, synthetic_dataset, tiny_config):
    with pytest.raises(ConfigurationError):
        create_app(make_settings(tmp_path, synthetic_dataset, tiny_config, demo_max_buildings=3))
    with pytest.raises(ConfigurationError):
        create_app(make_settings(tmp_path, synthetic_dataset, tiny_config, demo_max_runs=5))


# ---- SSE (passing) ----------------------------------------------------------------------------------------------


def test_sse_bad_last_event_id(api, tiny_config):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    r = api.get(f"/api/v1/runs/{rid}/events", headers={**bearer("alice"), "Last-Event-ID": "abc"})
    assert r.status_code == 422


def test_sse_terminal_run_gets_final_status(api, tiny_config):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    with api.app.state.session_factory() as s:
        row = s.get(Run, uuid.UUID(rid))
        row.status = RunStatus.cancelled.value
        s.commit()
    r = api.get(f"/api/v1/runs/{rid}/events", headers=bearer("alice"))
    assert "cancelled" in r.text


# ---- enqueue failure --------------------------------------------------------------------------------------------


def test_enqueue_failure_does_not_leak_broker_url(api, tiny_config, caplog):
    def boom(run_id):
        raise RuntimeError("cannot connect redis://:hunter2@broker.internal:6379/0")

    api.app.state.enqueue = boom
    r = _post(api, client_config(tiny_config))
    assert r.status_code == 503
    assert "hunter2" not in r.text and "hunter2" not in caplog.text
    with api.app.state.session_factory() as s:
        row = s.query(Run).one()
        assert row.status == "failed" and "hunter2" not in (row.error_message or "")


# ---- reaper ----------------------------------------------------------------------------------------------------


def test_reaper_naive_and_aware_started_at_and_status_guard(http_settings, api):
    sf = api.app.state.session_factory
    now = datetime.now(UTC)
    with sf() as s:
        runs = {
            "aware": _bare_run(status="running", started_at=now - timedelta(hours=3)),
            "naive": _bare_run(status="running", started_at=(now - timedelta(hours=3)).replace(tzinfo=None)),
            "fresh": _bare_run(status="running", started_at=now - timedelta(minutes=5)),
            "done": _bare_run(status="succeeded", started_at=now - timedelta(hours=9)),
        }
        s.add_all(runs.values())
        s.commit()
    reaped = worker.reap_once(http_settings, sf)
    assert set(reaped) == {runs["aware"].id, runs["naive"].id}
    with sf() as s:
        assert s.get(Run, runs["fresh"].id).status == "running"
        assert s.get(Run, runs["done"].id).status == "succeeded"


def test_reaper_loses_race_to_finishing_worker(http_settings, api, monkeypatch):
    sf = api.app.state.session_factory
    with sf() as s:
        r = _bare_run(status="running", started_at=datetime.now(UTC) - timedelta(hours=3))
        s.add(r)
        s.commit()
        rid = r.id
    real = worker.find_stale_running

    def finish_then_return(session, older):
        ids = real(session, older)
        with sf() as s2:
            s2.get(Run, rid).status = "succeeded"
            s2.commit()
        return ids

    monkeypatch.setattr(worker, "find_stale_running", finish_then_return)
    assert worker.reap_once(http_settings, sf) == []
    with sf() as s:
        assert s.get(Run, rid).status == "succeeded"


# ---- openapi ----------------------------------------------------------------------------------------------------


def test_openapi_check_detects_schema_mutation(tmp_path):
    spec = json.loads(openapi_export.render())
    spec["components"]["schemas"]["RunCreate"]["properties"]["composition_id"]["title"] = "Mutated"
    p = tmp_path / "o.json"
    p.write_text(json.dumps(spec, indent=2, sort_keys=True) + "\n")
    assert openapi_export.main(["--check", "--path", str(p)]) == 1
