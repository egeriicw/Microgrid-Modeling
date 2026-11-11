"""Story A1: HTTP run endpoints (create, list, detail, SSE events, cancel), server-built data section, ownership."""

from __future__ import annotations

import json
import shutil
import threading
import time
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from microgrid_api.db.models import Run, RunEvent, RunStatus
from microgrid_api.main import create_app
from microgrid_api.services import runs as svc

from .http_support import bearer, client_config, make_settings


@pytest.fixture
def http_settings(tmp_path, synthetic_dataset, tiny_config):
    return make_settings(tmp_path, synthetic_dataset, tiny_config)


@pytest.fixture
def api(http_settings):
    app = create_app(http_settings)
    app.state.enqueued = []
    app.state.enqueue = lambda run_id: app.state.enqueued.append(str(run_id))
    with TestClient(app) as c:
        yield c


def _post(api, cfg, who="alice", **extra):
    return api.post("/api/v1/runs", headers=bearer(who), json={"config": cfg, **extra})


def _row(api, run_id) -> Run:
    with api.app.state.session_factory() as s:
        row = s.get(Run, uuid.UUID(run_id))
        assert row is not None
        return row


def test_requires_authentication(api, tiny_config):
    assert api.post("/api/v1/runs", json={"config": client_config(tiny_config)}).status_code == 401
    assert api.get("/api/v1/runs").status_code == 401
    r = api.get("/api/v1/runs", headers={"Authorization": "Bearer nope"})
    assert r.status_code == 401
    assert "nope" not in r.text


def test_create_run_builds_data_section_server_side(api, tiny_config, http_settings):
    r = _post(api, client_config(tiny_config))
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["status"] == "queued"
    assert body["owner"] == "alice"
    assert api.app.state.enqueued == [body["id"]]
    row = _row(api, body["id"])
    data = row.composition_snapshot["data"]
    root = http_settings.data_dir.resolve()
    assert data["background_dir"] == str(root / "background")
    assert row.owner == "alice"
    assert set(row.seeds) == {"composition_seed", "archetype_seed", "perturbation_seed"}
    # server paths are never echoed to clients
    assert str(root) not in r.text
    detail = api.get(f"/api/v1/runs/{body['id']}", headers=bearer("alice"))
    assert str(root) not in detail.text
    assert "data" not in detail.json()["composition_snapshot"]


def test_client_supplied_data_section_is_rejected_loudly(api, tiny_config, synthetic_dataset):
    cfg = client_config(tiny_config)
    cfg["data"] = synthetic_dataset.data
    r = _post(api, cfg)
    assert r.status_code == 422
    assert "data" in r.json()["detail"]
    assert str(synthetic_dataset.root) not in r.text
    assert api.app.state.enqueued == []
    with api.app.state.session_factory() as s:
        assert s.scalars(select(Run)).all() == []


@pytest.mark.parametrize("key", ["output_dir", "data_dir", "data_dirs"])
def test_client_supplied_path_keys_rejected(api, tiny_config, key):
    cfg = client_config(tiny_config)
    cfg[key] = "/etc"
    r = _post(api, cfg)
    assert r.status_code == 422
    assert "/etc" not in r.text


def test_data_at_body_top_level_rejected(api, tiny_config):
    r = api.post(
        "/api/v1/runs",
        headers=bearer("alice"),
        json={"config": client_config(tiny_config), "data": {"background_dir": "/etc"}},
    )
    assert r.status_code == 422
    assert api.app.state.enqueued == []


def test_exactly_one_of_config_or_composition_id(api, tiny_config):
    r = api.post("/api/v1/runs", headers=bearer("alice"), json={})
    assert r.status_code == 422
    r = api.post(
        "/api/v1/runs",
        headers=bearer("alice"),
        json={"config": client_config(tiny_config), "composition_id": "small-mixed"},
    )
    assert r.status_code == 422


def test_invalid_config_is_422_and_nothing_stored(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["ensemble"] = {"mode": "bogus"}
    assert _post(api, cfg).status_code == 422
    assert api.app.state.enqueued == []


def test_unconfigured_data_dir_is_503(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=None)
    with TestClient(create_app(s)) as c:
        r = c.post("/api/v1/runs", headers=bearer("alice"), json={"config": client_config(tiny_config)})
    assert r.status_code == 503


def test_symlink_escaping_data_root_is_refused(tmp_path, synthetic_dataset, tiny_config):
    root = tmp_path / "root"
    shutil.copytree(synthetic_dataset.root, root)
    outside = tmp_path / "outside"
    outside.mkdir()
    shutil.rmtree(root / "background")
    (root / "background").symlink_to(outside)
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=root)
    with TestClient(create_app(s)) as c:
        r = c.post("/api/v1/runs", headers=bearer("alice"), json={"config": client_config(tiny_config)})
    assert r.status_code == 503
    assert str(outside) not in r.text


def test_seed_override_is_stored(api, tiny_config):
    seeds = {"composition_seed": 7, "archetype_seed": 8, "perturbation_seed": 9}
    r = _post(api, client_config(tiny_config), seeds=seeds)
    assert r.status_code == 202
    assert r.json()["seeds"] == seeds
    assert _post(api, client_config(tiny_config), seeds={"bogus": 1}).status_code == 422


def test_enqueue_failure_marks_run_failed_and_returns_503(api, tiny_config):
    def boom(run_id):
        raise ConnectionError("redis://user:secret@host")

    api.app.state.enqueue = boom
    r = _post(api, client_config(tiny_config))
    assert r.status_code == 503
    assert "secret" not in r.text
    with api.app.state.session_factory() as s:
        row = s.scalars(select(Run)).one()
        assert row.status == RunStatus.failed.value
        assert "secret" not in (row.error_message or "")


def test_list_and_detail_respect_ownership(api, tiny_config):
    a = _post(api, client_config(tiny_config), who="alice").json()["id"]
    b = _post(api, client_config(tiny_config), who="bob").json()["id"]
    la = api.get("/api/v1/runs", headers=bearer("alice")).json()
    assert [i["id"] for i in la["items"]] == [a] and la["total"] == 1
    assert "result" not in la["items"][0]
    assert api.get(f"/api/v1/runs/{b}", headers=bearer("alice")).status_code == 404
    assert api.get(f"/api/v1/runs/{a}", headers=bearer("alice")).status_code == 200
    ladmin = api.get("/api/v1/runs", headers=bearer("admin")).json()
    assert {i["id"] for i in ladmin["items"]} == {a, b}
    assert api.get(f"/api/v1/runs/{b}", headers=bearer("admin")).status_code == 200
    assert api.get(f"/api/v1/runs/{uuid.uuid4()}", headers=bearer("admin")).status_code == 404


def test_list_pagination_and_status_filter(api, tiny_config):
    for _ in range(3):
        _post(api, client_config(tiny_config))
    page = api.get("/api/v1/runs?limit=2&offset=0", headers=bearer("alice")).json()
    assert len(page["items"]) == 2 and page["total"] == 3
    assert api.get("/api/v1/runs?status=succeeded", headers=bearer("alice")).json()["total"] == 0
    assert api.get("/api/v1/runs?status=bogus", headers=bearer("alice")).status_code == 422
    assert api.get("/api/v1/runs?limit=0", headers=bearer("alice")).status_code == 422


def test_detail_includes_result(api, tiny_config):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    with api.app.state.session_factory() as s:
        row = s.get(Run, uuid.UUID(rid))
        row.status = RunStatus.succeeded.value
        row.result = {"x": 1}
        s.commit()
    d = api.get(f"/api/v1/runs/{rid}", headers=bearer("alice")).json()
    assert d["result"] == {"x": 1} and d["status"] == "succeeded"


def test_cancel_own_queued_run(api, tiny_config):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    revoked: list[str] = []

    def fake_cancel(session, run_id):
        revoked.append(str(run_id))
        return svc.request_cancel(session, run_id)

    api.app.state.cancel_run = fake_cancel
    r = api.post(f"/api/v1/runs/{rid}/cancel", headers=bearer("alice"))
    assert r.status_code == 200
    assert r.json()["status"] == "cancelled" and r.json()["cancel_requested"] is True
    assert revoked == [rid]


def test_cancel_other_users_run_is_404_and_changes_nothing(api, tiny_config):
    api.app.state.cancel_run = lambda session, run_id: svc.request_cancel(session, run_id)
    rid = _post(api, client_config(tiny_config), who="bob").json()["id"]
    r = api.post(f"/api/v1/runs/{rid}/cancel", headers=bearer("alice"))
    assert r.status_code == 404
    assert _row(api, rid).status == "queued"
    assert api.post(f"/api/v1/runs/{rid}/cancel", headers=bearer("admin")).status_code == 200


def test_cancel_requires_auth(api, tiny_config):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    assert api.post(f"/api/v1/runs/{rid}/cancel").status_code == 401


def _seed_events(api, rid, n=3, status=RunStatus.succeeded):
    with api.app.state.session_factory() as s:
        for i in range(n):
            s.add(RunEvent(run_id=uuid.UUID(rid), stage="ensemble", message=f"m{i}", payload={"i": i}))
        row = s.get(Run, uuid.UUID(rid))
        row.status = status.value
        s.commit()


def _parse_sse(text: str) -> list[dict]:
    out = []
    for block in text.replace("\r\n", "\n").strip().split("\n\n"):
        ev: dict = {}
        for line in block.splitlines():
            if line.startswith(":"):
                continue
            k, _, v = line.partition(":")
            ev[k.strip()] = v.strip()
        if ev:
            out.append(ev)
    return out


def test_sse_streams_events_then_final_status(api, tiny_config):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    _seed_events(api, rid)
    r = api.get(f"/api/v1/runs/{rid}/events", headers=bearer("alice"))
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    evs = _parse_sse(r.text)
    progress = [e for e in evs if e["event"] == "progress"]
    assert [json.loads(e["data"])["message"] for e in progress] == ["m0", "m1", "m2"]
    assert evs[-1]["event"] == "status" and json.loads(evs[-1]["data"])["status"] == "succeeded"
    # resume after the first event
    r2 = api.get(f"/api/v1/runs/{rid}/events", headers={**bearer("alice"), "Last-Event-ID": progress[0]["id"]})
    again = [e for e in _parse_sse(r2.text) if e["event"] == "progress"]
    assert [json.loads(e["data"])["message"] for e in again] == ["m1", "m2"]


def test_sse_follows_a_running_run_until_it_finishes(api, tiny_config):
    rid = _post(api, client_config(tiny_config)).json()["id"]
    with api.app.state.session_factory() as s:
        s.get(Run, uuid.UUID(rid)).status = RunStatus.running.value
        s.commit()

    def finish():
        time.sleep(0.2)
        _seed_events(api, rid, n=1)

    t = threading.Thread(target=finish)
    t.start()
    r = api.get(f"/api/v1/runs/{rid}/events", headers=bearer("alice"))
    t.join()
    evs = _parse_sse(r.text)
    assert evs[-1]["event"] == "status"
    assert any(e["event"] == "progress" for e in evs)


def test_sse_ownership_and_auth(api, tiny_config):
    rid = _post(api, client_config(tiny_config), who="bob").json()["id"]
    assert api.get(f"/api/v1/runs/{rid}/events", headers=bearer("alice")).status_code == 404
    assert api.get(f"/api/v1/runs/{rid}/events").status_code == 401


def test_client_with_filters_forbidden_admin_allowed(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["archetype_filters"]["resstock"]["mf_small"]["attributes"] = [{"column": "x", "op": "not_null"}]
    r = _post(api, cfg, who="alice")
    assert r.status_code == 403
    assert "filters.use" in r.json()["detail"]
    assert api.app.state.enqueued == []
    assert _post(api, cfg, who="admin").status_code == 202
