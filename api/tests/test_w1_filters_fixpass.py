"""W1-FILTERS fix pass: security review items A-K (results paths, run-path pool check, caps, number bounds, nesting
depth, body-cap middleware, error redaction for non-admins, selection categories, type-specific attributes, version
CHECK constraint, activation concurrency)."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import shutil
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from microgrid.community_load.synthetic.config import DataConfig
from sqlalchemy.exc import IntegrityError

from microgrid_api import openapi_export
from microgrid_api.db.models import ExposedAttributeActivation, ExposedAttributeVersion, Run
from microgrid_api.limits import BodySizeLimit
from microgrid_api.main import create_app
from microgrid_api.services import exposed_attributes as ea_svc

from .http_support import bearer, client_config, make_settings

BASE = "/api/v1/exposed-attributes"
SEEDS = {"composition_seed": 11, "archetype_seed": 22, "perturbation_seed": 33}
STORIES_RANGE = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]}


@pytest.fixture(scope="module")
def data_root(synthetic_dataset, tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("fixpass_data") / "data"
    shutil.copytree(synthetic_dataset.root, root)
    shutil.copy(synthetic_dataset.resstock_chars, root / "background" / DataConfig().resstock_chars_file)
    shutil.copy(synthetic_dataset.comstock_chars, root / "background" / DataConfig().comstock_chars_file)
    return root


def _settings(tmp_path, synthetic_dataset, tiny_config, data_root, **over):
    return make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=data_root, **over)


def _client(settings) -> TestClient:
    app = create_app(settings)
    app.state.enqueued = []
    app.state.enqueue = lambda run_id: app.state.enqueued.append(str(run_id))
    return TestClient(app)


@pytest.fixture
def fp_settings(tmp_path, synthetic_dataset, tiny_config, data_root, monkeypatch):
    from microgrid_api import worker

    s = _settings(tmp_path, synthetic_dataset, tiny_config, data_root)
    monkeypatch.setattr(worker, "_settings_override", s)
    return s


@pytest.fixture
def api(fp_settings):
    with _client(fp_settings) as c:
        yield c


@pytest.fixture
def client_api(tmp_path, synthetic_dataset, tiny_config, data_root):
    """Clients have filters.use through configuration (so their error messages can be checked)."""
    s = _settings(
        tmp_path, synthetic_dataset, tiny_config, data_root, permission_overrides={"client": {"filters.use": True}}
    )
    with _client(s) as c:
        yield c


def _post_run(api, body, who="admin"):
    return api.post("/api/v1/runs", headers=bearer(who), json=body)


def _preview(api, body, who="admin"):
    return api.post(f"{BASE}/preview", headers=bearer(who), json=body)


def _activate_doc(api, doc) -> None:
    latest = api.get(f"{BASE}/versions", headers=bearer("admin")).json()["latest_version"]
    r = api.post(
        f"{BASE}/versions", headers=bearer("admin"), json={"base_version": latest, "document": doc, "activate": True}
    )
    assert r.status_code == 201, r.text


def _all_columns() -> set[str]:
    return {c for a in ea_svc.packaged_document()["attributes"] for c in a["columns"].values()}


# --- A. results document never carries server paths --------------------------------------------------------------


def test_worker_writes_a_path_free_results_file_and_serves_it(api, tiny_config, data_root, fp_settings):
    rid = _post_run(api, {"config": client_config(tiny_config), "seeds": SEEDS}, who="alice").json()["id"]
    from microgrid_api import worker

    worker.execute_run(rid, api.app.state.session_factory)
    with api.app.state.session_factory() as s:
        base = Path(s.get(Run, uuid.UUID(rid)).composition_snapshot["output"]["base_dir"])
    on_disk = (base / "results.json").read_text()
    for root in (str(data_root.resolve()), str(fp_settings.output_dir.resolve())):
        assert root not in on_disk
    r = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice"))
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/json")
    assert str(data_root.resolve()) not in r.text and str(fp_settings.output_dir.resolve()) not in r.text
    assert r.json()["pool_summary"]


def test_results_file_already_on_disk_with_paths_is_filtered_when_served(api, tiny_config, data_root, fp_settings):
    rid = _post_run(api, {"config": client_config(tiny_config)}, who="alice").json()["id"]
    with api.app.state.session_factory() as s:
        row = s.get(Run, uuid.UUID(rid))
        snap = copy.deepcopy(row.composition_snapshot)
        row.status = "succeeded"
        s.commit()
    base = Path(snap["output"]["base_dir"])
    base.mkdir(parents=True, exist_ok=True)
    legacy = {"config_snapshot": snap, "pool_summary": {"resstock/mf_small": {"matched": 1}}, "results": {"x": [1]}}
    (base / "results.json").write_text(json.dumps(legacy))
    assert str(data_root.resolve()) in (base / "results.json").read_text()
    r = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice"))
    assert r.status_code == 200
    assert str(data_root.resolve()) not in r.text and str(fp_settings.output_dir.resolve()) not in r.text
    assert r.json()["results"] == {"x": [1]} and r.json()["config_snapshot"]["composition"]


# --- B. zero pool / missing column refused at creation ------------------------------------------------------------


def test_zero_pool_in_a_category_that_cannot_be_sampled_is_allowed(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["composition"]["categories"]["mf_small"] = {"min_fraction": 1.0, "max_fraction": 1.0, "source": "resstock"}
    cfg["composition"]["categories"]["mf_mid"] = {"min_fraction": 0.0, "max_fraction": 0.0, "source": "resstock"}
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 150, "max": 200}]}
    r = _post_run(api, {"config": cfg, "filter_selections": sel})
    assert r.status_code == 202, r.text


def test_zero_pool_refusal_names_the_category(api, tiny_config):
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 150, "max": 200}]}
    r = _post_run(api, {"config": client_config(tiny_config), "filter_selections": sel})
    assert r.status_code == 422 and "mf_mid" in r.json()["detail"] and "no buildings" in r.json()["detail"]
    assert api.app.state.enqueued == []


# --- C. category caps and a single cold read --------------------------------------------------------------------


@pytest.mark.parametrize("where", ["composition", "archetype_filters"])
def test_category_count_capped_for_every_role(api, tiny_config, where):
    cfg = client_config(tiny_config)
    for i in range(51):
        if where == "composition":
            cfg["composition"]["categories"][f"c{i}"] = {"min_fraction": 0.0, "max_fraction": 0.0}
        else:
            cfg["archetype_filters"]["resstock"][f"c{i}"] = {"building_type_heights": ["x"]}
    for who in ("admin", "alice"):
        r = _post_run(api, {"config": cfg}, who=who)
        assert r.status_code == 422 and "50" in r.json()["detail"], (who, r.text)
    r = _preview(api, {"config": cfg})
    assert r.status_code == 422 and "50" in r.json()["detail"]


def test_concurrent_cold_reads_load_the_workbook_once(data_root, monkeypatch):
    ea_svc.clear_characteristics_cache()
    calls: list[str] = []
    real = ea_svc.load_resstock_characteristics

    def slow(path):
        calls.append(str(path))
        time.sleep(0.2)
        return real(path)

    monkeypatch.setattr(ea_svc, "load_resstock_characteristics", slow)
    n = 6
    barrier = threading.Barrier(n)
    sizes: list[int] = []

    def reader():
        barrier.wait()
        df = ea_svc._characteristics(
            data_root, str(data_root / "background"), DataConfig().resstock_chars_file, "resstock"
        )
        sizes.append(len(df))

    threads = [threading.Thread(target=reader) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    try:
        assert len(calls) == 1 and len(sizes) == n and len(set(sizes)) == 1
    finally:
        ea_svc.clear_characteristics_cache()


# --- D. number bounds and an /active that cannot 500 ---------------------------------------------------------------


@pytest.mark.parametrize("token", ["1" + "0" * 400, "-" + "1" + "0" * 400, "1e300", "10000000000000000"])
def test_huge_numbers_in_a_document_are_422_and_never_stored(api, token):
    doc = ea_svc.packaged_document()
    doc["attributes"][0]["max"] = "__NUM__"
    content = json.dumps({"base_version": 0, "document": doc}).replace('"__NUM__"', token)
    r = api.post(f"{BASE}/versions", headers={**bearer("admin"), "Content-Type": "application/json"}, content=content)
    assert r.status_code == 422, r.text
    with api.app.state.session_factory() as s:
        assert s.query(ExposedAttributeVersion).count() == 0


@pytest.mark.parametrize("field", ["value", "min"])
def test_huge_numbers_in_selections_are_422(api, tiny_config, field):
    sel: dict[str, Any] = {"attribute": "stories", "op": "equals" if field == "value" else "range", field: "__N__"}
    body = {"config": client_config(tiny_config), "filter_selections": {"mf_mid": [sel]}}
    content = json.dumps(body).replace('"__N__"', "1" + "0" * 400)
    h = {**bearer("admin"), "Content-Type": "application/json"}
    assert api.post(f"{BASE}/preview", headers=h, content=content).status_code == 422
    assert api.post("/api/v1/runs", headers=h, content=content).status_code == 422


def test_stored_document_with_huge_numbers_never_500s(api):
    bad = ea_svc.packaged_document()
    bad["attributes"][0]["max"] = 10**400
    with api.app.state.session_factory() as s:
        s.add(ExposedAttributeVersion(version=1, document=bad, based_on=0, created_by="admin"))
        s.flush()
        s.add(ExposedAttributeActivation(version=1, activated_by="admin"))
        s.commit()
    r = api.get(f"{BASE}/active", headers=bearer("admin"))
    assert r.status_code == 503, r.text
    assert api.get(f"{BASE}/versions/1", headers=bearer("admin")).status_code in (422, 503)
    assert api.post(f"{BASE}/versions/1/activate", headers=bearer("admin")).status_code == 422


# --- E. nesting depth -------------------------------------------------------------------------------------------


def _nested(depth: int) -> Any:
    value: Any = "leaf"
    for _ in range(depth):
        value = {"k": value}
    return value


def test_deeply_nested_configs_are_422(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["scenario"]["extra"] = _nested(40)
    r = _post_run(api, {"config": cfg})
    assert r.status_code == 422 and "nested" in r.json()["detail"]
    r = _preview(api, {"config": cfg})
    assert r.status_code == 422 and "nested" in r.json()["detail"]
    cfg["scenario"]["extra"] = [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["x"]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]
    assert _post_run(api, {"config": cfg}).status_code == 422


def test_config_depth_helper_boundary():
    from microgrid_api.routers.runs import MAX_CONFIG_DEPTH, config_too_deep

    assert MAX_CONFIG_DEPTH == 32
    assert not config_too_deep(_nested(31))
    assert config_too_deep(_nested(33))


# --- F. body-cap middleware does not depend on FastAPI -------------------------------------------------------------


def _run_asgi(app, chunks: list[bytes]) -> list[dict[str, Any]]:
    sent: list[dict[str, Any]] = []
    queue = [{"type": "http.request", "body": c, "more_body": i < len(chunks) - 1} for i, c in enumerate(chunks)]

    async def receive():
        return queue.pop(0) if queue else {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "method": "POST", "path": "/", "headers": []}
    asyncio.run(app(scope, receive, send))
    return sent


async def _plain(scope, receive, send):
    while (await receive()).get("more_body"):
        pass
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


async def _swallows_errors(scope, receive, send):
    try:
        while (await receive()).get("more_body"):
            pass
    except Exception:  # an app that hides the error must not turn an oversized body into a 200
        pass
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


@pytest.mark.parametrize("inner", [_plain, _swallows_errors])
def test_body_cap_on_a_plain_asgi_app_returns_413(inner):
    sent = _run_asgi(BodySizeLimit(inner, max_bytes=10), [b"x" * 6, b"x" * 6])
    starts = [m for m in sent if m["type"] == "http.response.start"]
    assert len(starts) == 1 and starts[0]["status"] == 413
    assert b"ok" not in b"".join(m.get("body", b"") for m in sent)


def test_body_cap_after_the_response_started_returns_quietly():
    async def streams_then_reads(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})
        while (await receive()).get("more_body"):
            pass

    sent = _run_asgi(BodySizeLimit(streams_then_reads, max_bytes=10), [b"x" * 6, b"x" * 6])
    starts = [m for m in sent if m["type"] == "http.response.start"]
    assert len(starts) == 1 and starts[0]["status"] == 200


# --- G. non-admins never see column names or data values in errors -------------------------------------------------


def test_selection_error_for_a_client_hides_columns_admin_sees_them(client_api, tiny_config):
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 6, "max": 5}]}
    body = {"config": client_config(tiny_config), "filter_selections": sel}
    admin = _preview(client_api, body, who="admin")
    assert admin.status_code == 422 and "in.geometry_stories" in admin.text
    for r in (_preview(client_api, body, who="alice"), _post_run(client_api, body, who="alice")):
        assert r.status_code == 422
        assert not any(col in r.text for col in _all_columns())
        assert "stories" in r.json()["detail"] and "mf_mid" in r.json()["detail"]


def test_data_error_for_a_client_hides_columns_admin_sees_them(client_api, tiny_config):
    doc = ea_svc.packaged_document()
    for a in doc["attributes"]:
        if a["key"] == "stories":
            a["columns"]["resstock"] = "in.not_in_the_data"
    _activate_doc(client_api, doc)
    body = {"config": client_config(tiny_config), "filter_selections": STORIES_RANGE}
    admin = _preview(client_api, body, who="admin")
    assert admin.status_code == 422 and "in.not_in_the_data" in admin.text
    for r in (_preview(client_api, body, who="alice"), _post_run(client_api, body, who="alice")):
        assert r.status_code == 422, r.text
        assert "in.not_in_the_data" not in r.text and "in.geometry" not in r.text
        assert "stories" in r.json()["detail"]
    assert "in.not_in_the_data" in _post_run(client_api, body, who="admin").text


# --- H. selection categories validated; demo never sends selections ------------------------------------------------


def test_demo_with_only_empty_selection_lists_is_refused(api):
    for sel in ({"mf_small": []}, {}):
        r = _post_run(api, {"composition_id": "small-mixed", "filter_selections": sel}, who="demo")
        assert r.status_code == 403, sel
    assert _post_run(api, {"composition_id": "small-mixed"}, who="demo").status_code == 202


def test_unknown_category_with_empty_list_is_422_in_preview_too(api, tiny_config):
    r = _preview(api, {"config": client_config(tiny_config), "filter_selections": {"ghost": []}})
    assert r.status_code == 422 and "ghost" in r.json()["detail"]


# --- J. version CHECK constraint --------------------------------------------------------------------------------


@pytest.mark.parametrize("version", [0, -1])
def test_stored_version_must_be_at_least_one(api, version):
    with api.app.state.session_factory() as s:
        s.add(ExposedAttributeVersion(version=version, document={}, based_on=None, created_by="admin"))
        with pytest.raises(IntegrityError):
            s.commit()


# --- K. activation with an expected active version ---------------------------------------------------------------


def test_activation_with_stale_expected_version_is_409(api):
    _activate_doc(api, ea_svc.packaged_document())  # version 1, active
    r = api.post(f"{BASE}/versions/0/activate", headers=bearer("admin"), json={"expected_active_version": 0})
    assert r.status_code == 409 and "1" in r.json()["detail"]
    assert api.get(f"{BASE}/active", headers=bearer("admin")).json()["version"] == 1
    r = api.post(f"{BASE}/versions/0/activate", headers=bearer("admin"), json={"expected_active_version": 1})
    assert r.status_code == 200
    assert api.get(f"{BASE}/active", headers=bearer("admin")).json()["version"] == 0
    # no body keeps last-writer-wins
    assert api.post(f"{BASE}/versions/1/activate", headers=bearer("admin")).status_code == 200
    for bad in (-1, "1", True, 1.5):
        r = api.post(f"{BASE}/versions/0/activate", headers=bearer("admin"), json={"expected_active_version": bad})
        assert r.status_code == 422, bad


def test_racing_activations_with_the_same_expectation_one_wins(api, monkeypatch):
    _activate_doc(api, ea_svc.packaged_document())  # version 1 active (activation id 1)
    factory = api.app.state.session_factory
    with factory() as a, factory() as b:
        stale = ea_svc._latest_activation(b)  # b read the state before a committed
        ea_svc.activate_version(a, 0, "admin", expected_active_version=1)
        monkeypatch.setattr(ea_svc, "_latest_activation", lambda session: stale)
        with pytest.raises(ea_svc.ActivationConflict):
            ea_svc.activate_version(b, 0, "admin2", expected_active_version=1)
    monkeypatch.undo()
    with factory() as s:
        assert [
            r.activated_by for r in s.query(ExposedAttributeActivation).order_by(ExposedAttributeActivation.id)
        ] == [
            "admin",
            "admin",
        ]


def test_openapi_documents_expected_active_version():
    spec = json.loads(openapi_export.render())
    op = spec["paths"][f"{BASE}/versions/{{version}}/activate"]["post"]
    assert "expected_active_version" in json.dumps(op) or "expected_active_version" in json.dumps(
        spec["components"]["schemas"].get("ActivateRequest", {})
    )
    assert "409" in op["responses"] or "409" in op.get("description", "")


PG = os.environ.get("MICROGRID_TEST_DATABASE_URL")


@pytest.mark.integration
@pytest.mark.skipif(not PG, reason="set MICROGRID_TEST_DATABASE_URL to run Postgres tests")
def test_postgres_version_check_and_activation_race():
    """J on Postgres (CHECK version >= 1), and K with real threads: racing activations that expect the same active
    version produce exactly one activation; the others get ActivationConflict."""
    from alembic import command
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from .test_migrations import _cfg

    assert PG
    command.downgrade(_cfg(PG), "base")
    command.upgrade(_cfg(PG), "head")
    engine = create_engine(PG)
    factory = sessionmaker(engine, expire_on_commit=False)
    try:
        with factory() as s:
            s.add(ExposedAttributeVersion(version=0, document={}, based_on=None, created_by="admin"))
            with pytest.raises(IntegrityError):
                s.commit()
        with factory() as s:
            ea_svc.create_version(s, ea_svc.packaged_document(), base_version=0, subject="a", note=None, activate=True)
        n = 6
        barrier = threading.Barrier(n)
        outcomes: list[str] = []

        def activator(i: int) -> None:
            with factory() as s:
                barrier.wait()
                try:
                    ea_svc.activate_version(s, 0, f"admin{i}", expected_active_version=1)
                    outcomes.append("ok")
                except ea_svc.ActivationConflict:
                    outcomes.append("conflict")
                except Exception as exc:  # anything else would be a 500
                    outcomes.append(type(exc).__name__)

        threads = [threading.Thread(target=activator, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(outcomes) == ["conflict"] * (n - 1) + ["ok"], outcomes
        with factory() as s:
            assert s.query(ExposedAttributeActivation).count() == 2
            assert ea_svc.active_version_number(s) == 0
    finally:
        engine.dispose()
        command.downgrade(_cfg(PG), "base")


def test_lead_review_permission_is_refused_before_selection_categories_are_validated(tmp_path):
    """Lead full-diff read: a caller without filters.use must get the same 403 whether or not the selections name a
    real category (no validation detail before the refusal)."""
    import inspect

    from microgrid_api.routers import runs as runs_router

    src = inspect.getsource(runs_router.create_run)
    assert src.index('can(principal.role, "filters.use"') < src.index("check_selection_categories")
