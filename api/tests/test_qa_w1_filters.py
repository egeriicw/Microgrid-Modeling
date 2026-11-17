"""Independent QA review of W1-FILTERS (FR-ENG-11 b-f): gaps the builder's tests do not cover.

Tests marked ``xfail(strict=True)`` document defects found in review; they turn into XPASS (and so fail the suite)
once fixed, at which point the marker must be removed.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import threading
import uuid
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from fastapi.testclient import TestClient
from microgrid.community_load.synthetic.archetype import build_archetype_pool
from microgrid.community_load.synthetic.config import DataConfig, config_from_dict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from microgrid_api.db.models import ExposedAttributeActivation, ExposedAttributeVersion, Run
from microgrid_api.main import create_app
from microgrid_api.services import exposed_attributes as ea_svc

from .http_support import bearer, client_config, make_settings
from .test_migrations import _cfg

BASE = "/api/v1/exposed-attributes"
SEEDS = {"composition_seed": 11, "archetype_seed": 22, "perturbation_seed": 33}
PG = os.environ.get("MICROGRID_TEST_DATABASE_URL")


# --- fixtures ------------------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def full_data_root(synthetic_dataset, tmp_path_factory) -> Path:
    """Time series plus characteristics under the server's default layout, so the worker can really execute."""
    root = tmp_path_factory.mktemp("qa_w1_data") / "data"
    shutil.copytree(synthetic_dataset.root, root)
    shutil.copy(synthetic_dataset.resstock_chars, root / "background" / DataConfig().resstock_chars_file)
    shutil.copy(synthetic_dataset.comstock_chars, root / "background" / DataConfig().comstock_chars_file)
    return root


@pytest.fixture
def qa_settings(tmp_path, synthetic_dataset, tiny_config, full_data_root, monkeypatch):
    from microgrid_api import worker

    s = make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=full_data_root)
    monkeypatch.setattr(worker, "_settings_override", s)
    return s


def _client(settings) -> TestClient:
    app = create_app(settings)
    app.state.enqueued = []
    app.state.enqueue = lambda run_id: app.state.enqueued.append(str(run_id))
    return TestClient(app)


@pytest.fixture
def api(qa_settings):
    with _client(qa_settings) as c:
        yield c


def _post_run(api, body, who="admin"):
    return api.post("/api/v1/runs", headers=bearer(who), json=body)


def _execute(api, rid: str) -> dict[str, Any]:
    from microgrid_api import worker

    worker.execute_run(rid, api.app.state.session_factory)
    return api.get(f"/api/v1/runs/{rid}", headers=bearer("admin")).json()


def _packaged_doc() -> dict[str, Any]:
    return ea_svc.packaged_document()


def _without_run_dir(doc: Any) -> Any:
    """Drop values that legitimately differ between two runs of the same inputs (run id, per-run folder)."""
    if isinstance(doc, dict):
        return {
            k: _without_run_dir(v)
            for k, v in doc.items()
            if k not in ("base_dir", "id", "created_at", "started_at", "finished_at")
        }
    if isinstance(doc, list):
        return [_without_run_dir(v) for v in doc]
    return doc


# --- (e) pool size with EVERY run, visible in detail and in the results document -----------------------------------


def test_unfiltered_run_stores_pool_size_in_detail_and_results_document(api, tiny_config, full_data_root):
    r = _post_run(api, {"config": client_config(tiny_config), "seeds": SEEDS}, who="alice")
    assert r.status_code == 202, r.text
    rid = r.json()["id"]
    from microgrid_api import worker

    worker.execute_run(rid, api.app.state.session_factory)
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("alice")).json()
    assert detail["status"] == "succeeded", detail["error_message"]
    assert detail["filter_selections"] is None
    assert detail["effective_filters"] == {}
    pools = detail["pool_summary"]
    assert set(pools) == {"resstock/mf_small", "resstock/mf_mid"}
    assert all(isinstance(p["matched"], int) and p["matched"] > 0 for p in pools.values())
    results = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice"))
    assert results.status_code == 200
    assert results.json()["pool_summary"] == pools


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT: GET /runs/{id}/results serves results.json verbatim; its config_snapshot carries the server data "
    "paths and output.base_dir that run detail now strips (pre-existing leak, made visible by FR-ENG-11 e).",
)
def test_results_document_never_contains_server_paths(api, tiny_config, full_data_root, qa_settings):
    rid = _post_run(api, {"config": client_config(tiny_config), "seeds": SEEDS}, who="alice").json()["id"]
    from microgrid_api import worker

    worker.execute_run(rid, api.app.state.session_factory)
    r = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice"))
    assert r.status_code == 200
    assert str(full_data_root.resolve()) not in r.text
    assert str(qa_settings.output_dir.resolve()) not in r.text


def test_legacy_run_without_pool_summary_or_selections_reads_cleanly(api, tiny_config):
    rid = _post_run(api, {"config": client_config(tiny_config)}).json()["id"]
    with api.app.state.session_factory() as s:
        row = s.get(Run, uuid.UUID(rid))
        row.result = {"engine_version": "0.0.1", "seeds": SEEDS}  # a result written before pool_summary existed
        row.filter_selections = None
        s.commit()
    body = api.get(f"/api/v1/runs/{rid}", headers=bearer("admin")).json()
    assert body["pool_summary"] is None and body["filter_selections"] is None and body["effective_filters"] == {}


# --- (f) an empty selection changes nothing: bit-identical to no field --------------------------------------------


@pytest.mark.parametrize("who", ["admin", "alice"])
def test_empty_selections_are_bit_identical_to_no_filter_field(api, tiny_config, who):
    variants: list[dict[str, Any]] = [{}, {"filter_selections": None}, {"filter_selections": {}}]
    variants.append({"filter_selections": {"mf_small": []}})
    variants.append({"filter_selections": {"mf_small": [], "mf_mid": []}})
    rids = []
    for extra in variants:
        r = _post_run(api, {"config": client_config(tiny_config), "seeds": SEEDS, **extra}, who=who)
        assert r.status_code == 202, (extra, r.text)  # an empty list is not "using filters", even for a client
        rids.append(r.json()["id"])
    with api.app.state.session_factory() as s:
        rows = [s.get(Run, uuid.UUID(rid)) for rid in rids]
        snaps = [_without_run_dir(row.composition_snapshot) for row in rows]
        assert all(sn == snaps[0] for sn in snaps), "an empty selection changed the stored snapshot"
        assert all(row.seeds == rows[0].seeds for row in rows)
        assert all(row.filter_selections is None for row in rows)
    first = _without_run_dir(_execute(api, rids[0]))
    for rid in rids[1:3]:
        assert _without_run_dir(_execute(api, rid)) == first
    full = [
        json.loads((Path(r.composition_snapshot["output"]["base_dir"]) / "results.json").read_text()) for r in rows[:3]
    ]
    assert _without_run_dir(full[1]) == _without_run_dir(full[0])
    assert _without_run_dir(full[2]) == _without_run_dir(full[0])


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (FR-ENG-3): a filter_selections key naming a category that does not exist is silently ignored when "
    "its list is empty; with a non-empty list the same key is a 422.",
)
def test_unknown_category_with_empty_selection_list_is_an_error(api, tiny_config):
    r = _post_run(api, {"config": client_config(tiny_config), "filter_selections": {"no_such_category": []}})
    assert r.status_code == 422


def test_selection_only_narrows_the_pool_and_keeps_every_other_setting(api, tiny_config):
    sel = {"mf_small": [{"attribute": "vintage", "op": "in", "values": ["1960s", "1980s"]}]}
    a = _post_run(api, {"config": client_config(tiny_config), "seeds": SEEDS}).json()["id"]
    b = _post_run(api, {"config": client_config(tiny_config), "seeds": SEEDS, "filter_selections": sel}).json()["id"]
    with api.app.state.session_factory() as s:
        sa, sb = (copy.deepcopy(s.get(Run, uuid.UUID(x)).composition_snapshot) for x in (a, b))
    del sb["archetype_filters"]["resstock"]["mf_small"]["attributes"]
    assert _without_run_dir(sa) == _without_run_dir(sb)


# --- (b) predefined compositions carrying admin filters stay runnable by others ------------------------------------


def _filtered_composition(tiny_config) -> dict[str, Any]:
    cfg = client_config(tiny_config)
    cfg["archetype_filters"]["resstock"]["mf_small"]["attributes"] = [
        {"column": "in.vintage", "op": "in", "values": ["1960s", "1980s", "2000s", "2010s"]}
    ]
    return cfg


def test_predefined_filtered_composition_is_runnable_by_client_and_demo_with_the_same_pool(api, tiny_config):
    """The run path itself honours 'permission on edit, not on run' (library loader bypassed, see next test)."""
    api.app.state.demo_compositions = {"filtered": _filtered_composition(tiny_config)}
    ids = {}
    for who in ("admin", "alice", "demo"):
        r = _post_run(api, {"composition_id": "filtered"}, who=who)
        assert r.status_code == 202, (who, r.text)
        ids[who] = r.json()["id"]
    with api.app.state.session_factory() as s:
        filters = {
            who: s.get(Run, uuid.UUID(rid)).composition_snapshot["archetype_filters"]["resstock"]["mf_small"]
            for who, rid in ids.items()
        }
    assert filters["alice"] == filters["admin"] == filters["demo"]
    detail = api.get(f"/api/v1/runs/{ids['alice']}", headers=bearer("alice")).json()
    assert detail["effective_filters"]["resstock/mf_small"][0]["column"] == "in.vintage"
    # a client may not change or drop the stored filters through selections
    sel = {"mf_small": [{"attribute": "vintage", "op": "in", "values": ["1960s"]}]}
    assert _post_run(api, {"composition_id": "filtered", "filter_selections": sel}, who="alice").status_code == 403


@pytest.mark.xfail(
    strict=True,
    reason="GAP FR-ENG-11 b: the only predefined-composition library refuses any composition with attribute filters "
    "at startup (validate_filters_allowed for the demo role), and there is no API to save a composition, so a "
    "composition created by an Admin with filters cannot be offered to Clients/Demo at all.",
)
def test_admin_filtered_composition_can_be_offered_as_predefined(tmp_path, synthetic_dataset, tiny_config):
    import yaml

    path = tmp_path / "library.yaml"
    path.write_text(yaml.safe_dump({"compositions": {"filtered": _filtered_composition(tiny_config)}}))
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, demo_compositions_file=path)
    with _client(s) as c:
        assert c.get("/api/v1/demo-compositions", headers=bearer("alice")).json()["items"] == ["filtered"]


# --- (c)/(d) preview and the worker agree; zero pools and missing columns ------------------------------------------


@pytest.mark.parametrize(
    "selections",
    [
        None,
        {"mf_small": [{"attribute": "vintage", "op": "in", "values": ["1960s", "1980s"]}]},
        {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]},
        {
            "mf_small": [
                {"attribute": "heating_fuel", "op": "equals", "value": "Electricity"},
                {"attribute": "vintage", "op": "in", "values": ["2000s", "2010s"]},
            ],
            "mf_mid": [{"attribute": "has_heat_pump", "op": "is_true"}],
        },
        {"mf_small": [{"attribute": "has_ev", "op": "is_false"}]},
        {"mf_mid": [{"attribute": "stories", "op": "range", "min": 150, "max": 200}]},  # zero
    ],
)
def test_preview_counts_equal_the_pool_the_worker_samples_from(api, tiny_config, selections):
    body: dict[str, Any] = {"config": client_config(tiny_config)}
    if selections is not None:
        body["filter_selections"] = selections
    pv = api.post(f"{BASE}/preview", headers=bearer("admin"), json=body)
    assert pv.status_code == 200, pv.text
    r = _post_run(api, body)
    assert r.status_code == 202, r.text
    with api.app.state.session_factory() as s:
        snap = s.get(Run, uuid.UUID(r.json()["id"])).composition_snapshot
    cfg = config_from_dict(copy.deepcopy(snap), source="<qa>")
    bg = Path(cfg.data.background_dir)
    pool = build_archetype_pool(bg / cfg.data.resstock_chars_file, bg / cfg.data.comstock_chars_file, cfg)
    sampled = {f"resstock/{k}": len(v) for k, v in pool.residential.items()}
    sampled |= {f"comstock/{k}": len(v) for k, v in pool.commercial.items()}
    assert {k: v["matched"] for k, v in pv.json()["categories"].items()} == sampled


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (FR-ENG-11 c/d): preview warns 'no buildings match', but POST /runs accepts the same selection and "
    "the worker then fails with only 'engine run failed (ValueError); see worker logs'. Refuse at creation (422) or "
    "store a clear, path-free reason.",
)
def test_zero_pool_selection_is_refused_or_explained_on_the_run_path(api, tiny_config):
    sel = {"mf_small": [{"attribute": "vintage", "op": "equals", "value": "<1940"}]}
    sel["mf_small"].append({"attribute": "stories", "op": "range", "min": 150, "max": 200})
    body = {"config": client_config(tiny_config), "filter_selections": sel}
    pv = api.post(f"{BASE}/preview", headers=bearer("admin"), json=body)
    assert pv.status_code == 200, pv.text
    assert pv.json()["categories"]["resstock/mf_small"]["matched"] == 0
    assert any("no buildings match" in w for w in pv.json()["categories"]["resstock/mf_small"]["warnings"])
    r = _post_run(api, {**body, "seeds": SEEDS})
    if r.status_code == 422:
        assert "mf_small" in r.json()["detail"]
        return
    detail = _execute(api, r.json()["id"])
    assert detail["status"] == "failed"
    assert "mf_small" in (detail["error_message"] or "") or "no buildings" in (detail["error_message"] or "")


def _activate_doc(api, doc) -> None:
    latest = api.get(f"{BASE}/versions", headers=bearer("admin")).json()["latest_version"]
    r = api.post(
        f"{BASE}/versions", headers=bearer("admin"), json={"base_version": latest, "document": doc, "activate": True}
    )
    assert r.status_code == 201, r.text


def _doc_with_missing_column() -> dict[str, Any]:
    doc = _packaged_doc()
    for a in doc["attributes"]:
        if a["key"] == "stories":
            a["columns"]["resstock"] = "in.not_in_the_data"
    return doc


def test_attribute_column_missing_from_data_is_a_clear_422_in_preview(api, tiny_config):
    _activate_doc(api, _doc_with_missing_column())
    body = {
        "config": client_config(tiny_config),
        "filter_selections": {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]},
    }
    r = api.post(f"{BASE}/preview", headers=bearer("admin"), json=body)
    assert r.status_code == 422
    assert "in.not_in_the_data" in r.json()["detail"] and "not in the characteristics data" in r.json()["detail"]


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (FR-ENG-11 d): an active attribute whose column is not in the data is a clear 422 in preview, but "
    "POST /runs accepts it and the run fails later with only the exception class name.",
)
def test_attribute_column_missing_from_data_is_clear_on_the_run_path(api, tiny_config):
    _activate_doc(api, _doc_with_missing_column())
    body = {
        "config": client_config(tiny_config),
        "filter_selections": {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]},
        "seeds": SEEDS,
    }
    r = _post_run(api, body)
    if r.status_code == 422:
        return
    detail = _execute(api, r.json()["id"])
    assert "not in the characteristics data" in (detail["error_message"] or "")


# --- (b) versioning: invalid active version, recovery, concurrency -------------------------------------------------


def test_invalid_active_version_blocks_only_filter_use_and_admin_can_recover(api, tiny_config):
    bad = _packaged_doc()
    bad["attributes"][0]["building_types"] = ["retired_type"]  # e.g. the registry dropped a type in a library upgrade
    with api.app.state.session_factory() as s:
        s.add(ExposedAttributeVersion(version=1, document=bad, based_on=0, created_by="admin"))
        s.flush()
        s.add(ExposedAttributeActivation(version=1, activated_by="admin"))
        s.commit()
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]}
    cfg = client_config(tiny_config)
    assert _post_run(api, {"config": cfg, "filter_selections": sel}).status_code == 503
    assert api.post(f"{BASE}/preview", headers=bearer("admin"), json={"config": cfg}).status_code == 503
    assert _post_run(api, {"config": cfg}, who="alice").status_code == 202  # unfiltered runs are unaffected
    listing = api.get(f"{BASE}/versions", headers=bearer("admin"))
    assert listing.status_code == 200 and listing.json()["active_version"] == 1
    assert api.get(f"{BASE}/versions/1", headers=bearer("admin")).status_code == 200  # admin can inspect it
    # creating a fixed version from the latest base and activating it recovers
    r = api.post(
        f"{BASE}/versions",
        headers=bearer("admin"),
        json={"base_version": 1, "document": _packaged_doc(), "activate": True},
    )
    assert r.status_code == 201, r.text
    assert _post_run(api, {"config": cfg, "filter_selections": sel}).status_code == 202
    # and falling back to the packaged set also works
    assert api.post(f"{BASE}/versions/0/activate", headers=bearer("admin")).status_code == 200
    assert api.get(f"{BASE}/active", headers=bearer("admin")).json()["version"] == 0


def test_run_records_the_configuration_version_it_used(api, tiny_config):
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]}
    _activate_doc(api, _packaged_doc())
    rid = _post_run(api, {"config": client_config(tiny_config), "filter_selections": sel}).json()["id"]
    assert api.post(f"{BASE}/versions/0/activate", headers=bearer("admin")).status_code == 200
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("admin")).json()
    assert detail["filter_selections"]["exposed_attributes_version"] == 1  # unchanged by a later activation


def _race_creates(factory, n: int) -> list[str]:
    barrier = threading.Barrier(n)
    outcomes: list[str] = []

    def writer(i: int) -> None:
        with factory() as s:
            barrier.wait()
            try:
                ea_svc.create_version(s, _packaged_doc(), base_version=0, subject=f"a{i}", note=None, activate=True)
                outcomes.append("ok")
            except ea_svc.VersionConflict:
                outcomes.append("conflict")
            except Exception as exc:  # anything else would surface to the client as a 500
                outcomes.append(type(exc).__name__)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return outcomes


def test_concurrent_creates_on_sqlite_threads(tmp_path):
    url = f"sqlite:///{tmp_path / 'race.db'}"
    command.upgrade(_cfg(url), "head")
    engine = create_engine(url)
    factory = sessionmaker(engine, expire_on_commit=False)
    try:
        outcomes = _race_creates(factory, 6)
        assert sorted(outcomes) == ["conflict"] * 5 + ["ok"], outcomes
        with factory() as s:
            assert s.query(ExposedAttributeVersion).count() == 1
            assert s.query(ExposedAttributeActivation).count() == 1  # the losers' activations were rolled back
    finally:
        engine.dispose()


def test_activation_of_an_uncommitted_version_is_404_not_a_dangling_row(api):
    # a version that does not (yet) exist cannot be activated; nothing is written
    assert api.post(f"{BASE}/versions/1/activate", headers=bearer("admin")).status_code == 404
    with api.app.state.session_factory() as s:
        assert s.query(ExposedAttributeActivation).count() == 0


# --- migration 0004 on a database that already holds runs ----------------------------------------------------------


def _seed_0003_runs(url: str) -> list[str]:
    engine = create_engine(url)
    ids = [str(uuid.uuid4()) for _ in range(3)]
    raw_filters = {"resstock": {"mf_small": {"attributes": [{"column": "in.vintage", "op": "not_null"}]}}}
    rows = [
        (ids[0], "succeeded", "alice", {"composition": {"total_buildings": 4}}, {"pool_summary": {"x": 1}}),
        (ids[1], "queued", None, {"archetype_filters": raw_filters}, None),
        (ids[2], "failed", "bob", {}, None),
    ]
    with engine.begin() as conn:
        for rid, status, owner, snap, result in rows:
            conn.execute(
                text(
                    "INSERT INTO runs (id, status, owner, composition_snapshot, seeds, engine_version, progress, "
                    "cancel_requested, result) VALUES (:id, :status, :owner, :snap, :seeds, '0.1.0', :progress, "
                    ":cancel, :result)"
                ),
                {
                    "id": rid if url.startswith("postgresql") else uuid.UUID(rid).hex,
                    "status": status,
                    "owner": owner,
                    "snap": json.dumps(snap),
                    "seeds": json.dumps(SEEDS),
                    "progress": json.dumps({}),
                    "cancel": False,
                    "result": None if result is None else json.dumps(result),
                },
            )
    engine.dispose()
    return ids


def _check_upgrade_with_runs(url: str) -> None:
    cfg = _cfg(url)
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "0003")
    ids = _seed_0003_runs(url)
    command.upgrade(cfg, "head")
    engine = create_engine(url)
    factory = sessionmaker(engine, expire_on_commit=False)
    try:
        with factory() as s:
            runs = {str(r.id): r for r in s.query(Run).all()}
            assert set(runs) == set(ids)
            assert all(r.filter_selections is None for r in runs.values())
            assert runs[ids[0]].owner == "alice" and runs[ids[0]].result == {"pool_summary": {"x": 1}}
            assert ea_svc.effective_filters(runs[ids[1]].composition_snapshot) == {
                "resstock/mf_small": [{"column": "in.vintage", "op": "not_null"}]
            }
            runs[ids[2]].filter_selections = {"exposed_attributes_version": 0, "categories": {}}
            s.commit()
        # downgrade drops only the new column/tables: every run and its other fields survive
        command.downgrade(cfg, "0003")
        with engine.connect() as conn:
            n = conn.execute(text("SELECT count(*) FROM runs")).scalar()
            owners = {row[0] for row in conn.execute(text("SELECT owner FROM runs"))}
        assert n == 3 and owners == {"alice", None, "bob"}
        command.upgrade(cfg, "head")
        with factory() as s:
            assert {str(r.id) for r in s.query(Run).all()} == set(ids)
    finally:
        engine.dispose()
        command.downgrade(cfg, "base")


def test_0004_upgrade_keeps_existing_runs_readable_sqlite(tmp_path):
    _check_upgrade_with_runs(f"sqlite:///{tmp_path / 'with_runs.db'}")


@pytest.mark.integration
@pytest.mark.skipif(not PG, reason="set MICROGRID_TEST_DATABASE_URL to run Postgres tests")
def test_0004_upgrade_keeps_existing_runs_readable_postgres():
    assert PG
    _check_upgrade_with_runs(PG)


@pytest.mark.integration
@pytest.mark.skipif(not PG, reason="set MICROGRID_TEST_DATABASE_URL to run Postgres tests")
def test_activation_fk_refuses_deleting_a_version_postgres():
    """Versions are append-only; the activation log's RESTRICT foreign key stops a referenced version being removed."""
    assert PG
    cfg = _cfg(PG)
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")
    engine = create_engine(PG)
    factory = sessionmaker(engine, expire_on_commit=False)
    try:
        with factory() as s:
            ea_svc.create_version(s, _packaged_doc(), base_version=0, subject="admin", note=None, activate=True)
        with pytest.raises(Exception, match="(?i)foreign key|violates"), engine.begin() as conn:
            conn.execute(text("DELETE FROM exposed_attribute_versions WHERE version = 1"))
    finally:
        engine.dispose()
        command.downgrade(cfg, "base")


# --- global middleware / validation handler: existing endpoints keep their behaviour -------------------------------


def test_existing_endpoint_422_keeps_loc_msg_type_and_never_500s(api, tiny_config):
    # model_validator ValueError (ctx holds an exception object) must still render as JSON
    r = _post_run(api, {"config": client_config(tiny_config), "composition_id": "small-mixed"})
    assert r.status_code == 422, r.text
    for e in r.json()["detail"]:
        assert {"loc", "msg", "type"} <= set(e) and "input" not in e
    # unknown field on RunCreate (extra=forbid) and bad query parameter on an existing GET
    r = _post_run(api, {"config": client_config(tiny_config), "surprise": 1})
    assert r.status_code == 422 and r.json()["detail"][0]["loc"][-1] == "surprise"
    r = api.get("/api/v1/runs?limit=0", headers=bearer("admin"))
    assert r.status_code == 422 and r.json()["detail"][0]["loc"] == ["query", "limit"]
    r = api.get("/api/v1/runs/not-a-uuid", headers=bearer("admin"))
    assert r.status_code == 422
    # HTTPException bodies are unchanged (plain string detail)
    r = _post_run(api, {"composition_id": "nope"})
    assert r.status_code == 404 and r.json() == {"detail": "unknown composition_id"}


def test_body_cap_boundary_and_bodiless_endpoints(tmp_path, synthetic_dataset, tiny_config, full_data_root):
    cap = 2048
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=full_data_root, max_request_bytes=cap)
    with _client(s) as c:
        payload = json.dumps({"base_version": 0, "document": {"version": 1, "attributes": []}, "note": ""})
        exact = payload.replace('"note": ""', '"note": "' + "n" * (cap - len(payload)) + '"')
        assert len(exact.encode()) == cap
        h = {**bearer("admin"), "Content-Type": "application/json"}
        r = c.post(f"{BASE}/versions", headers=h, content=exact)
        assert r.status_code != 413  # exactly at the cap is allowed (422 here: note too long)
        r = c.post(f"{BASE}/versions", headers=h, content=exact + " ")
        assert r.status_code == 413
        # health and GET endpoints are unaffected
        assert c.get("/api/v1/health").status_code == 200
        assert c.get("/api/v1/runs", headers=bearer("admin")).status_code == 200
        # a GET that declares an oversized body is refused before routing
        r = c.request("GET", "/api/v1/runs", headers={**bearer("admin")}, content=b"x" * (cap + 1))
        assert r.status_code == 413


def test_sse_stream_still_works_behind_the_body_cap(api, tiny_config):
    rid = _post_run(api, {"config": client_config(tiny_config), "seeds": SEEDS}).json()["id"]
    from microgrid_api import worker

    worker.execute_run(rid, api.app.state.session_factory)
    with api.stream("GET", f"/api/v1/runs/{rid}/events", headers=bearer("admin")) as r:
        assert r.status_code == 200
        body = "".join(r.iter_text())
    assert "building_pool" in body and "finished" in body


# --- mutation survivors: limits enforced at the schema boundary (before any library work) -------------------------


def _schema_422(r) -> list[dict[str, Any]]:
    """A 422 produced by the request schema (a list of located errors), not by later library validation."""
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert isinstance(detail, list), f"rejected only after library work: {detail!r}"
    return detail


def test_selection_column_field_rejected_by_schema(api, tiny_config):
    sel = {"mf_mid": [{"attribute": "stories", "column": "in.geometry_stories", "op": "not_null"}]}
    detail = _schema_422(
        api.post(
            f"{BASE}/preview",
            headers=bearer("admin"),
            json={"config": client_config(tiny_config), "filter_selections": sel},
        )
    )
    assert any(e["loc"][-1] == "column" for e in detail)


def test_category_and_selection_counts_capped_by_schema(api, tiny_config):
    cfg = client_config(tiny_config)
    many_cats = {f"c{i}": [] for i in range(51)}
    _schema_422(
        api.post(f"{BASE}/preview", headers=bearer("admin"), json={"config": cfg, "filter_selections": many_cats})
    )
    many_sels = {"mf_mid": [{"attribute": "stories", "op": "equals", "value": 4}] * 51}
    _schema_422(
        api.post(f"{BASE}/preview", headers=bearer("admin"), json={"config": cfg, "filter_selections": many_sels})
    )
    _schema_422(_post_run(api, {"config": cfg, "filter_selections": many_sels}))
    bad_name = {"../etc": [{"attribute": "stories", "op": "equals", "value": 4}]}
    _schema_422(
        api.post(f"{BASE}/preview", headers=bearer("admin"), json={"config": cfg, "filter_selections": bad_name})
    )


def test_document_operator_list_capped(api):
    doc = _packaged_doc()
    doc["attributes"][0]["operators"] = ["equals", "range"] * 4  # 8 entries
    r = api.post(f"{BASE}/versions", headers=bearer("admin"), json={"base_version": 0, "document": doc})
    _schema_422(r)
    doc = _packaged_doc()
    doc["attributes"][0]["building_types"] = ["mf small!"]
    _schema_422(api.post(f"{BASE}/versions", headers=bearer("admin"), json={"base_version": 0, "document": doc}))


def test_version_list_limit_capped(api):
    assert api.get(f"{BASE}/versions?limit=201", headers=bearer("admin")).status_code == 422
    assert api.get(f"{BASE}/versions?limit=200", headers=bearer("admin")).status_code == 200


def test_inactive_attribute_is_not_offered(api):
    doc = _packaged_doc()
    for a in doc["attributes"]:
        if a["key"] == "vintage":
            a["active"] = False
    _activate_doc(api, doc)
    keys = {a["key"] for a in api.get(f"{BASE}/active", headers=bearer("admin")).json()["attributes"]}
    assert "vintage" not in keys and "stories" in keys


def test_empty_raw_attribute_list_is_not_an_effective_filter(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["archetype_filters"]["resstock"]["mf_small"]["attributes"] = []
    rid = _post_run(api, {"config": cfg}).json()["id"]
    assert api.get(f"/api/v1/runs/{rid}", headers=bearer("admin")).json()["effective_filters"] == {}


def test_characteristics_outside_the_data_root_are_refused(tmp_path, synthetic_dataset):
    outside = tmp_path / "outside"
    outside.mkdir()
    shutil.copy(synthetic_dataset.resstock_chars, outside / "chars.xlsx")
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(ea_svc.DataUnavailable) as err:
        ea_svc._characteristics(root, str(outside), "chars.xlsx", "resstock")
    assert str(outside) not in str(err.value)


def test_type_specific_attribute_refused_for_a_registered_type_it_does_not_apply_to(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["composition"]["categories"]["sf_detached"] = {"min_fraction": 0.0, "max_fraction": 0.5, "source": "resstock"}
    cfg["archetype_filters"]["resstock"]["sf_detached"] = {"building_type_heights": ["Single-Family Detached"]}
    sel = {"sf_detached": [{"attribute": "dwelling_units", "op": "range", "min": 2, "max": 4}]}
    r = api.post(f"{BASE}/preview", headers=bearer("admin"), json={"config": cfg, "filter_selections": sel})
    assert r.status_code == 422, r.text
    r = _post_run(api, {"config": cfg, "filter_selections": sel})
    assert r.status_code == 422, r.text


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (LOW): apply_selections passes the building type only when the category NAME is a registry key; a "
    "category named freely (e.g. 'single_family_detached' in configs/scenarios/dc_multifamily_baseline_2.yaml) gets "
    "building_type=None, so a multifamily-only attribute is accepted for a single-family pool.",
)
def test_type_specific_attribute_refused_for_an_unregistered_category_name(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["composition"]["categories"]["single_family_detached"] = {
        "min_fraction": 0.0,
        "max_fraction": 0.5,
        "source": "resstock",
    }
    cfg["archetype_filters"]["resstock"]["single_family_detached"] = {
        "building_type_heights": ["Single-Family Detached"]
    }
    sel = {"single_family_detached": [{"attribute": "dwelling_units", "op": "range", "min": 2, "max": 4}]}
    r = _post_run(api, {"config": cfg, "filter_selections": sel})
    assert r.status_code == 422, r.text
