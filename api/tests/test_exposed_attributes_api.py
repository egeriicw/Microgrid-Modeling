"""Story W1-FILTERS (FR-ENG-11 b-e): DB-stored, versioned exposed-attribute configuration; admin edit endpoints;
pool preview; filter selections on runs (only through ExposedAttributes.to_filters); size and shape limits."""

from __future__ import annotations

import copy
import json
import os
import shutil
import threading
import uuid
from typing import Any

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from microgrid.community_load.exposed_attributes import default_exposed_attributes
from microgrid.community_load.synthetic.config import DataConfig
from microgrid.community_load.synthetic.strict_yaml import load_yaml_strict
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from microgrid_api.config import ConfigurationError
from microgrid_api.db.models import ExposedAttributeActivation, ExposedAttributeVersion, Run
from microgrid_api.main import create_app
from microgrid_api.services import exposed_attributes as ea_svc

from .http_support import KEYS, bearer, client_config, make_settings

BASE = "/api/v1/exposed-attributes"
WRONG = {"Authorization": "Bearer wrong-key-0123456789abcdef"}


def _packaged_document() -> dict[str, Any]:
    from importlib import resources

    text = resources.files("microgrid.community_load").joinpath("exposed_attributes.yaml").read_text()
    return load_yaml_strict(text)


@pytest.fixture(scope="session")
def filter_data_root(synthetic_dataset, tmp_path_factory):
    """A data root laid out like the server expects (characteristics under the engine's default file names)."""
    root = tmp_path_factory.mktemp("filter_data")
    bg = root / "background"
    bg.mkdir()
    shutil.copy(synthetic_dataset.resstock_chars, bg / DataConfig().resstock_chars_file)
    shutil.copy(synthetic_dataset.comstock_chars, bg / DataConfig().comstock_chars_file)
    return root


@pytest.fixture
def fa_settings(tmp_path, synthetic_dataset, tiny_config, filter_data_root):
    return make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=filter_data_root)


def _client_for(settings) -> TestClient:
    app = create_app(settings)
    app.state.enqueued = []
    app.state.enqueue = lambda run_id: app.state.enqueued.append(str(run_id))
    return TestClient(app)


@pytest.fixture
def api(fa_settings):
    with _client_for(fa_settings) as c:
        yield c


@pytest.fixture
def res_chars(synthetic_dataset) -> pd.DataFrame:
    return pd.read_excel(synthetic_dataset.resstock_chars, sheet_name="building_characteristics")


def _doc(**changes: Any) -> dict[str, Any]:
    doc = copy.deepcopy(_packaged_document())
    for key, value in changes.items():
        doc[key] = value
    return doc


def _create(api, doc=None, base=0, who="admin", **extra):
    body = {"base_version": base, "document": doc if doc is not None else _doc(), **extra}
    return api.post(f"{BASE}/versions", headers=bearer(who), json=body)


def _preview_body(tiny_config, selections=None, **cfg_over):
    cfg = client_config(tiny_config)
    cfg.update(cfg_over)
    body: dict[str, Any] = {"config": cfg}
    if selections is not None:
        body["filter_selections"] = selections
    return body


def _ok_mask(df: pd.DataFrame, heights: list[str]) -> pd.Series:
    status = df["completed_status"].astype(str).str.strip().str.lower() == "success"
    applicable = df["applicability"].astype(str).str.strip().str.lower() == "true"
    return df["in.geometry_building_type_height"].isin(heights) & status & applicable


MF_SMALL = ["Multi-Family with 2 - 4 Units", "Multi-Family with 5+ Units, 1-3 Stories"]
MF_MID = ["Multi-Family with 5+ Units, 4-7 Stories"]


# --- (b) storage, versions, packaged fallback -----------------------------------------------------------------------


def test_empty_table_means_packaged_starter_set_is_active(api):
    r = api.get(f"{BASE}/active", headers=bearer("admin"))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version"] == 0
    assert body["source"] == "packaged"
    want = {a.key for a in default_exposed_attributes().attributes if a.active}
    assert {a["key"] for a in body["attributes"]} == want
    orders = [a["order"] for a in body["attributes"]]
    assert orders == sorted(orders)


def test_active_view_never_contains_column_names(api):
    r = api.get(f"{BASE}/active", headers=bearer("admin"))
    assert "columns" not in r.text and "in.geometry_stories" not in r.text
    stories = next(a for a in r.json()["attributes"] if a["key"] == "stories")
    assert stories["sources"] == ["resstock", "comstock"]


def test_version_zero_is_the_packaged_document(api):
    r = api.get(f"{BASE}/versions/0", headers=bearer("admin"))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version"] == 0 and body["active"] is True
    parsed = ea_svc.parse_document(body["document"])
    assert parsed == default_exposed_attributes()


def test_create_version_is_inactive_until_activated(api):
    r = _create(api, note="first edit")
    assert r.status_code == 201, r.text
    created = r.json()
    assert created["version"] == 1 and created["based_on"] == 0 and created["active"] is False
    assert created["created_by"] == "admin" and created["note"] == "first edit"
    assert api.get(f"{BASE}/active", headers=bearer("admin")).json()["version"] == 0
    a = api.post(f"{BASE}/versions/1/activate", headers=bearer("admin"))
    assert a.status_code == 200, a.text
    assert a.json()["active"] is True
    active = api.get(f"{BASE}/active", headers=bearer("admin")).json()
    assert active["version"] == 1 and active["source"] == "database"


def test_create_and_activate_in_one_step_and_revert_to_packaged(api):
    doc = _doc()
    doc["attributes"] = [a for a in doc["attributes"] if a["key"] != "has_ev"]
    r = _create(api, doc, activate=True)
    assert r.status_code == 201 and r.json()["active"] is True
    keys = {a["key"] for a in api.get(f"{BASE}/active", headers=bearer("admin")).json()["attributes"]}
    assert "has_ev" not in keys
    assert api.post(f"{BASE}/versions/0/activate", headers=bearer("admin")).status_code == 200
    back = api.get(f"{BASE}/active", headers=bearer("admin")).json()
    assert back["version"] == 0 and "has_ev" in {a["key"] for a in back["attributes"]}


def test_versions_are_immutable_and_listed_with_who_and_when(api):
    assert _create(api).status_code == 201
    assert _create(api, base=1, activate=True).status_code == 201
    r = api.get(f"{BASE}/versions", headers=bearer("admin"))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["latest_version"] == 2 and body["active_version"] == 2 and body["total"] == 2
    assert [v["version"] for v in body["items"]] == [2, 1]
    assert all(v["created_by"] == "admin" and v["created_at"] for v in body["items"])
    assert [v["active"] for v in body["items"]] == [True, False]
    # no endpoint edits or deletes a version
    for method in ("put", "patch", "delete"):
        assert getattr(api, method)(f"{BASE}/versions/1", headers=bearer("admin")).status_code == 405


def test_activations_are_recorded_with_who_and_when(api):
    _create(api)
    api.post(f"{BASE}/versions/1/activate", headers=bearer("admin"))
    with api.app.state.session_factory() as s:
        rows = s.query(ExposedAttributeActivation).all()
        assert [(r.version, r.activated_by) for r in rows] == [(1, "admin")]
        assert rows[0].activated_at is not None


# --- optimistic concurrency ---------------------------------------------------------------------------------------


def test_stale_base_version_is_a_conflict(api):
    assert _create(api, base=0).status_code == 201
    r = _create(api, base=0)
    assert r.status_code == 409
    assert "version 1" in r.json()["detail"]
    assert _create(api, base=7).status_code == 409
    with api.app.state.session_factory() as s:
        assert s.query(ExposedAttributeVersion).count() == 1


def test_concurrent_create_with_same_base_one_wins_one_conflicts(api, monkeypatch):
    """Both writers pass the base check (simulated race); the primary key on version makes the second fail."""
    factory = api.app.state.session_factory
    with factory() as a, factory() as b:
        ea_svc.create_version(a, _doc(), base_version=0, subject="admin", note=None, activate=False)
        monkeypatch.setattr(ea_svc, "latest_version", lambda session: 0)  # b read "latest" before a committed
        with pytest.raises(ea_svc.VersionConflict):
            ea_svc.create_version(b, _doc(), base_version=0, subject="admin2", note=None, activate=False)
    with factory() as s:
        rows = s.query(ExposedAttributeVersion).all()
        assert [(r.version, r.created_by) for r in rows] == [(1, "admin")]


PG = os.environ.get("MICROGRID_TEST_DATABASE_URL")


@pytest.mark.integration
@pytest.mark.skipif(not PG, reason="set MICROGRID_TEST_DATABASE_URL to run Postgres tests")
def test_concurrent_create_on_postgres_threads():
    """Real concurrency: many writers from the same base; exactly one version is stored, all others conflict."""
    from alembic import command

    from .test_migrations import _cfg

    assert PG
    command.downgrade(_cfg(PG), "base")
    command.upgrade(_cfg(PG), "head")
    engine = create_engine(PG)
    factory = sessionmaker(engine, expire_on_commit=False)
    n = 8
    barrier = threading.Barrier(n)
    outcomes: list[str] = []

    def writer(i: int) -> None:
        with factory() as s:
            barrier.wait()
            try:
                ea_svc.create_version(s, _doc(), base_version=0, subject=f"admin{i}", note=None, activate=True)
                outcomes.append("ok")
            except ea_svc.VersionConflict:
                outcomes.append("conflict")

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    try:
        assert sorted(outcomes) == ["conflict"] * (n - 1) + ["ok"]
        with factory() as s:
            assert s.query(ExposedAttributeVersion).count() == 1
            assert s.query(ExposedAttributeActivation).count() == 1
    finally:
        engine.dispose()
        command.downgrade(_cfg(PG), "base")


def test_base_version_must_be_a_non_negative_integer(api):
    for bad in (-1, "1", 1.5, None, True):
        r = api.post(f"{BASE}/versions", headers=bearer("admin"), json={"base_version": bad, "document": _doc()})
        assert r.status_code == 422, bad


# --- (d) validation with the library's validators --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mutate", "fragment"),
    [
        (lambda d: d["attributes"][0].update(operators=["is_true"]), "not valid for numeric"),
        (lambda d: d["attributes"][0].update(building_types=["castle"]), "castle"),
        (lambda d: d["attributes"].append(copy.deepcopy(d["attributes"][0])), "duplicate key"),
        (lambda d: d["attributes"][0].update(min=10, max=1), "exceeds max"),
        (lambda d: d["attributes"][2].update(min=1), "only apply to numeric"),
    ],
)
def test_library_validation_errors_are_clear_422(api, mutate, fragment):
    doc = _doc()
    mutate(doc)
    r = _create(api, doc)
    assert r.status_code == 422, r.text
    assert fragment in json.dumps(r.json())


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(version=2),
        lambda d: d.update(extra="x"),
        lambda d: d["attributes"][0].update(surprise=True),
        lambda d: d["attributes"][0].update(kind="text"),
        lambda d: d["attributes"][0].update(operators=["equals", "regex"]),
        lambda d: d["attributes"][0].update(label="x" * 201),
        lambda d: d["attributes"][0].update(help="x" * 2001),
        lambda d: d["attributes"][0].update(key="Bad Key!"),
        lambda d: d["attributes"][0]["columns"].update(resstock="../../etc/passwd"),
        lambda d: d["attributes"][0]["columns"].update(resstock="/data/background/DC_upgrade0.xlsx"),
        lambda d: d["attributes"][0]["columns"].update(elsewhere="in.x"),
        lambda d: d["attributes"][2].update(choices=["c"] * 501),
        lambda d: d["attributes"][2].update(choices=["x" * 201]),
        lambda d: d["attributes"][0].update(building_types=["mf_small"] * 51),
        lambda d: d["attributes"][0].update(order=10**7),
        lambda d: d["attributes"][0].update(active="yes"),
        lambda d: d["attributes"][0].update(min="1"),
        lambda d: d.update(attributes=[dict(d["attributes"][0], key=f"a{i}") for i in range(101)]),
    ],
)
def test_shape_and_size_limits_on_the_document(api, mutate):
    doc = _doc()
    mutate(doc)
    r = _create(api, doc)
    assert r.status_code == 422, r.text
    with api.app.state.session_factory() as s:
        assert s.query(ExposedAttributeVersion).count() == 0


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity"])
def test_nan_and_infinity_rejected_in_document(api, token):
    doc = _doc()
    doc["attributes"][0]["max"] = "__NUM__"
    content = json.dumps({"base_version": 0, "document": doc}).replace('"__NUM__"', token)
    r = api.post(f"{BASE}/versions", headers={**bearer("admin"), "Content-Type": "application/json"}, content=content)
    assert r.status_code == 422, r.text


def test_note_length_limited(api):
    assert _create(api, note="n" * 501).status_code == 422


# --- request size --------------------------------------------------------------------------------------------------


def test_oversized_body_rejected_with_413(tmp_path, synthetic_dataset, tiny_config, filter_data_root):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=filter_data_root, max_request_bytes=4096)
    with _client_for(s) as c:
        big = _doc(attributes=[dict(_doc()["attributes"][0], help="h" * 1500, key=f"k{i}") for i in range(5)])
        r = c.post(f"{BASE}/versions", headers=bearer("admin"), json={"base_version": 0, "document": big})
        assert r.status_code == 413
        assert "h" * 50 not in r.text
        # chunked upload without a Content-Length is counted too
        payload = json.dumps({"base_version": 0, "document": big}).encode()

        def chunks():
            for i in range(0, len(payload), 1000):
                yield payload[i : i + 1000]

        r = c.post(
            f"{BASE}/versions", headers={**bearer("admin"), "Content-Type": "application/json"}, content=chunks()
        )
        assert r.status_code == 413
        # the cap applies to run creation too
        r = c.post("/api/v1/runs", headers=bearer("admin"), json={"config": {"pad": "x" * 5000}})
        assert r.status_code == 413
        with c.app.state.session_factory() as session:
            assert session.query(ExposedAttributeVersion).count() == 0


def test_bad_content_length_header_rejected(api):
    r = api.post(
        f"{BASE}/versions",
        headers={**bearer("admin"), "Content-Type": "application/json", "Content-Length": "-5"},
        content=b"{}",
    )
    assert r.status_code in (400, 413)


# --- roles: every endpoint x every caller ---------------------------------------------------------------------------


def _endpoints(tiny_config) -> list[tuple[str, str, Any, int]]:
    """(method, path, json body, admin success status). Bodies are valid so only the caller decides the outcome."""
    return [
        ("get", f"{BASE}/active", None, 200),
        ("get", f"{BASE}/versions", None, 200),
        ("get", f"{BASE}/versions/0", None, 200),
        ("post", f"{BASE}/versions", {"base_version": 0, "document": _doc()}, 201),
        ("post", f"{BASE}/versions/0/activate", None, 200),
        ("post", f"{BASE}/preview", _preview_body(tiny_config), 200),
    ]


@pytest.mark.parametrize("who", ["admin", "alice", "demo", None, "wrong"])
def test_role_matrix(api, tiny_config, who):
    headers = {} if who is None else WRONG if who == "wrong" else bearer(who)
    for method, path, body, ok in _endpoints(tiny_config):
        kwargs = {"headers": headers} | ({"json": body} if body is not None else {})
        r = getattr(api, method)(path, **kwargs)
        if who == "admin":
            assert r.status_code == ok, (method, path, r.text)
        elif who in (None, "wrong"):
            assert r.status_code == 401, (method, path)
            assert KEYS["admin"] not in r.text
        else:
            assert r.status_code == 403, (method, path, r.status_code)
    with api.app.state.session_factory() as s:
        assert s.query(ExposedAttributeVersion).count() == (1 if who == "admin" else 0)


def test_client_with_flag_enabled_by_configuration(tmp_path, synthetic_dataset, tiny_config, filter_data_root):
    s = make_settings(
        tmp_path,
        synthetic_dataset,
        tiny_config,
        data_dir=filter_data_root,
        permission_overrides={"client": {"filters.use": True}},
    )
    with _client_for(s) as c:
        r = c.get(f"{BASE}/active", headers=bearer("alice"))
        assert r.status_code == 200 and "columns" not in r.text
        assert c.post(f"{BASE}/preview", headers=bearer("alice"), json=_preview_body(tiny_config)).status_code == 200
        # managing the configuration stays admin-only
        assert c.get(f"{BASE}/versions", headers=bearer("alice")).status_code == 403
        assert _create(c, who="alice").status_code == 403
        assert c.get(f"{BASE}/active", headers=bearer("demo")).status_code == 403


def test_demo_can_never_get_filters_by_configuration(tmp_path, synthetic_dataset, tiny_config):
    with pytest.raises(ConfigurationError, match="(?i)permission"):
        make_settings(tmp_path, synthetic_dataset, tiny_config, permission_overrides={"demo": {"filters.use": True}})
    with pytest.raises(ConfigurationError, match="(?i)permission"):
        make_settings(tmp_path, synthetic_dataset, tiny_config, permission_overrides={"client": {"nope": True}})


def test_version_ids_cannot_be_probed(api):
    _create(api)
    seen = set()
    for v in ("0", "1", "999", "-1", "abc", str(2**40)):
        r = api.get(f"{BASE}/versions/{v}", headers=bearer("alice"))
        a = api.post(f"{BASE}/versions/{v}/activate", headers=bearer("alice"))
        seen.add((r.status_code, r.text, a.status_code, a.text))
    assert len(seen) == 1 and next(iter(seen))[0] == 403
    assert api.get(f"{BASE}/versions/999", headers=bearer("admin")).status_code == 404
    assert api.post(f"{BASE}/versions/999/activate", headers=bearer("admin")).status_code == 404
    assert api.get(f"{BASE}/versions/-1", headers=bearer("admin")).status_code == 422
    assert api.get(f"{BASE}/versions/{2**40}", headers=bearer("admin")).status_code == 422


def test_stored_document_that_no_longer_validates_fails_loudly(api):
    bad = _doc()
    bad["attributes"][0]["building_types"] = ["retired_type"]
    with api.app.state.session_factory() as s:
        s.add(ExposedAttributeVersion(version=1, document=bad, based_on=0, created_by="admin"))
        s.commit()
    r = api.post(f"{BASE}/versions/1/activate", headers=bearer("admin"))
    assert r.status_code == 422 and "retired_type" in r.text
    with api.app.state.session_factory() as s:
        s.add(ExposedAttributeActivation(version=1, activated_by="admin"))
        s.commit()
    r = api.get(f"{BASE}/active", headers=bearer("admin"))
    assert r.status_code == 503
    assert "Traceback" not in r.text and "version 1" in r.text


# --- (c) pool preview ----------------------------------------------------------------------------------------------


def test_preview_counts_match_the_data(api, tiny_config, res_chars):
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]}
    r = api.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, sel))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["exposed_attributes_version"] == 0
    assert body["min_pool_size"] == 10
    mid = _ok_mask(res_chars, MF_MID) & res_chars["in.geometry_stories"].between(4, 5)
    small = _ok_mask(res_chars, MF_SMALL)
    assert body["categories"]["resstock/mf_mid"]["matched"] == int(mid.sum())
    assert body["categories"]["resstock/mf_small"]["matched"] == int(small.sum())
    assert body["categories"]["resstock/mf_small"]["total"] == len(res_chars)


def test_preview_warns_on_zero_and_below_minimum(api, tiny_config, res_chars):
    zero = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 150, "max": 200}]}
    r = api.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, zero))
    assert r.status_code == 200, r.text
    mid = r.json()["categories"]["resstock/mf_mid"]
    assert mid["matched"] == 0 and any("no buildings match" in w for w in mid["warnings"])
    n_small = int(_ok_mask(res_chars, MF_SMALL).sum())
    cfg_filters = copy.deepcopy(tiny_config["archetype_filters"])
    cfg_filters["min_pool_size"] = n_small + 1  # configurable minimum
    r = api.post(
        f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, archetype_filters=cfg_filters)
    )
    body = r.json()
    assert body["min_pool_size"] == n_small + 1
    assert any(f"fewer than {n_small + 1}" in w for w in body["categories"]["resstock/mf_small"]["warnings"])


@pytest.mark.parametrize(
    ("selections", "fragment"),
    [
        ({"mf_mid": [{"attribute": "not_an_attribute", "op": "equals", "value": 1}]}, "unknown exposed attribute"),
        ({"mf_mid": [{"attribute": "vintage", "op": "equals", "value": "1850s"}]}, "not one of the allowed choices"),
        ({"mf_mid": [{"attribute": "stories", "op": "range", "min": 0, "max": 3}]}, "below the allowed minimum"),
        ({"mf_mid": [{"attribute": "stories", "op": "regex", "value": "x"}]}, "op"),
        ({"nowhere": [{"attribute": "stories", "op": "equals", "value": 3}]}, "nowhere"),
        ({"mf_mid": [{"attribute": "stories", "op": "equals", "value": 3}] * 2}, "more than once"),
    ],
)
def test_preview_invalid_selection_is_clear_422(api, tiny_config, selections, fragment):
    r = api.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, selections))
    assert r.status_code == 422, r.text
    assert fragment in json.dumps(r.json())


def test_raw_column_names_from_the_client_are_rejected(api, tiny_config):
    raw_sel = {"mf_mid": [{"attribute": "stories", "column": "in.geometry_stories", "op": "not_null"}]}
    r = api.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, raw_sel))
    assert r.status_code == 422
    body = _preview_body(tiny_config)
    body["config"]["archetype_filters"]["resstock"]["mf_mid"]["attributes"] = [
        {"column": "in.geometry_stories", "op": "not_null"}
    ]
    r = api.post(f"{BASE}/preview", headers=bearer("admin"), json=body)
    assert r.status_code == 422 and "filter_selections" in r.json()["detail"]


def test_client_supplied_paths_are_rejected_in_preview(api, tiny_config, synthetic_dataset):
    body = _preview_body(tiny_config)
    body["config"]["data"] = synthetic_dataset.data
    r = api.post(f"{BASE}/preview", headers=bearer("admin"), json=body)
    assert r.status_code == 422 and str(synthetic_dataset.root) not in r.text
    body = _preview_body(tiny_config)
    body["config"]["data_dir"] = "/etc"
    assert api.post(f"{BASE}/preview", headers=bearer("admin"), json=body).status_code == 422
    body = _preview_body(tiny_config)
    body["data_dir"] = "/etc"
    assert api.post(f"{BASE}/preview", headers=bearer("admin"), json=body).status_code == 422


def test_preview_nan_rejected(api, tiny_config):
    body = _preview_body(tiny_config, {"mf_mid": [{"attribute": "stories", "op": "range", "min": "__N__"}]})
    content = json.dumps(body).replace('"__N__"', "NaN")
    r = api.post(f"{BASE}/preview", headers={**bearer("admin"), "Content-Type": "application/json"}, content=content)
    assert r.status_code == 422


def test_preview_selection_limits(api, tiny_config):
    many_values = {"mf_mid": [{"attribute": "vintage", "op": "in", "values": ["1950s"] * 501}]}
    assert (
        api.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, many_values)).status_code
        == 422
    )
    many_cats = {f"c{i}": [] for i in range(51)}
    assert (
        api.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, many_cats)).status_code
        == 422
    )
    long_str = {"mf_mid": [{"attribute": "vintage", "op": "equals", "value": "x" * 201}]}
    assert (
        api.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, long_str)).status_code
        == 422
    )


def test_preview_without_server_data_is_503_without_paths(tmp_path, synthetic_dataset, tiny_config):
    missing = tmp_path / "no-such-data"
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=missing)
    with _client_for(s) as c:
        r = c.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config))
        assert r.status_code == 503
        assert str(missing) not in r.text and "xlsx" not in r.text


def test_preview_uses_the_active_version(api, tiny_config):
    doc = _doc()
    for a in doc["attributes"]:
        if a["key"] == "stories":
            a["active"] = False
    assert _create(api, doc, activate=True).status_code == 201
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]}
    r = api.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, sel))
    assert r.status_code == 422 and "not active" in r.json()["detail"]


# --- (e) runs: selections only through to_filters; effective filters and pool size on the run ---------------------


def _post_run(api, body, who="admin"):
    return api.post("/api/v1/runs", headers=bearer(who), json=body)


def test_run_with_selections_stores_effective_filters_and_provenance(api, tiny_config):
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]}
    r = _post_run(api, {"config": client_config(tiny_config), "filter_selections": sel})
    assert r.status_code == 202, r.text
    rid = r.json()["id"]
    with api.app.state.session_factory() as s:
        row = s.get(Run, uuid.UUID(rid))
        attrs = row.composition_snapshot["archetype_filters"]["resstock"]["mf_mid"]["attributes"]
        assert attrs == [{"column": "in.geometry_stories", "op": "range", "min": 4.0, "max": 5.0}]
        assert row.filter_selections == {"exposed_attributes_version": 0, "categories": sel}
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("admin")).json()
    assert detail["effective_filters"] == {"resstock/mf_mid": attrs}
    assert detail["filter_selections"] == {"exposed_attributes_version": 0, "categories": sel}
    assert detail["pool_summary"] is None  # known once the worker has built the pool


def test_run_detail_shows_pool_summary_and_never_leaks_data_paths(api, tiny_config, filter_data_root):
    rid = _post_run(api, {"config": client_config(tiny_config)}).json()["id"]
    with api.app.state.session_factory() as s:
        row = s.get(Run, uuid.UUID(rid))
        snap = dict(row.composition_snapshot)
        row.result = {
            "pool_summary": {"resstock/mf_mid": {"matched": 7, "total": 40, "warnings": ["only 7"]}},
            "config_snapshot": snap,
        }
        s.commit()
    r = api.get(f"/api/v1/runs/{rid}", headers=bearer("admin"))
    assert r.json()["pool_summary"] == {"resstock/mf_mid": {"matched": 7, "total": 40, "warnings": ["only 7"]}}
    assert r.json()["effective_filters"] == {}
    assert str(filter_data_root) not in r.text
    assert "base_dir" not in r.text


def test_client_selections_forbidden_and_nothing_queued(api, tiny_config):
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]}
    r = _post_run(api, {"config": client_config(tiny_config), "filter_selections": sel}, who="alice")
    assert r.status_code == 403 and "filters.use" in r.json()["detail"]
    r = _post_run(api, {"composition_id": "small-mixed", "filter_selections": sel}, who="demo")
    assert r.status_code == 403
    assert api.app.state.enqueued == []


def test_raw_attribute_filters_rejected_on_runs_even_for_admin(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["archetype_filters"]["resstock"]["mf_small"]["attributes"] = [{"column": "x", "op": "not_null"}]
    r = _post_run(api, {"config": cfg})
    assert r.status_code == 422 and "filter_selections" in r.json()["detail"]
    cfg["archetype_filters"]["resstock"]["mf_small"]["attributes"] = []  # empty list narrows nothing: accepted
    assert _post_run(api, {"config": cfg}).status_code == 202


def test_predefined_composition_with_selections_admin_only(api):
    sel = {"mf_mid": [{"attribute": "stories", "op": "range", "min": 4, "max": 5}]}
    assert _post_run(api, {"composition_id": "small-mixed", "filter_selections": sel}, who="alice").status_code == 403
    r = _post_run(api, {"composition_id": "small-mixed", "filter_selections": sel})
    assert r.status_code == 202, r.text
    assert _post_run(api, {"composition_id": "small-mixed"}, who="alice").status_code == 202


def test_run_selection_errors_are_422(api, tiny_config):
    for sel in (
        {"mf_mid": [{"attribute": "ghost", "op": "not_null"}]},
        {"mf_mid": [{"attribute": "stories", "column": "in.geometry_stories", "op": "not_null"}]},
        {"not_a_category": [{"attribute": "stories", "op": "not_null"}]},
    ):
        r = _post_run(api, {"config": client_config(tiny_config), "filter_selections": sel})
        assert r.status_code == 422, sel
    assert api.app.state.enqueued == []


def test_run_with_selections_executes_and_pool_summary_matches_preview(
    tmp_path, synthetic_dataset, tiny_config, monkeypatch
):
    """End to end on the synthetic fixture: the engine accepts the server-built filters, and the matched pool size
    stored with the run equals the preview for the same selections."""
    from microgrid_api import worker

    root = tmp_path / "data"
    shutil.copytree(synthetic_dataset.root, root)
    shutil.copy(synthetic_dataset.resstock_chars, root / "background" / DataConfig().resstock_chars_file)
    shutil.copy(synthetic_dataset.comstock_chars, root / "background" / DataConfig().comstock_chars_file)
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=root)
    monkeypatch.setattr(worker, "_settings_override", s)
    sel = {"mf_small": [{"attribute": "vintage", "op": "in", "values": ["1960s", "1980s", "2000s", "2010s"]}]}
    with _client_for(s) as c:
        pv = c.post(f"{BASE}/preview", headers=bearer("admin"), json=_preview_body(tiny_config, sel))
        assert pv.status_code == 200, pv.text
        rid = _post_run(c, {"config": client_config(tiny_config), "filter_selections": sel}).json()["id"]
        worker.execute_run(rid, c.app.state.session_factory)
        detail = c.get(f"/api/v1/runs/{rid}", headers=bearer("admin")).json()
    assert detail["status"] == "succeeded", detail["error_message"]
    assert detail["pool_summary"] == pv.json()["categories"]
    assert detail["effective_filters"]["resstock/mf_small"][0]["column"] == "in.vintage"
    assert str(root) not in json.dumps(detail)


def test_openapi_documents_the_filter_endpoints():
    from microgrid_api import openapi_export

    paths = json.loads(openapi_export.render())["paths"]
    assert "get" in paths[f"{BASE}/active"]
    assert {"get", "post"} <= set(paths[f"{BASE}/versions"])
    assert "get" in paths[f"{BASE}/versions/{{version}}"]
    assert "post" in paths[f"{BASE}/versions/{{version}}/activate"]
    assert "post" in paths[f"{BASE}/preview"]
    run_create = json.loads(openapi_export.render())["components"]["schemas"]["RunCreate"]["properties"]
    assert "filter_selections" in run_create


def test_validation_errors_do_not_echo_the_submitted_input(api):
    doc = _doc()
    doc["attributes"][0]["kind"] = "echo-marker-7f3a"
    r = _create(api, doc)
    assert r.status_code == 422
    assert "echo-marker-7f3a" not in r.text
    assert any("kind" in "/".join(map(str, e["loc"])) for e in r.json()["detail"])
