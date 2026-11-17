"""Story A1/A4: role enforcement, demo restrictions, settings validation for API keys."""

from __future__ import annotations

import copy

import pytest
import yaml
from fastapi.testclient import TestClient

from microgrid_api.config import ApiKey, ConfigurationError, Settings
from microgrid_api.main import create_app

from .http_support import KEYS, bearer, client_config, make_settings, write_demo_file


@pytest.fixture
def demo_api(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config)
    app = create_app(s)
    app.state.enqueued = []
    app.state.enqueue = lambda rid: app.state.enqueued.append(str(rid))
    with TestClient(app) as c:
        yield c


def _demo_post(c, **body):
    return c.post("/api/v1/runs", headers=bearer("demo"), json=body)


def test_demo_runs_predefined_composition_with_fixed_seed(demo_api):
    r1 = _demo_post(demo_api, composition_id="small-mixed")
    r2 = _demo_post(demo_api, composition_id="small-mixed")
    assert r1.status_code == r2.status_code == 202, r1.text
    assert r1.json()["seeds"] == r2.json()["seeds"]
    assert set(r1.json()["seeds"].values()) == {Settings(environment="test").demo_seed}
    assert r1.json()["owner"] == "demo"


def test_demo_cannot_send_own_config_or_seeds(demo_api, tiny_config):
    assert _demo_post(demo_api, config=client_config(tiny_config)).status_code == 403
    r = _demo_post(demo_api, composition_id="small-mixed", seeds={"composition_seed": 1})
    assert r.status_code == 403
    assert _demo_post(demo_api, composition_id="nope").status_code == 404
    assert demo_api.app.state.enqueued == []


def test_demo_composition_listing_for_any_role(demo_api):
    for who in ("demo", "alice", "admin"):
        r = demo_api.get("/api/v1/demo-compositions", headers=bearer(who))
        assert r.status_code == 200 and r.json() == {"items": ["small-mixed"]}
    assert demo_api.get("/api/v1/demo-compositions").status_code == 401


def test_client_may_also_use_predefined_composition(demo_api):
    r = demo_api.post("/api/v1/runs", headers=bearer("alice"), json={"composition_id": "small-mixed"})
    assert r.status_code == 202


def test_no_demo_file_means_demo_cannot_run(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, demo_compositions_file=None)
    with TestClient(create_app(s)) as c:
        r = c.post("/api/v1/runs", headers=bearer("demo"), json={"composition_id": "small-mixed"})
        assert r.status_code == 404


def test_demo_composition_over_building_cap_fails_at_startup(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, demo_max_buildings=3)
    with pytest.raises(ConfigurationError, match="total_buildings"):
        create_app(s)


def test_demo_composition_over_run_cap_fails_at_startup(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, demo_max_runs=2)
    with pytest.raises(ConfigurationError, match="max_runs"):
        create_app(s)


def test_demo_composition_with_filters_or_data_fails_at_startup(tmp_path, synthetic_dataset, tiny_config):
    cfg = client_config(tiny_config)
    cfg["archetype_filters"]["resstock"]["mf_small"]["attributes"] = [{"column": "x", "op": "not_null"}]
    p = tmp_path / "bad.yaml"
    p.write_text(yaml.safe_dump({"compositions": {"f": cfg}}))
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, demo_compositions_file=p)
    with pytest.raises(ConfigurationError, match="filters"):
        create_app(s)
    p.write_text(yaml.safe_dump({"compositions": {"d": copy.deepcopy(tiny_config)}}))  # still has `data`
    with pytest.raises(ConfigurationError, match="data"):
        create_app(s)


def test_missing_or_malformed_demo_file_fails_loudly(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, demo_compositions_file=tmp_path / "missing.yaml")
    with pytest.raises(ConfigurationError, match="demo"):
        create_app(s)
    p = write_demo_file(tmp_path / "ok.yaml", tiny_config)
    p.write_text("compositions: [1, 2]")
    s2 = make_settings(tmp_path, synthetic_dataset, tiny_config, demo_compositions_file=p)
    with pytest.raises(ConfigurationError, match="compositions"):
        create_app(s2)


def test_no_api_keys_in_dev_means_every_endpoint_is_401(tmp_path):
    s = Settings(database_url=f"sqlite:///{tmp_path / 'x.db'}", environment="test")
    with TestClient(create_app(s)) as c:
        assert c.get("/api/v1/runs", headers=bearer("admin")).status_code == 401
        assert c.get("/api/v1/health").status_code == 200


def test_api_keys_required_outside_development():
    with pytest.raises(ConfigurationError, match="MICROGRID_API_KEYS"):
        Settings(environment="production", database_url="postgresql+psycopg://u:p@h/d", jwt_secret="s")


def test_api_key_validation():
    with pytest.raises(ValueError, match="at least 16"):
        ApiKey(subject="a", role="admin", key="short")
    with pytest.raises(ValueError):
        ApiKey(subject="a", role="root", key="x" * 20)
    base = {"environment": "test", "database_url": "sqlite://"}
    k = ApiKey(subject="a", role="admin", key="x" * 20)
    with pytest.raises(ConfigurationError, match="duplicate"):
        Settings(**base, api_keys=[k, ApiKey(subject="a", role="client", key="y" * 20)])
    with pytest.raises(ConfigurationError, match="duplicate"):
        Settings(**base, api_keys=[k, ApiKey(subject="b", role="client", key="x" * 20)])


def test_keys_never_in_repr():
    k = ApiKey(subject="a", role="admin", key="x" * 20)
    s = Settings(environment="test", database_url="sqlite://", api_keys=[k])
    assert "x" * 20 not in repr(s)
    assert "x" * 20 not in repr(k)
    assert KEYS["admin"] not in repr(s)


def test_env_json_keys(monkeypatch):
    monkeypatch.setenv("MICROGRID_API_KEYS", '[{"subject":"root","role":"admin","key":"%s"}]' % ("k" * 20))
    s = Settings(environment="test", database_url="sqlite://")
    assert s.api_keys[0].role == "admin"
