"""/ready Redis check (story 1.10). Uses fakeredis or a stub; no real Redis server is contacted."""

from __future__ import annotations

import logging

import fakeredis
from fastapi.testclient import TestClient

from microgrid_api.config import Settings
from microgrid_api.main import create_app

SECRET = "R3DISpw"


def _app(tmp_path, redis_url, migrated_settings=None):
    from microgrid_api.db.migrate import upgrade_to_head

    url = f"sqlite:///{tmp_path / 'r.db'}"
    upgrade_to_head(url)
    return create_app(Settings(database_url=url, environment="test", redis_url=redis_url))


def test_no_redis_check_when_not_configured(client):
    r = client.get("/api/v1/ready")
    assert r.status_code == 200
    assert "redis" not in r.json()["checks"]


def test_redis_ok_when_reachable(tmp_path, monkeypatch):
    server = fakeredis.FakeServer()
    monkeypatch.setattr(
        "microgrid_api.routers.health._redis_client", lambda url: fakeredis.FakeStrictRedis(server=server)
    )
    with TestClient(_app(tmp_path, f"redis://:{SECRET}@cache:6379/0")) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 200
    assert r.json() == {"ready": True, "checks": {"db": "ok", "schema": "ok", "redis": "ok"}}


def test_redis_down_gives_503_with_safe_class_only(tmp_path, monkeypatch, caplog):
    def boom(url):
        class Dead:
            def close(self):
                pass

            def ping(self):
                raise ConnectionError(f"Error connecting to redis://:{SECRET}@cache:6379/0")

        return Dead()

    monkeypatch.setattr("microgrid_api.routers.health._redis_client", boom)
    with caplog.at_level(logging.DEBUG), TestClient(_app(tmp_path, f"redis://:{SECRET}@cache:6379/0")) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 503
    body = r.json()
    assert body["ready"] is False
    assert body["checks"]["redis"] == "unavailable: ConnectionError"
    assert body["checks"]["db"] == "ok" and body["checks"]["schema"] == "ok"
    assert SECRET not in r.text and SECRET not in caplog.text


def test_real_unreachable_redis_does_not_leak_credentials(tmp_path):
    """Port 1 refuses the connection quickly; the real redis client error text embeds the host, never shown."""
    with TestClient(_app(tmp_path, f"redis://:{SECRET}@127.0.0.1:1/0")) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 503
    assert r.json()["checks"]["redis"].startswith("unavailable: ")
    assert SECRET not in r.text and "127.0.0.1" not in r.text


def test_db_failure_still_reported_first_when_redis_configured(tmp_path, monkeypatch):
    app = _app(tmp_path, f"redis://:{SECRET}@cache:6379/0")
    monkeypatch.setattr("microgrid_api.routers.health._probe_db", lambda request: (_ for _ in ()).throw(OSError("x")))
    with TestClient(app) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 503 and r.json()["checks"]["db"] == "unavailable: OSError"
    assert SECRET not in r.text


def test_settings_repr_and_worker_logs_hide_redis_url(caplog):
    s = Settings(environment="test", redis_url=f"redis://:{SECRET}@cache:6379/0")
    assert SECRET not in repr(s) and SECRET not in str(s)
    assert SECRET not in s.model_dump_json(exclude={"redis_url"})
