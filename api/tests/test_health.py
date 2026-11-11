"""FR-PLAT-1: health, readiness, OpenAPI and error contract."""

from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from microgrid_api.config import Settings
from microgrid_api.main import create_app

BAD_DB = "sqlite:////nonexistent_dir/x/y.db"


def test_health_ok_without_dependencies():
    # database URL points at an impossible location: liveness must not touch it.
    app = create_app(Settings(database_url=BAD_DB, environment="test"))
    with TestClient(app) as c:
        r = c.get("/api/v1/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_ready_ok_when_db_up(client):
    r = client.get("/api/v1/ready")
    assert r.status_code == 200
    assert r.json() == {"ready": True, "checks": {"db": "ok", "schema": "ok"}}


def test_ready_503_when_db_down(app, monkeypatch):
    def boom(*a, **k):
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))

    monkeypatch.setattr("microgrid_api.routers.health._probe_db", boom)
    with TestClient(app) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 503
    body = r.json()
    assert body["ready"] is False
    assert body["checks"]["db"].startswith("unavailable")


def test_ready_503_with_real_unreachable_db():
    app = create_app(Settings(database_url=BAD_DB, environment="test"))
    with TestClient(app) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 503
    assert r.json()["ready"] is False


def test_openapi_lists_health_and_ready(client):
    r = client.get("/api/v1/openapi.json")
    assert r.status_code == 200
    paths = r.json()["paths"]
    assert "/api/v1/health" in paths
    assert "/api/v1/ready" in paths


def test_docs_served_under_prefix(client):
    assert client.get("/api/v1/docs").status_code == 200


def test_unknown_route_returns_json_error(client):
    r = client.get("/api/v1/nope")
    assert r.status_code == 404
    assert r.headers["content-type"].startswith("application/json")
    assert r.json() == {"detail": "Not Found"}


def test_settings_read_env(monkeypatch):
    monkeypatch.setenv("MICROGRID_DATABASE_URL", "sqlite:///env.db")
    assert Settings().database_url == "sqlite:///env.db"
