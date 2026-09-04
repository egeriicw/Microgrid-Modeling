"""Shared fixtures for the API test suite.

RED-phase scaffolding: fixtures raise/skip until `microgrid_api` exists.
See docs/test-plan.md §3 (Fixtures) and §4 Phase 2/3.

Design targets:
  * `client`      — httpx client bound to the ASGI app (no network)
  * `db`          — schema via `alembic upgrade head` on a throwaway Postgres,
                    each test wrapped in a transaction that is rolled back
  * `fake_queue`  — fakeredis + RQ SimpleWorker (is_async=False)
  * `storage`     — LocalStorage(tmp_path) implementing the Storage protocol
  * `auth_headers`— {"Authorization": "Bearer test-token"}
"""
from __future__ import annotations

import pytest

microgrid_api = pytest.importorskip(
    "microgrid_api", reason="Phase 2: api package not implemented yet"
)


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer test-token"}


@pytest.fixture
def settings_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://microgrid:microgrid@localhost:5432/microgrid_test")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/15")
    monkeypatch.setenv("S3_ENDPOINT", "memory://")
    monkeypatch.setenv("S3_BUCKET", "test-artifacts")
    monkeypatch.setenv("API_TOKEN", "test-token")
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MAX_RUNS_CEILING", "50")


@pytest.fixture
def db(settings_env):
    """Throwaway schema + per-test rollback. Implemented in Phase 2."""
    pytest.skip("db fixture depends on Phase 2 models + alembic")


@pytest.fixture
def storage(tmp_path):
    from microgrid_api.storage import LocalStorage  # noqa

    return LocalStorage(root=tmp_path / "artifacts")


@pytest.fixture
def fake_queue(settings_env):
    import fakeredis
    from rq import Queue

    conn = fakeredis.FakeStrictRedis()
    return Queue("runs", connection=conn, is_async=False)


@pytest.fixture
def client(db, storage, fake_queue):
    from httpx import ASGITransport, AsyncClient  # noqa

    from microgrid_api.main import create_app

    app = create_app()
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")
