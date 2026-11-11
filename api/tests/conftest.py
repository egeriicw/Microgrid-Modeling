"""Shared fixtures for the API test suite.

Real fixtures: `settings`, `app`, `client` (sync TestClient, SQLite file per test).
Placeholder fixtures (`db`, `storage`, `fake_queue`, `async_client`) stay skipped until their stories land
(worker: Sprint 1 story 1.10; storage/runs/configs: later).
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from microgrid_api.config import Settings
from microgrid_api.main import create_app


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(database_url=f"sqlite:///{tmp_path / 'test.db'}", environment="test")


@pytest.fixture
def app(settings: Settings):
    return create_app(settings)


@pytest.fixture
def client(app) -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer test-token"}


@pytest.fixture
def db():
    pytest.skip("db fixture for placeholder tests lands with the runs/configs stories")


@pytest.fixture
def storage():
    pytest.skip("storage not implemented")


@pytest.fixture
def fake_queue():
    pytest.skip("worker queue lands in story 1.10 (Celery)")


@pytest.fixture
def async_client():
    pytest.skip("async client fixture lands with the runs/configs stories")
