from __future__ import annotations

import os

from fastapi.testclient import TestClient

# Use SQLite for test isolation in this PR.
os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"

from microgrid_api.db import engine  # noqa: E402
from microgrid_api.main import app  # noqa: E402
from microgrid_api.models import Base  # noqa: E402


def test_create_and_list_configs() -> None:
    # For sqlite tests, create tables directly.
    Base.metadata.create_all(bind=engine)

    client = TestClient(app)

    r = client.post("/configs", json={"name": "test", "yaml_text": "a: 1\n"})
    assert r.status_code == 200
    created = r.json()
    assert created["name"] == "test"

    r2 = client.get("/configs")
    assert r2.status_code == 200
    items = r2.json()
    assert len(items) >= 1
