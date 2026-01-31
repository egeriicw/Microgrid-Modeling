from __future__ import annotations

import os

from fastapi.testclient import TestClient

os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"
os.environ["MICROGRID_RUNNER_MODE"] = "mock"

from microgrid_api.db import engine  # noqa: E402
from microgrid_api.main import app  # noqa: E402
from microgrid_api.models import Base  # noqa: E402


def test_create_run_mock() -> None:
    Base.metadata.create_all(bind=engine)

    client = TestClient(app)

    # create config
    r = client.post("/configs", json={"name": "c1", "yaml_text": "x: 1\n"})
    assert r.status_code == 200
    cfg_id = r.json()["id"]

    # create run
    r2 = client.post("/runs", json={"config_id": cfg_id})
    assert r2.status_code == 200
    run_id = r2.json()["run_id"]

    # poll run status
    r3 = client.get(f"/runs/{run_id}")
    assert r3.status_code == 200
    assert r3.json()["status"] in {"queued", "running", "succeeded"}
