"""Independent QA review of W1-DEPLOY, API side.

* The env contract: the environment Compose really renders for migrate, api and worker builds valid production
  Settings (proves the single-quoted JSON in deploy/.env.example survives Compose's .env parsing).
* The response shape the smoke script's sed parsing depends on (compact JSON, `id` then `status`).
* The server-built `data` section names the characteristics files (a defect found here, fixed in W1-COMPOSE).
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from microgrid_api import worker
from microgrid_api.config import Settings
from microgrid_api.db.models import Run, RunStatus
from microgrid_api.main import create_app

from .http_support import bearer, client_config, make_settings

REPO_ROOT = Path(__file__).resolve().parents[2]
PROD_SERVICES = ("migrate", "api", "worker")


def _rendered_compose(extra_env: dict[str, str] | None = None) -> dict:
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not installed (no daemon needed, only `docker compose config`)")
    import os

    env = {k: v for k, v in os.environ.items() if not k.startswith(("MICROGRID_", "API_KEYS", "JWT_", "POSTGRES_"))}
    env |= extra_env or {}
    r = subprocess.run(
        ["docker", "compose", "--project-directory", str(REPO_ROOT), "--env-file",
         str(REPO_ROOT / "deploy" / ".env.example"), "-f", str(REPO_ROOT / "deploy" / "docker-compose.yml"),
         "config", "--format", "json"],
        capture_output=True, text=True, env=env, check=False,
    )  # fmt: skip
    if r.returncode != 0 and "compose" in r.stderr and "unknown" in r.stderr:
        pytest.skip("docker compose plugin not installed")
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


@pytest.mark.parametrize("service", PROD_SERVICES)
def test_rendered_compose_env_builds_valid_production_settings(service, monkeypatch, tmp_path):
    env = _rendered_compose()["services"][service]["environment"]
    monkeypatch.chdir(tmp_path)  # no stray .env file
    for key in list(__import__("os").environ):
        if key.startswith("MICROGRID_") or key == "DATABASE_URL":
            monkeypatch.delenv(key)
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))
    s = Settings()
    assert s.environment == "production"
    assert s.database_url and s.database_url.startswith("postgresql+psycopg://") and "@db:5432/" in s.database_url
    assert s.redis_url == "redis://redis:6379/0"
    assert s.jwt_secret is not None
    assert {k.subject: k.role for k in s.api_keys} == {"dev-admin": "admin", "dev-client": "client"}
    assert s.data_dir == Path("/data") and s.output_dir == Path("/outputs")


def test_host_data_dir_variable_never_leaks_into_the_containers():
    """MICROGRID_DATA_DIR means a HOST folder in deploy/.env and the shell, but /data inside the containers."""
    cfg = _rendered_compose({"MICROGRID_DATA_DIR": "/srv/elsewhere"})
    for name in ("api", "worker"):
        svc = cfg["services"][name]
        assert svc["environment"]["MICROGRID_DATA_DIR"] == "/data"
        mount = next(v for v in svc["volumes"] if v["target"] == "/data")
        assert mount["source"] == "/srv/elsewhere" and mount["read_only"] is True
    assert cfg["services"]["migrate"]["environment"]["MICROGRID_DATA_DIR"] == "/data"


# --- the response shape the smoke script parses with sed ---------------------------------------------------------


@pytest.fixture
def http_settings(tmp_path, synthetic_dataset, tiny_config):
    return make_settings(tmp_path, synthetic_dataset, tiny_config)


@pytest.fixture
def api(http_settings):
    app = create_app(http_settings)
    app.state.enqueue = lambda run_id: None
    with TestClient(app) as c:
        yield c


def test_run_json_is_compact_with_id_then_status(api, tiny_config):
    """deploy/tests/test_compose_smoke.sh extracts the id with `^{"id":"<uuid>"` and the status with
    `^{"id":"...","status":"<word>"`. A model field reorder or pretty printing would break the CI smoke run."""
    r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": client_config(tiny_config)})
    assert r.status_code == 202
    m = re.match(r'^\{"id":"([0-9a-f-]*)","status":"([a-z]*)"', r.text)
    assert m and m.group(2) == "queued", r.text[:80]
    d = api.get(f"/api/v1/runs/{m.group(1)}", headers=bearer("alice"))
    assert re.match(rf'^\{{"id":"{m.group(1)}","status":"queued"', d.text), d.text[:80]


# --- formerly a defect: characteristics file names are now set by the server (fixed in W1-COMPOSE) ---------------
# The server names them from MICROGRID_RESSTOCK_CHARS_FILE / MICROGRID_COMSTOCK_CHARS_FILE, stores them in the
# snapshot and checks the files exist before queueing.


def test_http_run_snapshot_names_the_characteristics_files_and_runs(api, http_settings, monkeypatch):
    """The synthetic dataset names its files chars_resstock.xlsx / chars_comstock.xlsx (as a real release might use
    any name). A run created over HTTP must record which files it reads and must execute."""
    from microgrid_api.services import runs as svc  # noqa: F401  (the code under review)

    cfg = {
        "scenario": {"name": "chars"},
        "composition": {
            "total_buildings": 4,
            "method": "dirichlet",
            "categories": {
                "mf_small": {"min_fraction": 0.5, "max_fraction": 1.0, "source": "resstock"},
                "mf_mid": {"min_fraction": 0.0, "max_fraction": 0.5, "source": "resstock"},
            },
        },
        "archetype_filters": {
            "resstock": {
                "mf_small": {
                    "building_type_heights": [
                        "Multi-Family with 2 - 4 Units",
                        "Multi-Family with 5+ Units, 1-3 Stories",
                    ]
                },
                "mf_mid": {"building_type_heights": ["Multi-Family with 5+ Units, 4-7 Stories"]},
            }
        },
        "ensemble": {"mode": "fixed", "max_runs": 2},
        "perturbation": {"enabled": False},
    }
    r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": cfg})
    assert r.status_code == 202, r.text
    rid = uuid.UUID(r.json()["id"])
    factory = api.app.state.session_factory
    with factory() as s:
        row = s.get(Run, rid)
        assert row is not None
        data = row.composition_snapshot["data"]
    assert {"resstock_chars_file", "comstock_chars_file"} <= set(data), sorted(data)
    background = Path(data["background_dir"])
    assert (background / data["resstock_chars_file"]).is_file()
    assert (background / data["comstock_chars_file"]).is_file()
    monkeypatch.setattr(worker, "_settings_override", http_settings)
    worker.execute_run(rid, factory)
    with factory() as s:
        assert s.get(Run, rid).status == RunStatus.succeeded.value
