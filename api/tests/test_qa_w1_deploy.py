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


def _rendered_compose(extra_env: dict[str, str] | None = None, env_file: Path | None = None) -> dict:
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not installed (no daemon needed, only `docker compose config`)")
    import os

    env = {k: v for k, v in os.environ.items() if not k.startswith(("MICROGRID_", "API_KEYS", "JWT_", "POSTGRES_"))}
    env |= extra_env or {}
    r = subprocess.run(
        ["docker", "compose", "--project-directory", str(REPO_ROOT), "--env-file",
         str(env_file or REPO_ROOT / "deploy" / ".env.example"), "-f", str(REPO_ROOT / "deploy" / "docker-compose.yml"),
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
        if value is None:  # a pass-through setting deploy/.env leaves unset: absent in the container
            continue
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


# --- W1-DEPLOY2: data-file, temperature and request settings set through deploy/.env ----------------------------


def _settings_from_rendered(env: dict, monkeypatch, tmp_path) -> Settings:
    monkeypatch.chdir(tmp_path)  # no stray .env file
    for key in list(__import__("os").environ):
        if key.startswith("MICROGRID_") or key == "DATABASE_URL":
            monkeypatch.delenv(key)
    for key, value in env.items():
        if value is not None:  # null = pass-through with nothing set = absent in the container
            monkeypatch.setenv(key, str(value))
    return Settings()


def test_template_defaults_reach_the_api_as_its_own_defaults(monkeypatch, tmp_path):
    """With every new line commented out in the template, the API sees its defaults, not empty strings."""
    s = _settings_from_rendered(_rendered_compose()["services"]["api"]["environment"], monkeypatch, tmp_path)
    fields = Settings.model_fields
    assert s.resstock_chars_file == fields["resstock_chars_file"].default == "DC_upgrade0.xlsx"
    assert s.comstock_chars_file == fields["comstock_chars_file"].default == "DC_upgrade0_agg.xlsx"
    assert s.temperature_source == "none" and s.temperature_column is None and s.temperature_units is None
    assert s.max_request_bytes == fields["max_request_bytes"].default and s.permission_overrides == {}
    # the template documents these same defaults in its commented-out lines
    template = (REPO_ROOT / "deploy" / ".env.example").read_text()
    assert f"# MICROGRID_RESSTOCK_CHARS_FILE={s.resstock_chars_file}\n" in template
    assert f"# MICROGRID_COMSTOCK_CHARS_FILE={s.comstock_chars_file}\n" in template
    assert f"# MICROGRID_MAX_REQUEST_BYTES={s.max_request_bytes}\n" in template


ENV_SET = (
    "\nMICROGRID_RESSTOCK_CHARS_FILE=DC_res_named.xlsx\nMICROGRID_COMSTOCK_CHARS_FILE=DC_com_named.xlsx\n"
    "MICROGRID_TEMPERATURE_SOURCE=weather_file\nMICROGRID_TEMPERATURE_WEATHER_FILE=weather/DC_2018.csv\n"
    "MICROGRID_TEMPERATURE_TIME_COLUMN=date_time\nMICROGRID_TEMPERATURE_COLUMN=Dry Bulb Temperature [°C]\n"
    "MICROGRID_TEMPERATURE_UNITS=C\nMICROGRID_MAX_REQUEST_BYTES=65536\n"
    """MICROGRID_PERMISSION_OVERRIDES='{"client":{"filters.use":true}}'\n"""
)


@pytest.mark.parametrize("service", PROD_SERVICES)
def test_owner_settings_in_deploy_env_reach_every_app_service(service, monkeypatch, tmp_path):
    env_file = tmp_path / "deploy.env"
    env_file.write_text((REPO_ROOT / "deploy" / ".env.example").read_text() + ENV_SET)
    rendered = _rendered_compose(env_file=env_file)["services"][service]["environment"]
    s = _settings_from_rendered(rendered, monkeypatch, tmp_path)
    assert (s.resstock_chars_file, s.comstock_chars_file) == ("DC_res_named.xlsx", "DC_com_named.xlsx")
    assert s.temperature_source == "weather_file" and s.temperature_weather_file == "weather/DC_2018.csv"
    assert s.temperature_column == "Dry Bulb Temperature [°C]" and s.temperature_units == "C"
    assert s.temperature_time_column == "date_time"
    assert s.max_request_bytes == 65536 and s.permission_overrides == {"client": {"filters.use": True}}


@pytest.mark.parametrize(
    "lines",
    [
        "MICROGRID_TEMPERATURE_COLUMN=out.temp\n",  # source stays none: the column would be ignored
        "MICROGRID_TEMPERATURE_COLUMN=\n",  # empty is "set" for Compose and for the API
        "MICROGRID_RESSTOCK_CHARS_FILE=\n",
        "MICROGRID_TEMPERATURE_SOURCE=timeseries_column\n",  # needs column and units
    ],
)
def test_a_half_set_temperature_or_empty_line_stops_the_api_loudly(lines, monkeypatch, tmp_path):
    from microgrid_api.config import ConfigurationError

    env_file = tmp_path / "deploy.env"
    env_file.write_text((REPO_ROOT / "deploy" / ".env.example").read_text() + "\n" + lines)
    rendered = _rendered_compose(env_file=env_file)["services"]["api"]["environment"]
    with pytest.raises((ConfigurationError, ValueError)):
        _settings_from_rendered(rendered, monkeypatch, tmp_path)


def test_smoke_custom_composition_runs_on_the_fixture(api, http_settings, monkeypatch):
    """The CI smoke's second run (deploy/tests/smoke-composition.json) succeeds on the synthetic fixture."""
    body = json.loads((REPO_ROOT / "deploy" / "tests" / "smoke-composition.json").read_text())
    r = api.post("/api/v1/runs", headers=bearer("alice"), json=body)
    assert r.status_code == 202, r.text
    rid = r.json()["id"]
    monkeypatch.setattr(worker, "_settings_override", http_settings)
    worker.execute_run(rid, api.app.state.session_factory)
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("alice")).json()
    assert detail["status"] == "succeeded", detail.get("error_message")
    results = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice"))
    assert results.status_code == 200
    doc = results.json()["results"]
    assert doc["n_runs"] >= 1 and doc["time"]["hourly"]["count"] > 0
