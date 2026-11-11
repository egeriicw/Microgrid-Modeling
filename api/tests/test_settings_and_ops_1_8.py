"""Story 1.8 post-QA fixes: fail-loud settings, env contract, CORS, schema-aware /ready, migrate entry point."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect

from microgrid_api.config import ApiKey, ConfigurationError, Settings
from microgrid_api.main import create_app

SECRET = "S3CRETjwt"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, tmp_path):
    for k in [k for k in os.environ if k.startswith("MICROGRID_") or k == "DATABASE_URL"]:
        monkeypatch.delenv(k)
    monkeypatch.chdir(tmp_path)  # no stray .env


# --- fail-loud settings ----------------------------------------------------------------------------------------


def test_development_defaults_to_sqlite():
    assert Settings().require_database_url().startswith("sqlite:///")


def test_production_requires_database_url():
    with pytest.raises(ConfigurationError, match="MICROGRID_DATABASE_URL"):
        Settings(environment="production", jwt_secret=SECRET)


def test_production_requires_jwt_secret_and_never_echoes_it():
    with pytest.raises(ConfigurationError, match="MICROGRID_JWT_SECRET"):
        Settings(environment="production", database_url="postgresql+psycopg://u:pw@h/db")


def test_production_ok_and_secret_not_in_repr():
    s = Settings(
        environment="production",
        database_url="postgresql+psycopg://u:pw@h/db",
        jwt_secret=SECRET,
        api_keys=[ApiKey(subject="a", role="admin", key="k" * 20)],
    )
    assert SECRET not in repr(s) and SECRET not in str(s.model_dump())


def test_unprefixed_database_url_error_names_both(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:pw@db/x")
    with pytest.raises(ConfigurationError) as ei:
        Settings()
    assert "MICROGRID_DATABASE_URL" in str(ei.value) and "pw" not in str(ei.value)


def test_unprefixed_ok_when_prefixed_also_set(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:pw@db/x")
    monkeypatch.setenv("MICROGRID_DATABASE_URL", f"sqlite:///{tmp_path / 'a.db'}")
    assert Settings().require_database_url().startswith("sqlite")


def test_contract_env_names_map_to_fields(monkeypatch):
    monkeypatch.setenv("MICROGRID_REDIS_URL", "redis://r:6379/0")
    monkeypatch.setenv("MICROGRID_JWT_SECRET", SECRET)
    monkeypatch.setenv("MICROGRID_DATA_DIR", "/data")
    monkeypatch.setenv("MICROGRID_OUTPUT_DIR", "/out")
    s = Settings()
    assert s.redis_url == "redis://r:6379/0"
    assert s.jwt_secret is not None and s.jwt_secret.get_secret_value() == SECRET
    assert s.data_dir == Path("/data") and s.output_dir == Path("/out")


def test_startup_logs_database_kind_without_credentials(caplog, tmp_path):
    with caplog.at_level(logging.INFO, logger="microgrid_api"):
        create_app(Settings(database_url=f"sqlite:///{tmp_path / 'l.db'}"))
    assert "sqlite" in caplog.text and str(tmp_path) not in caplog.text


def test_startup_log_hides_postgres_credentials(caplog):
    with caplog.at_level(logging.INFO, logger="microgrid_api"):
        create_app(Settings(database_url="postgresql+psycopg://bob:pw@dbhost/x"))
    assert "postgresql" in caplog.text and "pw" not in caplog.text and "bob" not in caplog.text


# --- CORS ------------------------------------------------------------------------------------------------------


def test_cors_wildcard_rejected():
    with pytest.raises(ConfigurationError, match="cors"):
        Settings(cors_origins=["*"])


def test_cors_unlisted_origin_gets_no_header(tmp_path):
    s = Settings(database_url=f"sqlite:///{tmp_path / 'c.db'}", cors_origins=["http://localhost:3000"])
    with TestClient(create_app(s)) as c:
        r = c.get("/api/v1/health", headers={"Origin": "http://evil.example"})
    assert "access-control-allow-origin" not in r.headers


# --- /ready schema check ---------------------------------------------------------------------------------------


def test_ready_not_ready_on_empty_db(tmp_path):
    with TestClient(create_app(Settings(database_url=f"sqlite:///{tmp_path / 'e.db'}"))) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 503
    body = r.json()
    assert body["ready"] is False and body["checks"]["db"] == "ok"
    assert body["checks"]["schema"].startswith("not migrated")
    assert str(tmp_path) not in r.text


def test_ready_not_ready_on_stale_revision(tmp_path):
    url = f"sqlite:///{tmp_path / 's.db'}"
    eng = create_engine(url)
    with eng.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        conn.exec_driver_sql("INSERT INTO alembic_version VALUES ('deadbeef')")
    with TestClient(create_app(Settings(database_url=url))) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 503 and r.json()["checks"]["schema"].startswith("not migrated")


def test_ready_ok_when_migrated(client):
    # conftest `client` fixture migrates its DB
    r = client.get("/api/v1/ready")
    assert r.status_code == 200
    assert r.json() == {"ready": True, "checks": {"db": "ok", "schema": "ok"}}


# --- migrate entry point ---------------------------------------------------------------------------------------


def test_migrate_module_works_from_other_cwd(tmp_path):
    url = f"sqlite:///{tmp_path / 'm.db'}"
    env = {**os.environ, "MICROGRID_DATABASE_URL": url}
    r = subprocess.run(
        [sys.executable, "-m", "microgrid_api.db.migrate"], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    assert r.returncode == 0, r.stderr
    assert "users" in inspect(create_engine(url)).get_table_names()


def test_migrate_fails_loudly_without_database_url(tmp_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith("MICROGRID_")}
    env["MICROGRID_ENVIRONMENT"] = "production"
    r = subprocess.run(
        [sys.executable, "-m", "microgrid_api.db.migrate"], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    assert r.returncode != 0 and "MICROGRID_DATABASE_URL" in r.stderr


def test_migrations_are_package_data():
    import microgrid_api.migrations as m

    d = Path(m.__file__).parent
    assert (d / "env.py").is_file() and (d / "script.py.mako").is_file() and any((d / "versions").glob("0001*.py"))
