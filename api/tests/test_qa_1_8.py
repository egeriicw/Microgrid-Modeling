"""QA additions for story 1.8 (FR-PLAT-1): secrecy, settings contract, deploy-facing entry points, migrated schema."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from microgrid_api.config import Settings
from microgrid_api.db.models import Run, RunEvent
from microgrid_api.main import create_app

API_DIR = Path(__file__).resolve().parents[1]
SECRET = "S3CRETpw"


def _cfg(url: str | None) -> Config:
    cfg = Config(str(API_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(API_DIR / "src" / "microgrid_api" / "migrations"))
    if url is not None:
        cfg.set_main_option("sqlalchemy.url", url)
    return cfg


# --- /ready must never leak credentials -------------------------------------------------------------------------


def test_ready_does_not_leak_credentials_from_real_driver_error():
    url = f"postgresql+psycopg://bob:{SECRET}@127.0.0.1:1/db"  # port 1: connection refused
    with TestClient(create_app(Settings(database_url=url, environment="test"))) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 503
    assert SECRET not in r.text and "bob" not in r.text and "127.0.0.1" not in r.text


def test_ready_does_not_leak_exception_message(app, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError(f"could not connect postgresql://bob:{SECRET}@db/x")

    monkeypatch.setattr("microgrid_api.routers.health._probe_db", boom)
    with TestClient(app) as c:
        r = c.get("/api/v1/ready")
    assert r.status_code == 503
    assert SECRET not in r.text
    assert r.json()["checks"]["db"] == "unavailable: RuntimeError"


def test_unparseable_database_url_fails_loudly_without_echoing_it():
    with pytest.raises(Exception) as ei:
        create_app(Settings(database_url=f"not a url {SECRET}"))
    assert SECRET not in str(ei.value)


# --- settings contract ------------------------------------------------------------------------------------------


def test_default_sends_no_cors_headers(client):
    r = client.get("/api/v1/health", headers={"Origin": "http://evil.example"})
    assert "access-control-allow-origin" not in r.headers


def test_configured_cors_origin_is_honoured(tmp_path):
    s = Settings(database_url=f"sqlite:///{tmp_path / 'c.db'}", cors_origins=["http://localhost:3000"])
    with TestClient(create_app(s)) as c:
        r = c.get("/api/v1/health", headers={"Origin": "http://localhost:3000"})
    assert r.headers.get("access-control-allow-origin") == "http://localhost:3000"


def test_unprefixed_database_url_is_not_silently_ignored(monkeypatch, tmp_path):
    for k in [k for k in os.environ if k.startswith("MICROGRID_")]:
        monkeypatch.delenv(k)
    monkeypatch.chdir(tmp_path)  # no stray .env
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@db:5432/x")
    try:
        got = Settings().database_url
    except Exception:
        return  # failing loudly is an acceptable fix
    assert got == "postgresql+psycopg://u:p@db:5432/x"


# --- entry points the deploy side will use ----------------------------------------------------------------------


def test_uvicorn_app_target_resolves(monkeypatch, tmp_path):
    from uvicorn.importer import import_from_string

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MICROGRID_DATABASE_URL", f"sqlite:///{tmp_path / 'u.db'}")
    assert isinstance(import_from_string("microgrid_api.main:app"), FastAPI)


def test_uvicorn_factory_target_resolves(monkeypatch, tmp_path):
    from uvicorn.importer import import_from_string

    monkeypatch.setenv("MICROGRID_DATABASE_URL", f"sqlite:///{tmp_path / 'u.db'}")
    assert isinstance(import_from_string("microgrid_api.main:create_app")(), FastAPI)


def test_alembic_uses_env_database_url_when_ini_url_blank(monkeypatch, tmp_path):
    url = f"sqlite:///{tmp_path / 'env.db'}"
    monkeypatch.setenv("MICROGRID_DATABASE_URL", url)
    command.upgrade(_cfg(None), "head")
    assert {"users", "runs", "run_events", "compositions"} <= set(inspect(create_engine(url)).get_table_names())


# --- the MIGRATED schema (not create_all) behaves like the models ----------------------------------------------


@pytest.fixture
def migrated_session(tmp_path):
    url = f"sqlite:///{tmp_path / 'mig_use.db'}"
    command.upgrade(_cfg(url), "head")
    engine = create_engine(url)
    event.listen(engine, "connect", lambda dbapi, _: dbapi.execute("PRAGMA foreign_keys=ON"))
    with Session(engine) as s:
        yield s


def test_migrated_schema_rejects_unknown_role(migrated_session):
    with pytest.raises(IntegrityError):
        migrated_session.connection().exec_driver_sql(
            "INSERT INTO users (email, password_hash, role) VALUES ('y@example.com', 'h', 'root')"
        )


def test_migrated_schema_run_event_roundtrip_and_cascade(migrated_session):
    s = migrated_session
    run = Run(composition_snapshot={"a": 1}, seeds={"master": 1}, engine_version="0.1.0")
    s.add(run)
    s.flush()
    s.add(RunEvent(run_id=run.id, stage="start"))
    s.commit()
    assert s.scalars(select(RunEvent)).one().message == ""
    s.delete(run)
    s.commit()
    assert s.scalars(select(RunEvent)).all() == []  # ON DELETE CASCADE present in the migrated schema


PG = os.environ.get("MICROGRID_TEST_DATABASE_URL")


@pytest.mark.integration
@pytest.mark.skipif(not PG, reason="set MICROGRID_TEST_DATABASE_URL to run Postgres tests")
def test_models_create_all_and_crud_on_postgres():
    """Portability of the models themselves (not just the migration): create_all + insert on a real Postgres."""
    from microgrid_api.db.base import Base

    assert PG
    engine = create_engine(PG)
    Base.metadata.drop_all(engine)
    try:
        Base.metadata.create_all(engine)
        with Session(engine) as s:
            run = Run(composition_snapshot={"a": 1}, seeds={"m": 1}, engine_version="0.1.0")
            s.add(run)
            s.flush()
            s.add(RunEvent(run_id=run.id, stage="start", payload={"k": [1, 2]}))
            s.commit()
            assert s.scalars(select(RunEvent)).one().payload == {"k": [1, 2]}
    finally:
        Base.metadata.drop_all(engine)
