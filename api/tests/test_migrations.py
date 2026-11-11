"""FR-PLAT-1: Alembic round trip and model/migration parity (SQLite; Postgres opt-in)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine, inspect

from microgrid_api.db.base import Base
from microgrid_api.db.models import Composition  # noqa: F401  (register models)

API_DIR = Path(__file__).resolve().parents[1]
TABLES = {"users", "compositions", "runs", "run_events"}


def _cfg(url: str) -> Config:
    cfg = Config(str(API_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(API_DIR / "src" / "microgrid_api" / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _tables(url: str) -> set[str]:
    return set(inspect(create_engine(url)).get_table_names()) - {"alembic_version"}


def _round_trip(url: str) -> None:
    cfg = _cfg(url)
    command.upgrade(cfg, "head")
    assert _tables(url) == TABLES
    command.downgrade(cfg, "base")
    assert _tables(url) == set()
    command.upgrade(cfg, "head")
    assert _tables(url) == TABLES


def _diffs(url: str):
    engine = create_engine(url)
    with engine.connect() as conn:
        ctx = MigrationContext.configure(conn, opts={"compare_type": True})
        return compare_metadata(ctx, Base.metadata)


def test_upgrade_downgrade_upgrade_sqlite(tmp_path):
    _round_trip(f"sqlite:///{tmp_path / 'mig.db'}")


def test_models_match_migration_sqlite(tmp_path):
    url = f"sqlite:///{tmp_path / 'drift.db'}"
    command.upgrade(_cfg(url), "head")
    assert _diffs(url) == []


PG = os.environ.get("MICROGRID_TEST_DATABASE_URL")


@pytest.mark.integration
@pytest.mark.skipif(not PG, reason="set MICROGRID_TEST_DATABASE_URL to run Postgres tests")
def test_round_trip_and_parity_postgres():
    assert PG
    _round_trip(PG)
    assert _diffs(PG) == []
    command.downgrade(_cfg(PG), "base")
