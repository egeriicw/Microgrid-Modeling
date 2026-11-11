"""FR-PLAT-1 / FR-DATA-4: MVP spine models (portable types) and role enum."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from microgrid_api.db.base import Base
from microgrid_api.db.models import Composition, Role, Run, RunEvent, User


@pytest.fixture
def session(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'm.db'}")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def test_roles():
    assert {r.value for r in Role} == {"admin", "client", "demo"}


def test_user_email_unique(session):
    session.add(User(email="a@example.com", password_hash="x", role=Role.admin))
    session.commit()
    session.add(User(email="a@example.com", password_hash="y", role=Role.client))
    with pytest.raises(IntegrityError):
        session.commit()


def test_composition_defaults_and_json(session):
    c = Composition(name="demo", body={"total": 10, "types": {"sf": [0.5, 1]}}, is_demo=True)
    session.add(c)
    session.commit()
    got = session.scalars(select(Composition)).one()
    assert got.owner_id is None and got.version == 1 and got.is_demo is True
    assert got.body["total"] == 10 and got.created_at is not None


def test_run_and_events(session):
    run = Run(composition_snapshot={"a": 1}, seeds={"master": 7}, engine_version="0.1.0")
    session.add(run)
    session.flush()
    assert isinstance(run.id, uuid.UUID)
    assert run.status == "queued" and run.data_manifest_hash is None and run.created_at is not None
    session.add(RunEvent(run_id=run.id, stage="start", message="hi", payload={"k": 1}))
    session.commit()
    ev = session.scalars(select(RunEvent)).one()
    assert ev.ts is not None and ev.payload == {"k": 1}
