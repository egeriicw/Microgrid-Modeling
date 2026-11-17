"""Independent QA tests for story 1.10 (worker, run service, cancel flag, /ready Redis).

Tests marked `xfail(strict=True)` document real defects found in review; they will start failing (XPASS) when
the defect is fixed, which forces the marker to be removed. Nothing here changes production code.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid

import pytest
from celery.exceptions import SoftTimeLimitExceeded
from microgrid.community_load.synthetic.run import ProgressEvent
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker

from microgrid_api import worker
from microgrid_api.config import Settings
from microgrid_api.db.models import Run, RunEvent, RunStatus
from microgrid_api.services import runs as svc

SECRET = "QAs3cret"
PG = os.environ.get("MICROGRID_TEST_DATABASE_URL")


class _Result:
    def to_dict(self, *, include_results: bool = True):
        return {"engine_version": "9.9.9", "seeds": {}, "metrics": [{"run_index": 0}]}


def _new(session, config=None) -> uuid.UUID:
    return svc.create_run(session, config or {"scenario": {"name": "x"}}, validate=False).id


def _ev(stage="running", i=0):
    return ProgressEvent(stage, i, 1, 1, 2, None, "m")


def _set(session_factory, run_id, **values):
    with session_factory() as s:
        s.execute(update(Run).where(Run.id == run_id).values(**values))
        s.commit()


def _row(session_factory, run_id) -> Run:
    with session_factory() as s:
        return s.get(Run, run_id)


# --- (1) races ---------------------------------------------------------------------------------------------------


def _race_claim(session_factory, run_id, monkeypatch, n=6):
    calls: list[int] = []
    lock = threading.Lock()

    def slow_engine(config, *, progress=None, should_cancel=None, **kw):
        with lock:
            calls.append(1)
        time.sleep(0.3)
        return _Result()

    monkeypatch.setattr(worker, "run_scenario", slow_engine)
    barrier = threading.Barrier(n)
    errors: list[BaseException] = []

    def go():
        barrier.wait()
        try:
            worker.execute_run(run_id, session_factory)
        except BaseException as exc:  # noqa: BLE001 - collected and asserted below
            errors.append(exc)

    threads = [threading.Thread(target=go) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return calls, errors


def test_concurrent_deliveries_run_engine_exactly_once_sqlite(session_factory, session, monkeypatch):
    run_id = _new(session)
    calls, errors = _race_claim(session_factory, run_id, monkeypatch)
    assert errors == []
    assert len(calls) == 1
    assert _row(session_factory, run_id).status == "succeeded"


@pytest.mark.integration
@pytest.mark.skipif(not PG, reason="set MICROGRID_TEST_DATABASE_URL to run Postgres tests")
def test_concurrent_deliveries_run_engine_exactly_once_postgres(monkeypatch):
    from microgrid_api.db.base import Base

    assert PG
    engine = create_engine(PG)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    try:
        with factory() as s:
            run_id = _new(s)
        calls, errors = _race_claim(factory, run_id, monkeypatch, n=8)
        assert errors == []
        assert len(calls) == 1
        assert _row(factory, run_id).status == "succeeded"
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_redelivery_while_running_is_a_noop(session_factory, session, monkeypatch):
    """A second delivery arriving while the first is mid-run must not start the engine or touch the row."""
    run_id = _new(session)
    inner_calls: list[int] = []

    def outer_engine(config, *, progress=None, should_cancel=None, **kw):
        monkeypatch.setattr(worker, "run_scenario", lambda *a, **k: inner_calls.append(1))
        worker.execute_run(run_id, session_factory)  # duplicate delivery during the run
        return _Result()

    monkeypatch.setattr(worker, "run_scenario", outer_engine)
    worker.execute_run(run_id, session_factory)
    assert inner_calls == []
    assert _row(session_factory, run_id).status == "succeeded"


def test_cancel_arriving_after_claim_before_first_poll_is_honored(session_factory, session, tiny_config, monkeypatch):
    """Real engine: the flag is set during pool building (after the claim, before the first poll). No run starts."""
    run_id = svc.create_run(session, tiny_config, trusted_data_paths=True).id
    real = worker.run_scenario
    stages: list[str] = []

    def wrapped(config, *, progress=None, **kw):
        def on_progress(ev):
            stages.append(ev.stage)
            if ev.stage == "building_pool":
                _set(session_factory, run_id, cancel_requested=True)
            progress(ev)

        return real(config, progress=on_progress, **kw)

    monkeypatch.setattr(worker, "run_scenario", wrapped)
    worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    assert row.status == "cancelled" and row.result is None
    assert "running" not in stages


def test_late_progress_event_does_not_change_terminal_status(session_factory, session, monkeypatch):
    """A late progress write never changes the status of a terminal run."""
    run_id = _new(session)

    def engine(config, *, progress=None, **kw):
        _set(session_factory, run_id, status="cancelled")  # terminal state written by someone else
        progress(_ev())
        return _Result()

    monkeypatch.setattr(worker, "run_scenario", engine)
    worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    assert row.status == "cancelled" and not row.progress  # late progress is dropped, status untouched


def test_completion_does_not_overwrite_terminal_cancelled(session_factory, session, monkeypatch):
    run_id = _new(session)

    def engine(config, *, progress=None, **kw):
        _set(session_factory, run_id, status="cancelled")
        return _Result()

    monkeypatch.setattr(worker, "run_scenario", engine)
    worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    assert row.status == "cancelled" and row.result is None


def test_cancel_racing_the_claim_does_not_clobber_running(session_factory, session, monkeypatch):
    run_id = _new(session)
    real_get = svc.get_run

    def get_then_worker_claims(s, rid):
        run = real_get(s, rid)  # request_cancel sees `queued` ...
        _set(
            session_factory,
            rid,
            status="running",
            started_at=run.created_at,  # ... then the worker claims before request_cancel commits
        )
        return run

    monkeypatch.setattr(svc, "get_run", get_then_worker_claims)
    svc.request_cancel(session, run_id)
    row = _row(session_factory, run_id)
    # Acceptable end states: still running with the flag set (worker will stop), never a terminal status that the
    # worker did not itself write.
    assert row.status == "running" and row.cancel_requested is True


def test_cancel_racing_completion_never_cancels_a_succeeded_run(session_factory, session, monkeypatch):
    """request_cancel read `running`, worker then finishes: only the flag is written, status stays succeeded."""
    run_id = _new(session)
    _set(session_factory, run_id, status="running")
    real_get = svc.get_run

    def get_then_finish(s, rid):
        run = real_get(s, rid)
        _set(session_factory, rid, status="succeeded", result={"ok": True})
        return run

    monkeypatch.setattr(svc, "get_run", get_then_finish)
    svc.request_cancel(session, run_id)
    row = _row(session_factory, run_id)
    assert row.status == "succeeded" and row.result == {"ok": True}


def test_progress_write_failure_marks_run_failed_not_stuck_running(session_factory, session, monkeypatch):
    """A DB error inside the progress callback (NOT NULL violation on stage) propagates out of the engine; the row
    is recorded failed with the exception class only."""
    run_id = _new(session)

    def engine(config, *, progress=None, **kw):
        progress(ProgressEvent(None, 0, 1, 1, 2, None, "x"))  # type: ignore[arg-type]
        return _Result()

    monkeypatch.setattr(worker, "run_scenario", engine)
    worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    assert row.status == "failed" and "IntegrityError" in row.error_message
    assert row.result is None and row.finished_at is not None


def test_soft_time_limit_marks_run_failed(session_factory, session, monkeypatch):
    run_id = _new(session)

    def engine(config, **kw):
        raise SoftTimeLimitExceeded()

    monkeypatch.setattr(worker, "run_scenario", engine)
    worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    assert row.status == "failed" and "SoftTimeLimitExceeded" in row.error_message


def test_db_failure_on_final_write_leaves_run_running_and_raises(session_factory, session, monkeypatch):
    """Characterization of a known limit (docstring: reaper is future work): if the terminal write itself fails the
    exception propagates (Celery logs it) and the row stays `running` with no error recorded."""
    run_id = _new(session)
    monkeypatch.setattr(worker, "run_scenario", lambda *a, **k: _Result())

    def broken_finish(*a, **k):
        raise RuntimeError("db gone")

    monkeypatch.setattr(worker, "_finish", broken_finish)
    with pytest.raises(RuntimeError):
        worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    assert row.status == "running" and row.error_message is None


# --- (2) secrets -------------------------------------------------------------------------------------------------


def test_failed_run_leaks_nothing_into_row_events_or_info_logs(session_factory, session, monkeypatch, caplog):
    run_id = _new(session)

    def engine(config, *, progress=None, **kw):
        progress(_ev())
        raise OSError(f"/home/owner/data/secret.xlsx postgresql://u:{SECRET}@db/x redis://:{SECRET}@r/0")

    monkeypatch.setattr(worker, "run_scenario", engine)
    with caplog.at_level(logging.INFO):
        worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    with session_factory() as s:
        events = s.scalars(select(RunEvent).where(RunEvent.run_id == run_id)).all()
    blob = json.dumps([row.error_message, row.progress, [e.message for e in events], [e.payload for e in events]])
    assert SECRET not in blob and "/home/owner" not in blob and "OSError" in row.error_message
    info_only = [r for r in caplog.records if r.levelno < logging.ERROR]
    assert all(SECRET not in r.getMessage() for r in info_only)


def test_celery_app_conf_and_reprs_do_not_expose_broker_password(monkeypatch):
    s = Settings(environment="test", redis_url=f"redis://:{SECRET}@127.0.0.1:1/0")
    app = worker.make_celery(s)
    assert SECRET not in repr(app) and SECRET not in str(app)
    assert SECRET not in app.conf.humanize()
    assert SECRET not in repr(app.tasks["microgrid.run_engine"])


# --- (3) trust boundary ------------------------------------------------------------------------------------------


def test_user_supplied_data_paths_are_rejected_unless_trusted(session):
    """The snapshot's `data` section holds file paths the engine reads; untrusted callers may not set them."""
    evil = {"background_dir": "/etc", "resstock_timeseries_dir": "/root", "comstock_timeseries_dir": "/proc"}
    with pytest.raises(ValueError) as exc:
        svc.create_run(session, {"scenario": {"name": "x"}, "data": evil})
    assert "background_dir" in str(exc.value)
    assert "/etc" not in str(exc.value) and "/root" not in str(exc.value)  # keys only, never values


def test_trusted_data_paths_reach_the_engine_unchanged(session_factory, session, monkeypatch):
    evil = {"background_dir": "/etc", "resstock_timeseries_dir": "/root", "comstock_timeseries_dir": "/proc"}
    run_id = svc.create_run(session, {"scenario": {"name": "x"}, "data": evil}, trusted_data_paths=True).id
    seen: dict = {}

    def engine(config, **kw):
        seen.update(config["data"])
        return _Result()

    monkeypatch.setattr(worker, "run_scenario", engine)
    worker.execute_run(run_id, session_factory)
    assert seen == evil


def test_create_run_rejects_user_supplied_data_paths():
    from sqlalchemy.orm import Session

    engine = create_engine("sqlite://")
    from microgrid_api.db.base import Base

    Base.metadata.create_all(engine)
    with Session(engine) as s, pytest.raises(ValueError):
        svc.create_run(s, {"data": {"background_dir": "/etc"}})


@pytest.mark.parametrize(
    "snapshot",
    [
        {"data": {"resstock_chars_file": "x.xlsx"}},
        {"data": {"comstock_chars_file": "x.xlsx"}},
        {"data": {"comstock_timeseries_dir": "/x"}},
        {"data": {"resstock_timeseries_dir": "/x"}},
        {"data_dir": "/x"},
        {"data_dirs": ["/x"]},
        {"output_dir": "/x"},
        {"data": {"output_dir": "/x"}},
        {"data": {"data_dir": "/x"}},
    ],
)
def test_every_path_key_is_rejected_by_name_only(session, snapshot):
    with pytest.raises(ValueError) as exc:
        svc.create_run(session, snapshot, validate=False)
    assert "/x" not in str(exc.value) and "x.xlsx" not in str(exc.value)


def test_non_path_data_keys_are_allowed(session):
    assert svc.create_run(session, {"data": {"upgrade": "0", "state": "DC"}}, validate=False).status == "queued"


def test_server_side_data_section_builds_paths_under_data_dir(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    data = svc.server_side_data_section(Settings(data_dir=root, environment="test"))
    assert set(data) == {"background_dir", "resstock_timeseries_dir", "comstock_timeseries_dir"}
    assert all(str(root.resolve()) in v for v in data.values())


def test_server_side_data_section_requires_data_dir_and_rejects_symlink_escape(tmp_path):
    with pytest.raises(ValueError):
        svc.server_side_data_section(Settings(environment="test"))
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "background").symlink_to(outside)
    with pytest.raises(ValueError) as exc:
        svc.server_side_data_section(Settings(data_dir=root, environment="test"))
    assert str(outside) not in str(exc.value)


# --- (6) serialization through the broker ------------------------------------------------------------------------


def test_task_payload_through_broker_is_json_with_only_the_run_id():
    """Publishes to the real in-memory transport (messages are serialized as in production) and reads it back."""
    rid = uuid.uuid4()
    worker.enqueue_run(rid)
    with worker.celery_app.connection_for_read() as conn:
        q = conn.SimpleQueue("microgrid")
        msg = q.get(timeout=2)
        msg.ack()
        q.close()
    assert msg.content_type == "application/json"
    args, kwargs, _embed = msg.payload
    assert args == [str(rid)] and kwargs == {}
    assert msg.headers["id"] == str(rid) and msg.headers["task"] == worker.TASK_NAME
    assert msg.headers["ignore_result"] is True
    json.dumps(msg.payload)


def test_execute_run_accepts_the_string_id_the_broker_delivers(session_factory, session, monkeypatch):
    run_id = _new(session)
    monkeypatch.setattr(worker, "run_scenario", lambda *a, **k: _Result())
    worker.execute_run(str(run_id), session_factory)
    assert _row(session_factory, run_id).status == "succeeded"


def test_succeeded_run_keeps_result_and_status_on_late_cancel_flag(session_factory, session, monkeypatch):
    run_id = _new(session)
    monkeypatch.setattr(worker, "run_scenario", lambda *a, **k: _Result())
    worker.execute_run(run_id, session_factory)
    svc.request_cancel(session, run_id)
    row = _row(session_factory, run_id)
    assert row.status == RunStatus.succeeded and row.result and row.cancel_requested is False


# --- (4) migration defaults --------------------------------------------------------------------------------------


def test_new_run_defaults_cancel_false_result_none(session_factory, session):
    run_id = _new(session)
    row = _row(session_factory, run_id)
    assert row.cancel_requested is False and row.result is None and row.error_message is None


def test_server_default_applies_to_raw_insert_without_orm_defaults(session_factory, session):
    """A raw INSERT that omits cancel_requested (an old writer during a rolling deploy) gets false, not NULL."""
    from sqlalchemy import text

    rid = uuid.uuid4()
    with session_factory() as s:
        s.execute(
            text(
                "INSERT INTO runs (id, composition_snapshot, seeds, engine_version, status, progress, created_at) "
                "VALUES (:id, '{}', '{}', 'v', 'queued', '{}', CURRENT_TIMESTAMP)"
            ),
            {"id": rid.hex if session.bind.dialect.name == "sqlite" else str(rid)},
        )
        s.commit()
    assert _row(session_factory, rid).cancel_requested is False


# --- post-QA fixes: guarded writes, cancel atomics, stale finder -------------------------------------------------


def test_failure_does_not_overwrite_terminal_status(session_factory, session, monkeypatch, caplog):
    run_id = _new(session)

    def engine(config, **kw):
        _set(session_factory, run_id, status="cancelled")
        raise RuntimeError("boom")

    monkeypatch.setattr(worker, "run_scenario", engine)
    with caplog.at_level(logging.INFO, logger="microgrid_api.worker"):
        worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    assert row.status == "cancelled" and row.error_message is None and row.result is None
    assert "already terminal" in caplog.text


def test_cancel_does_not_overwrite_succeeded(session_factory, session, monkeypatch):
    run_id = _new(session)

    def engine(config, **kw):
        _set(session_factory, run_id, status="succeeded", result={"ok": True})
        raise worker.RunCancelled()

    monkeypatch.setattr(worker, "run_scenario", engine)
    worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    assert row.status == "succeeded" and row.result == {"ok": True}


def test_late_progress_does_not_resurrect_terminal_run(session_factory, session, monkeypatch):
    run_id = _new(session)

    def engine(config, *, progress=None, **kw):
        _set(session_factory, run_id, status="cancelled")
        progress(_ev())
        return _Result()

    monkeypatch.setattr(worker, "run_scenario", engine)
    worker.execute_run(run_id, session_factory)
    row = _row(session_factory, run_id)
    assert row.status == "cancelled" and row.result is None and not row.progress


def test_request_cancel_queued_running_terminal_unknown(session_factory, session):
    q = _new(session)
    assert svc.request_cancel(session, q).status == "cancelled"
    assert _row(session_factory, q).finished_at is not None
    r = _new(session)
    _set(session_factory, r, status="running")
    got = svc.request_cancel(session, r)
    assert got.status == "running" and got.cancel_requested is True
    t = _new(session)
    _set(session_factory, t, status="failed")
    got = svc.request_cancel(session, t)
    assert got.status == "failed" and got.cancel_requested is False
    with pytest.raises(LookupError):
        svc.request_cancel(session, uuid.uuid4())


def test_find_stale_running_returns_only_old_running_ids_and_writes_nothing(session_factory, session):
    from datetime import UTC, datetime, timedelta

    old, fresh, done, queued = (_new(session) for _ in range(4))
    now = datetime.now(UTC)
    _set(session_factory, old, status="running", started_at=now - timedelta(hours=3))
    _set(session_factory, fresh, status="running", started_at=now - timedelta(seconds=5))
    _set(session_factory, done, status="succeeded", started_at=now - timedelta(hours=3))
    assert worker.find_stale_running(session, 3600) == [old]
    assert _row(session_factory, old).status == "running"
    with pytest.raises(ValueError):
        worker.find_stale_running(session, 0)
    assert queued not in worker.find_stale_running(session, 1)
