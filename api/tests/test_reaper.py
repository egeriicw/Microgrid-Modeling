"""Story A2: stuck-run reaper."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from microgrid_api import reaper as reaper_cli
from microgrid_api import worker
from microgrid_api.config import Settings
from microgrid_api.db.models import Run, RunStatus
from microgrid_api.services import runs as svc


def _mk(session, tiny_config, status, started_ago_s=None):
    run = svc.create_run(session, tiny_config, trusted_data_paths=True)
    run.status = status.value
    if started_ago_s is not None:
        run.started_at = datetime.now(UTC) - timedelta(seconds=started_ago_s)
    session.commit()
    return run.id


def test_reaps_only_stale_running_rows(session, tiny_config):
    stale = _mk(session, tiny_config, RunStatus.running, 5000)
    young = _mk(session, tiny_config, RunStatus.running, 10)
    queued = _mk(session, tiny_config, RunStatus.queued)
    done = _mk(session, tiny_config, RunStatus.succeeded, 9000)
    reaped = worker.reap_stale_runs(session, older_than_seconds=3600)
    assert reaped == [stale]
    session.expire_all()
    row = session.get(Run, stale)
    assert row.status == "failed" and row.finished_at is not None
    assert "time limit" in row.error_message
    assert session.get(Run, young).status == "running"
    assert session.get(Run, queued).status == "queued"
    assert session.get(Run, done).status == "succeeded"


def test_does_not_overwrite_a_run_that_finished_meanwhile(session, tiny_config, monkeypatch):
    rid = _mk(session, tiny_config, RunStatus.succeeded, 9000)
    monkeypatch.setattr(worker, "find_stale_running", lambda s, n: [rid])
    assert worker.reap_stale_runs(session, older_than_seconds=60) == []
    session.expire_all()
    assert session.get(Run, rid).status == "succeeded"


def test_threshold_is_hard_limit_plus_margin():
    s = Settings(environment="test", task_time_limit_s=3600, reaper_margin_s=300)
    assert worker.reaper_threshold_s(s) == 3900


def test_reap_once_uses_threshold(session_factory, session, tiny_config):
    s = Settings(environment="test", task_time_limit_s=600, task_soft_time_limit_s=500, reaper_margin_s=60)
    old = _mk(session, tiny_config, RunStatus.running, 700)
    keep = _mk(session, tiny_config, RunStatus.running, 500)
    assert worker.reap_once(s, session_factory) == [old]
    session.expire_all()
    assert session.get(Run, keep).status == "running"


def test_rejects_nonpositive_age(session):
    with pytest.raises(ValueError):
        worker.reap_stale_runs(session, older_than_seconds=0)


def test_celery_task_and_beat_schedule_registered():
    s = Settings(environment="test")
    app = worker.make_celery(s)
    assert worker.REAPER_TASK_NAME in app.tasks
    sched = app.conf.beat_schedule["reap-stale-runs"]
    assert sched["task"] == worker.REAPER_TASK_NAME
    assert sched["schedule"] == s.reaper_interval_s


def test_cli_reports_count(monkeypatch, capsys, settings, session, tiny_config):
    _mk(session, tiny_config, RunStatus.running, 10**6)
    monkeypatch.setattr(worker, "_settings_override", settings)
    assert reaper_cli.main([]) == 0
    assert "reaped 1" in capsys.readouterr().err
