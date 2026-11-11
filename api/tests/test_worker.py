"""FR-PLAT-2: Celery worker task runs the engine in the background (story 1.10).

No broker is needed: the service functions and `execute_run` are called directly, and one test enqueues through
Celery with `task_always_eager`. The engine facade is stubbed for control-flow tests and real (synthetic fixture
data) for end-to-end ones. Real Redis is NOT used anywhere in this file.
"""

from __future__ import annotations

import json
import logging
import uuid

import pytest
from microgrid.community_load.synthetic.run import ProgressEvent, RunCancelled
from sqlalchemy import select

from microgrid_api import worker
from microgrid_api.config import ConfigurationError, Settings
from microgrid_api.db.models import Run, RunEvent, RunStatus
from microgrid_api.services import runs as svc

SECRET = "S3CRETpw"


class FakeResult:
    def to_dict(self, *, include_results: bool = True):
        return {
            "engine_version": "9.9.9",
            "seeds": {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3},
            "config_snapshot": {"scenario": {"name": "x"}},
            "metrics": [{"run_index": 0, "peak_kw": 1.5}],
            "converged": True,
            "n_attempted": 2,
            "n_rejected": 0,
            "diversity_metrics": {},
        }


def _ev(stage, i, att, feas, converged=None, msg="m"):
    return ProgressEvent(stage, i, att, feas, 2, converged, msg)


def _new_run(session, config=None, seeds=None) -> uuid.UUID:
    return svc.create_run(session, config or {"scenario": {"name": "x"}}, seeds=seeds, validate=False).id


@pytest.fixture
def stub_engine(monkeypatch):
    """Replace the facade with a scripted stub; returns a dict of captured calls."""
    calls: dict = {}

    def fake(config, *, progress=None, should_cancel=None, **kw):
        calls["config"] = config
        calls["kw"] = kw
        progress(_ev("building_pool", -1, 0, 0))
        progress(_ev("running", 0, 1, 1))
        progress(_ev("running", 1, 2, 1))
        progress(_ev("checking_convergence", 1, 2, 1, True))
        progress(_ev("finished", 1, 2, 1, True))
        return FakeResult()

    monkeypatch.setattr(worker, "run_scenario", fake)
    return calls


# --- lifecycle ---------------------------------------------------------------------------------------------------


def test_status_transitions_queued_running_succeeded(session_factory, session, monkeypatch):
    run_id = _new_run(session)
    assert session.get(Run, run_id).status == "queued"
    seen: list[str] = []

    def fake(config, *, progress=None, should_cancel=None, **kw):
        with session_factory() as s2:  # a second connection sees the committed state mid-run
            r = s2.get(Run, run_id)
            seen.append(r.status)
            assert r.started_at is not None and r.finished_at is None
        return FakeResult()

    monkeypatch.setattr(worker, "run_scenario", fake)
    worker.execute_run(run_id, session_factory)
    assert seen == ["running"]
    session.expire_all()
    run = session.get(Run, run_id)
    assert run.status == RunStatus.succeeded and run.finished_at is not None and run.error_message is None


def test_progress_rows_in_order_and_progress_json(session_factory, session, stub_engine):
    run_id = _new_run(session)
    worker.execute_run(run_id, session_factory)
    session.expire_all()
    events = session.scalars(select(RunEvent).where(RunEvent.run_id == run_id).order_by(RunEvent.id)).all()
    assert [e.stage for e in events] == ["building_pool", "running", "running", "checking_convergence", "finished"]
    last = events[-1].payload
    assert last["n_attempted"] == 2 and last["n_feasible"] == 1 and last["converged"] is True
    assert events[1].message == "m" and events[1].payload["run_index"] == 0
    prog = session.get(Run, run_id).progress
    assert prog["stage"] == "finished" and prog["n_attempted"] == 2 and prog["max_runs"] == 2


def test_result_stored_json_serializable_and_complete(session_factory, session, stub_engine):
    run_id = _new_run(session)
    worker.execute_run(run_id, session_factory)
    session.expire_all()
    run = session.get(Run, run_id)
    assert json.loads(json.dumps(run.result)) == run.result
    assert run.result["metrics"] and run.result["engine_version"] == "9.9.9" and run.result["seeds"]
    assert run.engine_version == "9.9.9"


def test_engine_receives_snapshot_with_recorded_seeds(session_factory, session, stub_engine):
    run_id = _new_run(session, seeds={"composition_seed": 11})
    seeds = session.get(Run, run_id).seeds
    worker.execute_run(run_id, session_factory)
    assert stub_engine["config"]["rng"] == seeds
    assert seeds["composition_seed"] == 11 and isinstance(seeds["archetype_seed"], int)


def test_not_queued_run_is_not_executed_twice(session_factory, session, stub_engine):
    run_id = _new_run(session)
    worker.execute_run(run_id, session_factory)
    stub_engine.clear()
    worker.execute_run(run_id, session_factory)  # duplicate delivery (acks_late redelivery)
    assert stub_engine == {}


def test_unknown_run_id_fails_loudly(session_factory):
    with pytest.raises(LookupError):
        worker.execute_run(uuid.uuid4(), session_factory)


# --- failure -----------------------------------------------------------------------------------------------------


def test_failed_run_stores_safe_message_and_logs_traceback(session_factory, session, monkeypatch, caplog):
    def boom(*a, **k):
        raise RuntimeError(f"could not connect postgresql://bob:{SECRET}@db.internal/x password={SECRET}")

    monkeypatch.setattr(worker, "run_scenario", boom)
    run_id = _new_run(session)
    with caplog.at_level(logging.ERROR, logger="microgrid_api.worker"):
        worker.execute_run(run_id, session_factory)  # must not raise: state lives in the DB, no retry
    session.expire_all()
    run = session.get(Run, run_id)
    assert run.status == RunStatus.failed and run.finished_at is not None and run.result is None
    assert "RuntimeError" in run.error_message
    assert SECRET not in run.error_message and "bob" not in run.error_message and "postgresql" not in run.error_message
    assert any(r.exc_info for r in caplog.records)  # traceback went to the log, not the row


def test_failure_after_progress_keeps_no_final_result(session_factory, session, monkeypatch):
    def fake(config, *, progress=None, **kw):
        progress(_ev("running", 0, 1, 1))
        raise ValueError("bad")

    monkeypatch.setattr(worker, "run_scenario", fake)
    run_id = _new_run(session)
    worker.execute_run(run_id, session_factory)
    session.expire_all()
    run = session.get(Run, run_id)
    assert run.status == "failed" and run.result is None


# --- cancellation ------------------------------------------------------------------------------------------------


def test_cancel_before_start_never_calls_engine(session_factory, session, stub_engine):
    run_id = _new_run(session)
    svc.request_cancel(session, run_id)
    run = svc.get_run(session, run_id)
    assert run.cancel_requested is True and run.status == "cancelled" and run.finished_at is not None
    worker.execute_run(run_id, session_factory)  # a task still delivered must be a no-op
    assert stub_engine == {}
    session.expire_all()
    assert session.get(Run, run_id).status == "cancelled" and session.get(Run, run_id).result is None


def test_flag_set_mid_run_is_seen_by_should_cancel(session_factory, session, monkeypatch):
    """The flag is read from the database on every poll, so a different process can set it (and it survives a
    worker restart)."""
    run_id = _new_run(session)

    def fake(config, *, progress=None, should_cancel=None, **kw):
        progress(_ev("running", 0, 1, 1))
        assert should_cancel() is False
        svc.request_cancel(session, run_id)  # another process flips the flag mid-run
        assert should_cancel() is True
        raise RunCancelled(1, 1)

    monkeypatch.setattr(worker, "run_scenario", fake)
    worker.execute_run(run_id, session_factory)
    session.expire_all()
    run = session.get(Run, run_id)
    assert run.status == RunStatus.cancelled and run.finished_at is not None
    assert run.result is None and run.error_message is None


def test_request_cancel_on_running_run_keeps_status_running_until_worker_stops(session, session_factory):
    run_id = _new_run(session)
    run = session.get(Run, run_id)
    run.status = "running"
    session.commit()
    svc.request_cancel(session, run_id)
    session.expire_all()
    run = session.get(Run, run_id)
    assert run.cancel_requested is True and run.status == "running" and run.finished_at is None


def test_request_cancel_on_finished_run_is_noop(session_factory, session, stub_engine):
    run_id = _new_run(session)
    worker.execute_run(run_id, session_factory)
    svc.request_cancel(session, run_id)
    session.expire_all()
    run = session.get(Run, run_id)
    assert run.status == "succeeded" and run.cancel_requested is False


def test_request_cancel_unknown_run(session):
    with pytest.raises(LookupError):
        svc.request_cancel(session, uuid.uuid4())


def test_real_engine_cancel_during_run_stops_before_next_run(session_factory, session, tiny_config, monkeypatch):
    """Real facade on the synthetic dataset: cancel is requested after the 2nd run completes; the engine polls
    before each run, so nothing runs after the flag is seen (latency = one engine run)."""
    run_id = svc.create_run(session, tiny_config, trusted_data_paths=True).id
    real = worker.run_scenario
    done: list[int] = []

    def wrapped(config, *, progress=None, **kw):
        def on_progress(ev):
            if ev.stage == "running":
                done.append(ev.run_index)
                if len(done) == 2:
                    svc.request_cancel(session, run_id)
            progress(ev)

        return real(config, progress=on_progress, **kw)

    monkeypatch.setattr(worker, "run_scenario", wrapped)
    worker.execute_run(run_id, session_factory)
    session.expire_all()
    run = session.get(Run, run_id)
    assert run.status == "cancelled" and run.result is None
    assert len(done) == 2  # 6 were configured


# --- real engine, seeds, replay ----------------------------------------------------------------------------------


def test_create_run_records_resolved_seeds_before_execution(session, tiny_config):
    run = svc.create_run(session, tiny_config, trusted_data_paths=True)
    assert run.status == "queued" and run.started_at is None
    assert set(run.seeds) == {"composition_seed", "archetype_seed", "perturbation_seed"}
    assert all(isinstance(v, int) for v in run.seeds.values())
    assert run.composition_snapshot["rng"] == run.seeds
    assert "rng" not in tiny_config  # caller's dict is not mutated
    assert run.engine_version


def test_create_run_keeps_fixed_seeds_and_rejects_unknown_seed_keys(session, tiny_config):
    fixed = {"composition_seed": 5, "archetype_seed": 6, "perturbation_seed": 7}
    assert svc.create_run(session, tiny_config, seeds=fixed, trusted_data_paths=True).seeds == fixed
    with pytest.raises(ValueError):
        svc.create_run(session, tiny_config, seeds={"nope": 1})


def test_create_run_rejects_invalid_config_loudly(session):
    with pytest.raises(ValueError, match="ensemble.mode"):
        svc.create_run(session, {"ensemble": {"mode": "not-a-mode"}})


def test_end_to_end_real_engine_and_replay(session_factory, session, tiny_config, settings, tmp_path, monkeypatch):
    monkeypatch.setattr(worker, "_settings_override", settings.model_copy(update={"output_dir": tmp_path / "out"}))
    tiny_config["output"] = {"base_dir": str(tmp_path / "out" / "run1")}
    first = svc.create_run(session, tiny_config, trusted_data_paths=True)
    worker.execute_run(first.id, session_factory)
    session.expire_all()
    first = session.get(Run, first.id)
    assert first.status == "succeeded"
    assert first.result["seeds"] == first.seeds  # seeds recorded before execution are the ones actually used
    assert 1 <= len(first.result["metrics"]) <= 6
    json.dumps(first.result)
    assert "results" not in first.result  # the runs table keeps the compact summary only
    assert "results" in json.loads((tmp_path / "out" / "run1" / "results.json").read_text())

    # The run's own stored snapshot (config + recorded seeds) replays it. NOTE: result["config_snapshot"] is the
    # engine's asdict(cfg) dump, which config_from_dict rejects (top-level name/description, flattened feasibility).
    replay = svc.create_run(session, first.composition_snapshot, trusted_data_paths=True)
    assert replay.seeds == first.seeds
    worker.execute_run(replay.id, session_factory)
    session.expire_all()
    assert session.get(Run, replay.id).result["metrics"] == first.result["metrics"]
    stages = [
        e.stage for e in session.scalars(select(RunEvent).where(RunEvent.run_id == first.id).order_by(RunEvent.id))
    ]
    assert stages[0] == "building_pool" and stages[-1] == "finished" and stages.count("running") == 6


# --- celery app --------------------------------------------------------------------------------------------------


def test_celery_app_imports_and_configures_without_broker():
    app = worker.celery_app
    assert app.conf.task_serializer == "json" and list(app.conf.accept_content) == ["json"]
    assert app.conf.result_serializer == "json"
    assert app.conf.task_acks_late is True
    assert app.conf.task_time_limit == 3600 and app.conf.task_soft_time_limit == 3300
    assert not app.conf.result_backend
    task = app.tasks["microgrid.run_engine"]
    assert not getattr(task, "autoretry_for", ())  # engine failures are never retried


def test_time_limits_come_from_settings():
    s = Settings(environment="test", task_time_limit_s=900, task_soft_time_limit_s=800)
    app = worker.make_celery(s)
    assert app.conf.task_time_limit == 900 and app.conf.task_soft_time_limit == 800


def test_soft_limit_must_be_below_hard_limit():
    with pytest.raises(ConfigurationError):
        Settings(environment="test", task_time_limit_s=900, task_soft_time_limit_s=900)


def test_make_celery_requires_redis_url_outside_development():
    s = Settings(
        environment="production",
        database_url="postgresql+psycopg://u@h/d",
        jwt_secret="x" * 32,
        api_keys=[{"subject": "a", "role": "admin", "key": "k" * 20}],
    )
    with pytest.raises(ConfigurationError) as ei:
        worker.make_celery(s)
    assert "MICROGRID_REDIS_URL" in str(ei.value)


def test_make_celery_uses_redis_url_as_broker_without_logging_it(caplog):
    url = f"redis://:{SECRET}@127.0.0.1:1/0"
    with caplog.at_level(logging.DEBUG):
        app = worker.make_celery(Settings(environment="test", redis_url=url))
    assert app.conf.broker_url == url
    assert SECRET not in caplog.text
    assert SECRET not in repr(Settings(environment="test", redis_url=url))


def test_enqueue_run_through_celery_eager(settings, session, stub_engine, monkeypatch):
    monkeypatch.setattr(worker, "_settings_override", settings)
    monkeypatch.setattr(worker, "_factory_cache", {})
    monkeypatch.setitem(worker.celery_app.conf, "task_always_eager", True)
    run_id = _new_run(session)
    worker.enqueue_run(run_id)
    session.expire_all()
    assert session.get(Run, run_id).status == "succeeded"


def test_enqueue_run_uses_run_id_as_task_id_so_it_can_be_revoked(monkeypatch):
    sent = {}

    class T:
        def apply_async(self, args, task_id):
            sent.update(args=args, task_id=task_id)

    monkeypatch.setattr(worker, "run_engine_task", T())
    rid = uuid.uuid4()
    worker.enqueue_run(rid)
    assert sent == {"args": [str(rid)], "task_id": str(rid)}


def test_cancel_run_sets_flag_and_revokes_best_effort(session, monkeypatch):
    revoked = []
    monkeypatch.setattr(worker.celery_app.control, "revoke", lambda tid, **k: revoked.append((tid, k)))
    run_id = _new_run(session)
    worker.cancel_run(session, run_id)
    assert svc.get_run(session, run_id).status == "cancelled"
    assert revoked and revoked[0][0] == str(run_id) and not revoked[0][1].get("terminate")


def test_cancel_run_survives_broker_outage(session, monkeypatch, caplog):
    def down(*a, **k):
        raise ConnectionError(f"redis://:{SECRET}@x")

    monkeypatch.setattr(worker.celery_app.control, "revoke", down)
    run_id = _new_run(session)
    with caplog.at_level(logging.DEBUG):
        worker.cancel_run(session, run_id)  # the DB flag is authoritative; broker failure must not raise or leak
    assert svc.get_run(session, run_id).cancel_requested is True
    assert SECRET not in caplog.text
