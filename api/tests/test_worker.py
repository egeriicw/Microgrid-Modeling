"""FR-A13 — RQ worker executes ensembles, streams progress, uploads
artifacts, honours cancellation. RED placeholder (test-plan.md §4 Phase 3).
"""
from __future__ import annotations

import pytest

pytest.importorskip("microgrid_api", reason="Phase 3: worker not implemented")

pytestmark = pytest.mark.xfail(reason="FR-A13 not implemented", strict=False)


@pytest.fixture
def mock_mode(monkeypatch):
    monkeypatch.setenv("MICROGRID_RUNNER_MODE", "mock")


def _make_queued_run(db) -> str:
    from microgrid_api.models import Config, Run
    import uuid

    cfg = Config(name="w", version=1, body={"name": "w", "ensemble": {"max_runs": 6}})
    db.add(cfg)
    db.flush()
    run = Run(
        id=str(uuid.uuid4()), config_id=cfg.id, config_version=1, status="queued",
        config_snapshot=cfg.body, seeds={"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3},
        engine_version="0.0.0-test",
    )
    db.add(run)
    db.commit()
    return run.id


def test_mock_run_succeeds_and_publishes_progress(db, storage, mock_mode):
    from microgrid_api.models import Run, RunMetric
    from microgrid_api.worker.tasks import run_ensemble_task

    run_id = _make_queued_run(db)
    run_ensemble_task(run_id)

    run = db.get(Run, run_id)
    assert run.status == "succeeded"
    assert run.finished_at is not None
    assert run.summary and run.summary["n_attempted"] >= 1
    assert db.query(RunMetric).filter_by(run_id=run_id).count() >= 1


def test_engine_exception_marks_failed_with_partial_artifacts(db, storage, monkeypatch):
    from microgrid_api.models import Artifact, Run
    from microgrid_api.worker import runner
    from microgrid_api.worker.tasks import run_ensemble_task

    def boom(*a, **k):
        raise RuntimeError("engine blew up")

    monkeypatch.setattr(runner, "run_scenario", boom)
    run_id = _make_queued_run(db)
    run_ensemble_task(run_id)

    run = db.get(Run, run_id)
    assert run.status == "failed"
    assert "engine blew up" in run.error_message
    for art in db.query(Artifact).filter_by(run_id=run_id):
        assert art.partial is True


def test_cooperative_cancel_stops_within_a_few_iterations(db, storage, mock_mode):
    from microgrid_api.models import Run, RunMetric
    from microgrid_api.worker.cancellation import request_cancel
    from microgrid_api.worker.tasks import run_ensemble_task

    run_id = _make_queued_run(db)
    request_cancel(run_id)  # flag set before execution
    run_ensemble_task(run_id)

    run = db.get(Run, run_id)
    assert run.status == "cancelled"
    assert db.query(RunMetric).filter_by(run_id=run_id).count() < 6


def test_concurrent_pickup_no_double_run(db, storage, mock_mode):
    """UNIQUE(run_id, run_index) must prevent two workers double-processing."""
    from rq import SimpleWorker

    from microgrid_api.models import RunMetric
    from microgrid_api.queue import get_queue

    run_ids = [_make_queued_run(db) for _ in range(3)]
    q = get_queue()
    for rid in run_ids:
        q.enqueue("microgrid_api.worker.tasks.run_ensemble_task", rid)

    SimpleWorker([q], connection=q.connection).work(burst=True)

    for rid in run_ids:
        # exactly max_runs metric rows, never doubled
        assert db.query(RunMetric).filter_by(run_id=rid).count() == 6
