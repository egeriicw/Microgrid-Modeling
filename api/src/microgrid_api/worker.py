"""Celery app and the task that runs the engine in the background (FR-PLAT-2).

Start a worker with ``celery -A microgrid_api.worker worker`` (needs MICROGRID_REDIS_URL and MICROGRID_DATABASE_URL).

Design:
* All run state lives in the database (``runs``, ``run_events``); there is no Celery results backend.
* The task is ``acks_late`` (a task lost with a crashed worker is redelivered) but never retried on engine failure.
  A redelivered task is a no-op unless the run is still ``queued`` (atomic claim), so a run cannot execute twice.
  Known limit: a worker that dies mid-run leaves the row ``running`` (a reaper is future work).
* The engine is reached only through the facade ``run_scenario`` (FR-ENG-8).
* Failure messages stored on the row are a safe summary (exception class only); the traceback goes to the log.
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from celery import Celery
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session, sessionmaker

from microgrid.community_load.synthetic.run import ProgressEvent, RunCancelled, run_scenario
from microgrid_api.config import LOCAL_ENVIRONMENTS, ConfigurationError, Settings
from microgrid_api.db.models import Run, RunEvent, RunStatus
from microgrid_api.services import runs as svc

logger = logging.getLogger("microgrid_api.worker")

TASK_NAME = "microgrid.run_engine"
SessionFactory = Callable[[], Session]


def _now() -> datetime:
    return datetime.now(UTC)


# --- execution ---------------------------------------------------------------------------------------------------

_settings_override: Settings | None = None  # tests inject Settings here
_factory_cache: dict[str, SessionFactory] = {}
_factory_lock = threading.Lock()


def _session_factory() -> SessionFactory:
    settings = _settings_override or Settings()
    url = settings.require_database_url()
    with _factory_lock:
        if url not in _factory_cache:
            _factory_cache[url] = sessionmaker(create_engine(url, pool_pre_ping=True), expire_on_commit=False)
        return _factory_cache[url]


def _safe_error(exc: BaseException) -> str:
    """Exception class name only: raw messages can carry paths, connection strings or credentials."""
    return f"engine run failed ({type(exc).__name__}); see worker logs"


def _finish(session: Session, rid: uuid.UUID, status: RunStatus, **fields: Any) -> None:
    values = {k: v for k, v in fields.items() if v is not None}
    session.execute(update(Run).where(Run.id == rid).values(status=status.value, finished_at=_now(), **values))
    session.commit()


def execute_run(run_id: uuid.UUID | str, session_factory: SessionFactory) -> None:
    """Execute one run. Never raises for engine problems (the outcome is recorded on the row); raises LookupError
    for an unknown id. Cancellation latency is one engine run (see services/runs.py)."""
    rid = uuid.UUID(str(run_id))
    with session_factory() as session:
        # Atomic claim: only a queued run starts. Duplicate/late deliveries and cancelled runs are no-ops.
        claimed = session.execute(
            update(Run)
            .where(Run.id == rid, Run.status == RunStatus.queued.value)
            .values(status=RunStatus.running.value, started_at=_now())
        )
        session.commit()
        run = session.get(Run, rid)
        if run is None:
            raise LookupError(f"run {rid} not found")
        if getattr(claimed, "rowcount", 0) != 1:
            logger.info("run %s not started: status is %s", rid, run.status)
            return
        config = {**run.composition_snapshot, "rng": dict(run.seeds)}  # the recorded seeds are authoritative

        def should_cancel() -> bool:
            with session_factory() as poll:  # fresh connection: always sees the committed flag
                flag = poll.scalar(select(Run.cancel_requested).where(Run.id == rid))
            return bool(flag)

        def on_progress(ev: ProgressEvent) -> None:
            payload: dict[str, Any] = asdict(ev)
            run.progress = payload
            session.add(RunEvent(run_id=rid, stage=ev.stage, message=ev.message[:1000], payload=payload))
            session.commit()

        try:
            data = run_scenario(config, progress=on_progress, should_cancel=should_cancel).to_dict()
        except RunCancelled:
            session.rollback()
            _finish(session, rid, RunStatus.cancelled)
        except Exception as exc:
            logger.exception("run %s failed", rid)
            session.rollback()
            _finish(session, rid, RunStatus.failed, error_message=_safe_error(exc))
        else:
            _finish(
                session,
                rid,
                RunStatus.succeeded,
                result=data,
                seeds=data.get("seeds") or None,
                engine_version=str(data.get("engine_version") or "") or None,
            )


def _run_engine_task_body(run_id: str) -> None:
    execute_run(run_id, _session_factory())


# --- Celery app --------------------------------------------------------------------------------------------------


def make_celery(settings: Settings) -> Celery:
    """Build the Celery app. The broker is MICROGRID_REDIS_URL; without it only development/test may run (an
    in-memory broker, useful for eager tests); any other environment fails loudly. Nothing connects at import."""
    broker = settings.redis_url
    if not broker:
        if settings.environment not in LOCAL_ENVIRONMENTS:
            raise ConfigurationError(
                f"MICROGRID_REDIS_URL is required for the worker when MICROGRID_ENVIRONMENT is "
                f"'{settings.environment}'."
            )
        broker = "memory://"
    app = Celery("microgrid_api", broker=broker, set_as_current=False)
    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        result_backend=None,
        task_ignore_result=True,
        task_acks_late=True,
        task_time_limit=settings.task_time_limit_s,
        task_soft_time_limit=settings.task_soft_time_limit_s,
        worker_prefetch_multiplier=1,  # long tasks: do not reserve work another worker could take
        broker_connection_retry_on_startup=True,
        task_default_queue="microgrid",
    )
    app.task(name=TASK_NAME, max_retries=0)(_run_engine_task_body)  # no autoretry: engine failures are final
    return app


celery_app = make_celery(Settings())
app = celery_app  # `celery -A microgrid_api.worker` finds either name
run_engine_task = celery_app.tasks[TASK_NAME]


def enqueue_run(run_id: uuid.UUID | str) -> None:
    """Queue the run. The Celery task id equals the run id, so the task can be revoked by run id."""
    run_engine_task.apply_async(args=[str(run_id)], task_id=str(run_id))


def cancel_run(session: Session, run_id: uuid.UUID | str) -> Run:
    """Set the database cancel flag (authoritative) and best-effort revoke the queued task. A broker outage never
    fails the cancel and its message is not logged (it could contain the broker URL)."""
    rid = uuid.UUID(str(run_id))
    run = svc.request_cancel(session, rid)
    try:
        celery_app.control.revoke(str(rid), terminate=False)
    except Exception as exc:
        logger.warning("revoke for run %s failed (%s); database flag still set", rid, type(exc).__name__)
    return run
