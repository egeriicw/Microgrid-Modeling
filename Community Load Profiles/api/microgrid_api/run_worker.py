"""Background run worker.

This PR introduces a minimal in-process worker that executes queued runs.
It is intentionally simple (no external queue) and can be replaced later.

The worker supports a mock mode (MICROGRID_RUNNER_MODE=mock) for tests.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
import uuid
from pathlib import Path

from sqlalchemy.orm import Session

from .models import Config, Run


def _run_subprocess(run: Run, db: Session) -> None:
    root = Path(__file__).resolve().parents[2]  # .../Community Load Profiles/api
    clp_root = root.parent  # Community Load Profiles

    run_dir = clp_root / "data" / "output" / "scenario_runs" / run.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    cfg = db.get(Config, run.config_id)
    if cfg is None:
        raise RuntimeError(f"Missing config_id={run.config_id} for run {run.run_id}")

    cfg_path = run_dir / "config.yaml"
    cfg_path.write_text(cfg.yaml_text, encoding="utf-8")

    cmd = [
        "python3",
        str(clp_root / "scripts" / "run_pipeline.py"),
        "--config",
        str(cfg_path),
    ]

    env = os.environ.copy()
    env["PYTHONPATH"] = str(clp_root / "src") + os.pathsep + env.get("PYTHONPATH", "")

    p = subprocess.Popen(
        cmd,
        cwd=str(clp_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )

    assert p.stdout is not None
    for line in p.stdout:
        run.log_text = (run.log_text or "") + line
        run.progress_message = line.strip()[:500] if line.strip() else run.progress_message
        db.add(run)
        db.commit()

    rc = p.wait()
    if rc == 0:
        run.status = "succeeded"
    else:
        run.status = "failed"
        run.error_message = f"pipeline exited with code {rc}"


def run_once(run_id: str, db: Session) -> None:
    run = db.get(Run, run_id)
    if run is None:
        return

    run.status = "running"
    run.started_at = int(time.time())
    db.add(run)
    db.commit()

    mode = os.environ.get("MICROGRID_RUNNER_MODE", "subprocess")
    try:
        if mode == "mock":
            # Fast deterministic completion for tests.
            for i in range(3):
                run.progress_current = i + 1
                run.progress_total = 3
                run.progress_message = f"mock step {i+1}/3"
                db.add(run)
                db.commit()
                time.sleep(0.01)
            run.status = "succeeded"
        else:
            _run_subprocess(run, db)
    except Exception as e:
        run.status = "failed"
        run.error_message = str(e)
    finally:
        run.finished_at = int(time.time())
        db.add(run)
        db.commit()


class Worker:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, session_factory) -> None:
        if self._thread is not None:
            return

        def loop() -> None:
            while not self._stop.is_set():
                with session_factory() as db:
                    run = (
                        db.query(Run)
                        .filter(Run.status == "queued")
                        .order_by(Run.created_at.asc())
                        .first()
                    )
                    if run is None:
                        time.sleep(0.5)
                        continue

                    run_once(run.run_id, db)

        self._thread = threading.Thread(target=loop, name="microgrid-runner", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()


worker = Worker()
