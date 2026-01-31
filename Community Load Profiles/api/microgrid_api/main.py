"""FastAPI entrypoint."""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from .db import SessionLocal, get_db
from .models import Config, Run
from .run_worker import worker
import uuid

from .schemas import ConfigCreate, ConfigOut, ConfigUpdate

app = FastAPI(title="Microgrid API", version="0.1.0")


@app.on_event("startup")
def _startup() -> None:
    # Start in-process worker.
    worker.start(SessionLocal)


@app.get("/health")
def health() -> dict[str, str]:
    """Health check endpoint."""

    return {"status": "ok"}


@app.get("/configs", response_model=list[ConfigOut])
def list_configs(db: Session = Depends(get_db)) -> list[Config]:
    """List stored configs."""

    return db.query(Config).order_by(Config.id.desc()).all()


@app.post("/configs", response_model=ConfigOut)
def create_config(payload: ConfigCreate, db: Session = Depends(get_db)) -> Config:
    """Create a new config record."""

    cfg = Config(name=payload.name, yaml_text=payload.yaml_text)
    db.add(cfg)
    db.commit()
    db.refresh(cfg)
    return cfg


@app.post("/runs")
def create_run(payload: dict, db: Session = Depends(get_db)) -> dict:
    """Create a run from a config and enqueue it."""

    from .run_schemas import RunCreate

    rc = RunCreate(**payload)
    cfg = db.get(Config, rc.config_id)
    if cfg is None:
        raise HTTPException(status_code=404, detail="config not found")

    run_id = str(uuid.uuid4())
    run = Run(run_id=run_id, config_id=cfg.id, status="queued")
    # store yaml on run object by looking up config on execute; for now we duplicate
    run.log_text = ""
    db.add(run)
    db.commit()

    return {"run_id": run_id, "status": "queued"}


@app.get("/runs")
def list_runs(active: bool = False, db: Session = Depends(get_db)) -> list[dict]:
    q = db.query(Run)
    if active:
        q = q.filter(Run.status.in_(["queued", "running"]))
    runs = q.order_by(Run.created_at.desc()).all()
    return [
        {
            "id": r.run_id,
            "status": r.status,
            "progressCurrent": r.progress_current,
            "progressTotal": r.progress_total,
            "progressMessage": r.progress_message,
        }
        for r in runs
    ]


@app.get("/runs/{run_id}")
def get_run(run_id: str, db: Session = Depends(get_db)) -> dict:
    r = db.get(Run, run_id)
    if r is None:
        raise HTTPException(status_code=404, detail="run not found")
    return {
        "run_id": r.run_id,
        "config_id": r.config_id,
        "status": r.status,
        "progress_current": r.progress_current,
        "progress_total": r.progress_total,
        "progress_message": r.progress_message,
        "error_message": r.error_message,
        "log_text": r.log_text,
    }


@app.get("/configs/{config_id}", response_model=ConfigOut)
def get_config(config_id: int, db: Session = Depends(get_db)) -> Config:
    """Get a config by id."""

    cfg = db.get(Config, config_id)
    if cfg is None:
        raise HTTPException(status_code=404, detail="config not found")
    return cfg


@app.put("/configs/{config_id}", response_model=ConfigOut)
def update_config(config_id: int, payload: ConfigUpdate, db: Session = Depends(get_db)) -> Config:
    """Update a config record."""

    cfg = db.get(Config, config_id)
    if cfg is None:
        raise HTTPException(status_code=404, detail="config not found")

    if payload.name is not None:
        cfg.name = payload.name
    if payload.yaml_text is not None:
        cfg.yaml_text = payload.yaml_text

    db.add(cfg)
    db.commit()
    db.refresh(cfg)
    return cfg
