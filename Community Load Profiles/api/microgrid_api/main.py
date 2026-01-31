"""FastAPI entrypoint."""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from .db import get_db
from .models import Config
from .schemas import ConfigCreate, ConfigOut, ConfigUpdate

app = FastAPI(title="Microgrid API", version="0.1.0")


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
