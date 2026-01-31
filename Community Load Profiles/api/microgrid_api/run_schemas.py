"""Schemas for run API."""

from __future__ import annotations

from pydantic import BaseModel


class RunCreate(BaseModel):
    config_id: int


class RunOut(BaseModel):
    run_id: str
    config_id: int
    status: str
    progress_current: int | None = None
    progress_total: int | None = None
    progress_message: str | None = None
    error_message: str | None = None
    log_text: str | None = None
