"""Pydantic schemas for the API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ConfigCreate(BaseModel):
    name: str = Field(..., min_length=1)
    yaml_text: str = Field(..., min_length=1)


class ConfigUpdate(BaseModel):
    name: str | None = None
    yaml_text: str | None = None


class ConfigOut(BaseModel):
    id: int
    name: str
    yaml_text: str
    created_at: datetime
    updated_at: datetime
