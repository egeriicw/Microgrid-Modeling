"""Aggregates all v1 routers. Add new modules here (auth, compositions, runs, ... in later stories)."""

from __future__ import annotations

from fastapi import APIRouter

from microgrid_api.routers import health, runs

router = APIRouter()
router.include_router(health.router)
router.include_router(runs.router)
