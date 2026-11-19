"""Aggregates all v1 routers. Add new modules here (auth, compositions, runs, ... in later stories)."""

from __future__ import annotations

from fastapi import APIRouter

from microgrid_api.routers import building_types, exposed_attributes, health, me, runs

router = APIRouter()
router.include_router(health.router)
router.include_router(me.router)
router.include_router(building_types.router)
router.include_router(runs.router)
router.include_router(exposed_attributes.router)
