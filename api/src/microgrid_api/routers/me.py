"""Who the caller is and what the server lets them do (FR-WEB-5), so the web app can show or hide controls.

The role is decided by the server from the API key; the browser never decides it. Any authenticated role may call
`GET /me`; it returns only the caller's own subject and role, the effective permission flags (the library's ``can``
with MICROGRID_PERMISSION_OVERRIDES) and the size caps a composition form needs. No key material, nothing about other
subjects, no server paths. The API still enforces every rule itself.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from microgrid.community_load.permissions import FLAGS, can
from pydantic import BaseModel

from microgrid_api.auth import Principal, current_principal
from microgrid_api.config import Settings
from microgrid_api.routers.runs import run_inputs

router = APIRouter(tags=["identity"])


class DemoCaps(BaseModel):
    max_buildings: int
    max_runs: int


class Limits(BaseModel):
    max_total_buildings: int
    max_ensemble_runs: int
    demo_caps: DemoCaps | None  # only for the demo role (stricter; predefined compositions only)


class Me(BaseModel):
    subject: str
    role: str
    permissions: dict[str, bool]  # every permission flag the library defines, e.g. {"filters.use": false}
    run_inputs: list[str]  # which of composition / composition_id / config this role may send to POST /runs
    limits: Limits


@router.get("/me", response_model=Me, summary="The caller's identity, permissions and limits")
def me(request: Request, principal: Principal = Depends(current_principal)) -> Me:
    settings: Settings = request.app.state.settings
    overrides = settings.permission_overrides
    demo_caps = (
        DemoCaps(max_buildings=settings.demo_max_buildings, max_runs=settings.demo_max_runs)
        if principal.role == "demo"
        else None
    )
    return Me(
        subject=principal.subject,
        role=principal.role,
        permissions={flag: can(principal.role, flag, overrides) for flag in sorted(FLAGS)},
        run_inputs=run_inputs(principal.role),
        limits=Limits(
            max_total_buildings=settings.max_total_buildings,
            max_ensemble_runs=settings.max_ensemble_runs,
            demo_caps=demo_caps,
        ),
    )
