"""The building-type catalogue for a composition form (FR-ENG-1, FR-WEB-1).

The registry as data a form can show: key, label, class, source, an optional description and whether the type can be
used yet. Inactive types are listed (marked inactive), not hidden. Sampling filters, column names and mapping rules
stay on the server: a client composes by key and the server fills the filters (``POST /runs`` with `composition`).
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, Request
from microgrid.community_load.registry import BuildingType, BuildingTypeRegistry
from pydantic import BaseModel

from microgrid_api.auth import Principal, current_principal

router = APIRouter(tags=["building-types"])

Source = Literal["resstock", "comstock"]
BuildingClass = Literal["residential", "commercial"]
# Residential vs commercial follows the source dataset (ResStock models homes, ComStock everything else), the same
# split the results use.
_CLASS: dict[str, BuildingClass] = {"resstock": "residential", "comstock": "commercial"}
_SOURCE: dict[str, Source] = {"resstock": "resstock", "comstock": "comstock"}


class BuildingTypeOut(BaseModel):
    key: str
    label: str
    building_class: BuildingClass
    source: Source
    description: str | None  # plain-language description, when the registry has one (none do yet)
    active: bool  # false = listed but not usable yet (a composition naming it is refused)


class BuildingTypeList(BaseModel):
    items: list[BuildingTypeOut]


def _out(t: BuildingType) -> BuildingTypeOut:
    description = getattr(t, "description", None)  # the registry has no such field today; follow it if added
    return BuildingTypeOut(
        key=t.key,
        label=t.label,
        building_class=_CLASS[t.source],
        source=_SOURCE[t.source],
        description=description if isinstance(description, str) and description else None,
        active=t.active,
    )


@router.get("/building-types", response_model=BuildingTypeList, summary="Building types a composition can use")
def building_types(request: Request, _: Principal = Depends(current_principal)) -> BuildingTypeList:
    registry: BuildingTypeRegistry = request.app.state.building_registry
    return BuildingTypeList(items=[_out(t) for t in registry.types])
