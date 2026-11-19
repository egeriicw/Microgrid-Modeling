"""Exposed-attribute configuration and pool preview (FR-ENG-11 b, c, d).

Who may call what:
* ``GET /exposed-attributes/active`` and ``POST /exposed-attributes/preview``: roles with the ``filters.use`` permission
  flag (asked through ``permissions.can`` with the server's overrides, never by role name). Others get 403: the
  configuration is not a per-user resource, so its existence is not hidden; the flag is what is refused.
* ``/exposed-attributes/versions...`` (list, read, create, activate): admins only. Managing the configuration is an
  admin rule in the PRD (FR-ENG-11 b, FR-WEB-9), distinct from using filters. Non-admins get the same 403 for every
  version id, checked before the id is looked up or even parsed, so ids cannot be probed.

Data for the preview is read server-side only (MICROGRID_DATA_DIR); the client never sends a path or a column name.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi import Path as PathParam
from microgrid.community_load.permissions import can
from microgrid.community_load.synthetic.config import config_from_dict
from pydantic import ValidationError
from sqlalchemy.orm import Session

from microgrid_api.auth import Principal, current_principal
from microgrid_api.config import Settings
from microgrid_api.db.models import ExposedAttributeVersion
from microgrid_api.filter_schemas import (
    MAX_VERSION,
    ActivateRequest,
    ActiveConfigurationOut,
    ExposedAttributesDocument,
    OfferedAttribute,
    PoolPreviewOut,
    PreviewOut,
    PreviewRequest,
    VersionCreate,
    VersionDetail,
    VersionList,
    VersionOut,
    selections_to_plain,
)
from microgrid_api.routers.runs import check_client_config, check_config_shape, get_session, selection_http_error
from microgrid_api.services import exposed_attributes as svc
from microgrid_api.services import runs as run_svc

logger = logging.getLogger("microgrid_api.exposed_attributes")
router = APIRouter(prefix="/exposed-attributes", tags=["exposed-attributes"])

NOT_ADMIN = "only an admin can manage the exposed-attribute configuration"


def require_filters_use(request: Request, principal: Principal = Depends(current_principal)) -> Principal:
    settings: Settings = request.app.state.settings
    if not can(principal.role, "filters.use", settings.permission_overrides):
        raise HTTPException(403, f"role {principal.role!r} does not have the filters.use permission")
    return principal


def require_admin(principal: Principal = Depends(current_principal)) -> Principal:
    if principal.role != "admin":
        raise HTTPException(403, NOT_ADMIN)
    return principal


def active_or_503(session: Session) -> svc.ActiveConfiguration:
    try:
        return svc.active_configuration(session)
    except svc.InvalidStoredConfiguration as exc:
        raise HTTPException(503, str(exc)) from None


def _version_out(row: ExposedAttributeVersion, active: int) -> VersionOut:
    return VersionOut(
        version=row.version,
        based_on=row.based_on,
        note=row.note,
        created_by=row.created_by,
        created_at=row.created_at,
        active=row.version == active,
    )


VersionPath = PathParam(ge=0, le=MAX_VERSION, description="Stored version number; 0 is the packaged starter set")


@router.get("/active", response_model=ActiveConfigurationOut, summary="Attributes users may filter on (active)")
def get_active(
    _: Principal = Depends(require_filters_use), session: Session = Depends(get_session)
) -> ActiveConfigurationOut:
    active = active_or_503(session)
    attrs = sorted((a for a in active.exposed.attributes if a.active), key=lambda a: (a.order, a.key))
    try:
        offered = [OfferedAttribute(**svc.offered_view(a)) for a in attrs]
    except (ValidationError, ValueError, OverflowError, TypeError):  # a stored document the view cannot represent
        logger.error("active exposed-attribute configuration version %s cannot be shown", active.version)
        raise HTTPException(
            503,
            f"the active exposed-attribute configuration (version {active.version}) cannot be shown; an admin must "
            "activate or create a valid version",
        ) from None
    return ActiveConfigurationOut(
        version=active.version,
        source="packaged" if active.version == svc.PACKAGED_VERSION else "database",
        activated_at=active.activated_at,
        attributes=offered,
    )


@router.get("/versions", response_model=VersionList, summary="List stored versions (admin)")
def list_versions(
    _: Principal = Depends(require_admin),
    session: Session = Depends(get_session),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=1_000_000),
) -> VersionList:
    rows, total = svc.list_versions(session, limit, offset)
    active = svc.active_version_number(session)
    return VersionList(
        items=[_version_out(r, active) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
        latest_version=svc.latest_version(session),
        active_version=active,
    )


def _describe(session: Session, version: int) -> tuple[VersionOut, dict[str, Any]]:
    try:
        document, row = svc.get_document(session, version)
    except svc.VersionNotFound:
        raise HTTPException(404, "version not found") from None
    active = svc.active_version_number(session)
    if row is not None:
        return _version_out(row, active), document
    packaged = VersionOut(
        version=svc.PACKAGED_VERSION,
        based_on=None,
        note="packaged starter set",
        created_by=None,
        created_at=None,
        active=active == svc.PACKAGED_VERSION,
    )
    return packaged, document


@router.get("/versions/{version}", response_model=VersionDetail, summary="Read one version, with columns (admin)")
def get_version(
    version: int = VersionPath, _: Principal = Depends(require_admin), session: Session = Depends(get_session)
) -> VersionDetail:
    meta, document = _describe(session, version)
    try:
        parsed = ExposedAttributesDocument.model_validate(document)
    except ValidationError:  # stored before the current limits (or written outside the API); never a 500
        logger.error("stored exposed-attribute version %s does not match the document schema", version)
        raise HTTPException(
            503, f"stored version {version} does not match the current document limits; create a corrected version"
        ) from None
    return VersionDetail(**meta.model_dump(), document=parsed)


@router.post("/versions", response_model=VersionOut, status_code=201, summary="Save a new version (admin)")
def create_version(
    body: VersionCreate, principal: Principal = Depends(require_admin), session: Session = Depends(get_session)
) -> VersionOut:
    try:
        row = svc.create_version(
            session,
            body.document.to_plain(),
            base_version=body.base_version,
            subject=principal.subject,
            note=body.note,
            activate=body.activate,
        )
    except svc.VersionConflict as exc:
        raise HTTPException(409, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    return _version_out(row, svc.active_version_number(session))


@router.post(
    "/versions/{version}/activate",
    response_model=VersionOut,
    summary="Make a version active (admin)",
    description="Activates a stored version, or 0 for the packaged starter set. The body is optional: send "
    "`expected_active_version` (the version you believe is active now) to fail with **409** instead of silently "
    "replacing another admin's activation; without it the newest activation wins.",
    responses={409: {"description": "expected_active_version is stale (another admin activated a version)"}},
)
def activate_version(
    version: int = VersionPath,
    body: ActivateRequest | None = None,
    principal: Principal = Depends(require_admin),
    session: Session = Depends(get_session),
) -> VersionOut:
    expected = body.expected_active_version if body is not None else None
    try:
        svc.activate_version(session, version, principal.subject, expected_active_version=expected)
    except svc.VersionNotFound:
        raise HTTPException(404, "version not found") from None
    except svc.ActivationConflict as exc:
        raise HTTPException(409, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(422, f"version {version} cannot be activated: {exc}") from None
    return _describe(session, version)[0]


@router.post("/preview", response_model=PreviewOut, summary="Pool preview: buildings matching each category")
def preview(
    body: PreviewRequest,
    request: Request,
    principal: Principal = Depends(require_filters_use),
    session: Session = Depends(get_session),
) -> PreviewOut:
    settings: Settings = request.app.state.settings
    check_config_shape(body.config)  # depth and size before anything copies or walks it
    config: dict[str, Any] = copy.deepcopy(body.config)
    check_client_config(config)
    active = active_or_503(session)
    if body.filter_selections is not None:
        try:
            config = svc.apply_selections(config, selections_to_plain(body.filter_selections), active.exposed)
        except ValueError as exc:
            raise selection_http_error(exc, principal) from None
    try:
        data = run_svc.server_side_data_section(settings)
    except ValueError:
        logger.error("pool preview: data directory missing or outside its root")
        raise HTTPException(503, "the server data directory is not available") from None
    try:
        cfg = config_from_dict({**config, "data": data}, source="<preview config>")
    except (ValueError, OverflowError) as exc:
        raise HTTPException(422, str(exc)) from None
    assert settings.data_dir is not None  # server_side_data_section succeeded
    try:
        previews = svc.preview(cfg, settings.data_dir, active.exposed)
    except svc.DataUnavailable as exc:
        raise HTTPException(503, str(exc)) from None
    except KeyError as exc:
        logger.error("pool preview: characteristics data lacks column %s", exc)
        raise HTTPException(503, "the server's characteristics data lacks a column this category needs") from None
    except ValueError as exc:  # an attribute's column or value does not fit the data (FR-ENG-11 d)
        raise selection_http_error(exc, principal) from None
    return PreviewOut(
        exposed_attributes_version=active.version,
        min_pool_size=cfg.archetype_filters.min_pool_size,
        categories={k: PoolPreviewOut(**v.to_dict()) for k, v in previews.items()},
    )
