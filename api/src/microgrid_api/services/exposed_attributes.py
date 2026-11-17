"""Exposed-attribute configuration in the database (FR-ENG-11 b) and filter selections (c, e).

Storage: ``exposed_attribute_versions`` holds immutable documents (the same shape as the packaged
``exposed_attributes.yaml``); ``exposed_attribute_activations`` is an append-only log whose newest row names the active
version. With no activation rows (a fresh database) the packaged starter set is active; it is addressed as version 0
and is never copied into the database (fallback, not seeding), so the migration does not depend on library content.

Validation uses the library's own parser (``ExposedAttributes.from_dict`` with the packaged building-type registry).
User selections become engine filters only through ``ExposedAttributes.to_filters``; a client never names a column.
"""

from __future__ import annotations

import copy
import functools
import logging
from dataclasses import dataclass, fields
from datetime import datetime
from importlib import resources
from pathlib import Path
from typing import Any

from microgrid.community_load.exposed_attributes import ExposedAttribute, ExposedAttributes
from microgrid.community_load.registry import default_registry
from microgrid.community_load.synthetic.archetype import (
    load_comstock_characteristics,
    load_resstock_characteristics,
)
from microgrid.community_load.synthetic.attribute_filters import AttributeFilter
from microgrid.community_load.synthetic.config import ScenarioConfig
from microgrid.community_load.synthetic.pool_preview import PoolPreview, preview_pools
from microgrid.community_load.synthetic.strict_yaml import load_yaml_strict
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from microgrid_api.db.models import ExposedAttributeActivation, ExposedAttributeVersion

logger = logging.getLogger("microgrid_api.exposed_attributes")

PACKAGED_VERSION = 0
SOURCES = ("resstock", "comstock")


class VersionConflict(Exception):
    """The base version is not the latest one (another admin saved first)."""


class VersionNotFound(LookupError):
    pass


class InvalidStoredConfiguration(RuntimeError):
    """The active stored document no longer passes the library's validation (e.g. the registry changed)."""


@dataclass(frozen=True)
class ActiveConfiguration:
    version: int
    exposed: ExposedAttributes
    activated_by: str | None
    activated_at: datetime | None

    @property
    def source(self) -> str:
        return "packaged" if self.version == PACKAGED_VERSION else "database"


# --- documents --------------------------------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _packaged_text() -> str:
    return resources.files("microgrid.community_load").joinpath("exposed_attributes.yaml").read_text()


def packaged_document() -> dict[str, Any]:
    """The packaged starter set as a plain document (a fresh copy each call)."""
    doc = load_yaml_strict(_packaged_text())
    assert isinstance(doc, dict)
    return doc


def parse_document(document: dict[str, Any]) -> ExposedAttributes:
    """The library's validator, against the packaged building-type registry. Raises ValueError with a clear message."""
    return ExposedAttributes.from_dict(copy.deepcopy(document), default_registry())


# --- reading -----------------------------------------------------------------------------------------------------


def latest_version(session: Session) -> int:
    return int(session.scalar(select(func.max(ExposedAttributeVersion.version))) or 0)


def _latest_activation(session: Session) -> ExposedAttributeActivation | None:
    return session.scalars(
        select(ExposedAttributeActivation).order_by(ExposedAttributeActivation.id.desc()).limit(1)
    ).first()


def active_version_number(session: Session) -> int:
    act = _latest_activation(session)
    return PACKAGED_VERSION if act is None or act.version is None else int(act.version)


def get_document(session: Session, version: int) -> tuple[dict[str, Any], ExposedAttributeVersion | None]:
    """The document of ``version`` (0 = packaged) and its row (None for 0). Unknown version: VersionNotFound."""
    if version == PACKAGED_VERSION:
        return packaged_document(), None
    row = session.get(ExposedAttributeVersion, version)
    if row is None:
        raise VersionNotFound(version)
    return copy.deepcopy(row.document), row


def active_configuration(session: Session) -> ActiveConfiguration:
    act = _latest_activation(session)
    version = PACKAGED_VERSION if act is None or act.version is None else int(act.version)
    document, _ = get_document(session, version)
    try:
        exposed = parse_document(document)
    except ValueError as exc:
        logger.error("active exposed-attribute configuration version %s is invalid", version)
        raise InvalidStoredConfiguration(
            f"the active exposed-attribute configuration (version {version}) is no longer valid: {exc}. "
            "An admin must activate or create a valid version."
        ) from None
    return ActiveConfiguration(
        version=version,
        exposed=exposed,
        activated_by=None if act is None else act.activated_by,
        activated_at=None if act is None else act.activated_at,
    )


def list_versions(session: Session, limit: int, offset: int) -> tuple[list[ExposedAttributeVersion], int]:
    total = int(session.scalar(select(func.count()).select_from(ExposedAttributeVersion)) or 0)
    rows = session.scalars(
        select(ExposedAttributeVersion).order_by(ExposedAttributeVersion.version.desc()).limit(limit).offset(offset)
    ).all()
    return list(rows), total


# --- writing -----------------------------------------------------------------------------------------------------


def create_version(
    session: Session,
    document: dict[str, Any],
    *,
    base_version: int,
    subject: str,
    note: str | None,
    activate: bool,
) -> ExposedAttributeVersion:
    """Insert ``document`` as version latest + 1 if ``base_version`` is the latest; otherwise VersionConflict.

    The document is validated with the library first (ValueError, nothing stored). Two writers that both pass the
    base check collide on the primary key; the loser gets VersionConflict and nothing is stored.
    """
    parse_document(document)
    latest = latest_version(session)
    if base_version != latest:
        raise VersionConflict(
            f"base_version {base_version} is not the latest version (the latest is version {latest}); "
            "reload the configuration and apply your change again"
        )
    row = ExposedAttributeVersion(
        version=latest + 1, document=copy.deepcopy(document), based_on=base_version, note=note, created_by=subject
    )
    try:
        session.add(row)
        session.flush()
        if activate:
            session.add(ExposedAttributeActivation(version=row.version, activated_by=subject))
        session.commit()
    except IntegrityError:
        session.rollback()
        raise VersionConflict(
            f"another admin saved version {latest + 1} at the same time; reload the configuration and apply your "
            "change again"
        ) from None
    session.refresh(row)
    return row


def activate_version(session: Session, version: int, subject: str) -> None:
    """Make ``version`` active (0 = packaged). Unknown: VersionNotFound; invalid stored document: ValueError."""
    document, _ = get_document(session, version)
    parse_document(document)  # never activate something the library would reject
    session.add(
        ExposedAttributeActivation(version=None if version == PACKAGED_VERSION else version, activated_by=subject)
    )
    session.commit()


# --- selections -> engine filters --------------------------------------------------------------------------------


def offered_view(attr: ExposedAttribute) -> dict[str, Any]:
    """What a form needs; never the column names."""
    return {
        "key": attr.key,
        "label": attr.label,
        "help": attr.help,
        "kind": attr.kind,
        "operators": list(attr.operators),
        "choices": None if attr.choices is None else list(attr.choices),
        "min": attr.min,
        "max": attr.max,
        "building_types": list(attr.building_types),
        "sources": [s for s in SOURCES if s in attr.columns],
        "order": attr.order,
    }


_FILTER_DEFAULTS = {f.name: f.default for f in fields(AttributeFilter)}


def filter_to_dict(f: AttributeFilter) -> dict[str, Any]:
    """An engine filter as a scenario-config mapping (only non-default keys; tuples as lists)."""
    out: dict[str, Any] = {"column": f.column, "op": f.op}
    for name, default in _FILTER_DEFAULTS.items():
        if name in out:
            continue
        value = getattr(f, name)
        if value != default:
            out[name] = list(value) if isinstance(value, tuple) else value
    return out


def raw_attribute_filter_locations(config: dict[str, Any]) -> list[str]:
    """Where a client config carries raw (column-level) attribute filters. Empty lists narrow nothing: allowed."""
    found: list[str] = []
    af = config.get("archetype_filters")
    if not isinstance(af, dict):
        return found
    for source, cats in af.items():
        if not isinstance(cats, dict):
            continue
        for name, body in cats.items():
            if isinstance(body, dict) and body.get("attributes") not in (None, []):
                found.append(f"archetype_filters.{source}.{name}.attributes")
    return sorted(found)


def apply_selections(
    config: dict[str, Any], selections: dict[str, list[dict[str, Any]]], exposed: ExposedAttributes
) -> dict[str, Any]:
    """A copy of ``config`` whose categories carry the engine filters built by ``exposed.to_filters``.

    Each selected category must be in ``composition.categories`` and already have an ``archetype_filters`` entry for
    its source (the selections narrow that pool; they never replace it). The category name is passed as the building
    type when it is a registered type, so type-specific attributes apply only where they belong. ValueError on any
    problem.
    """
    out = copy.deepcopy(config)
    composition = out.get("composition")
    categories = composition.get("categories") if isinstance(composition, dict) else None
    if not isinstance(categories, dict):
        raise ValueError("filter_selections needs composition.categories in the config")
    registry = default_registry()
    for name, sels in selections.items():
        bounds = categories.get(name)
        if not isinstance(bounds, dict):
            raise ValueError(f"filter_selections names category {name!r}, which is not in composition.categories")
        source = bounds.get("source", "resstock")
        if source not in SOURCES:
            raise ValueError(f"category {name!r}: source must be one of {SOURCES}")
        af = out.get("archetype_filters")
        cats = af.get(source) if isinstance(af, dict) else None
        target = cats.get(name) if isinstance(cats, dict) else None
        if not isinstance(target, dict):
            raise ValueError(
                f"filter_selections names category {name!r}, which has no archetype_filters.{source} entry to narrow"
            )
        building_type = name if name in registry else None
        built = exposed.to_filters(sels, source=source, building_type=building_type)
        target["attributes"] = [filter_to_dict(f) for f in built]
    return out


def effective_filters(snapshot: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Column-level filters actually stored in a run snapshot, keyed ``"<source>/<category>"``."""
    out: dict[str, list[dict[str, Any]]] = {}
    af = snapshot.get("archetype_filters")
    if not isinstance(af, dict):
        return out
    for source in SOURCES:
        cats = af.get(source)
        if not isinstance(cats, dict):
            continue
        for name, body in cats.items():
            attrs = body.get("attributes") if isinstance(body, dict) else None
            if isinstance(attrs, list) and attrs:
                out[f"{source}/{name}"] = copy.deepcopy(attrs)
    return out


# --- pool preview ------------------------------------------------------------------------------------------------


class DataUnavailable(RuntimeError):
    """Server-side characteristics data is missing or unreadable. Messages never contain paths."""


@functools.lru_cache(maxsize=4)
def _read_characteristics(
    path: str, source: str, mtime_ns: int, size: int
) -> Any:  # a pandas DataFrame (pandas ships no type stubs here)
    del mtime_ns, size  # cache key only: a replaced file is read again
    loader = load_resstock_characteristics if source == "resstock" else load_comstock_characteristics
    return loader(Path(path))


def _characteristics(
    root: Path, background_dir: str, file_name: str, source: str
) -> Any:  # a pandas DataFrame (pandas ships no type stubs here)
    path = (Path(background_dir) / file_name).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        logger.error("%s characteristics file missing or outside the data root", source)
        raise DataUnavailable(f"the server's {source} characteristics data is not available")
    stat = path.stat()
    try:
        return _read_characteristics(str(path), source, stat.st_mtime_ns, stat.st_size)
    except Exception as exc:  # unreadable/corrupt file: report without path or library message
        logger.error("cannot read %s characteristics (%s)", source, type(exc).__name__)
        raise DataUnavailable(f"the server's {source} characteristics data could not be read") from None


def preview(cfg: ScenarioConfig, data_root: Path) -> dict[str, PoolPreview]:
    """Per-category pool counts for a parsed scenario whose ``data`` section was built server-side."""
    af = cfg.archetype_filters
    res = (
        _characteristics(data_root, cfg.data.background_dir, cfg.data.resstock_chars_file, "resstock")
        if af.resstock
        else None
    )
    com = (
        _characteristics(data_root, cfg.data.background_dir, cfg.data.comstock_chars_file, "comstock")
        if af.comstock
        else None
    )
    return preview_pools(res, com, af)
