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
import threading
from collections import OrderedDict
from dataclasses import dataclass, fields, replace
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
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from microgrid_api.db.models import ExposedAttributeActivation, ExposedAttributeVersion
from microgrid_api.filter_schemas import ExposedAttributesDocument

logger = logging.getLogger("microgrid_api.exposed_attributes")

PACKAGED_VERSION = 0
SOURCES = ("resstock", "comstock")


class VersionConflict(Exception):
    """The base version is not the latest one (another admin saved first)."""


class ActivationConflict(Exception):
    """The active version is not the one the caller expected, or another activation won a race."""


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


@functools.lru_cache(maxsize=1)
def packaged_exposed() -> ExposedAttributes:
    """The packaged starter set, parsed once. Used to word pool-check messages for runs without selections (whose
    filters carry no attributes), so those runs never depend on the stored configuration."""
    return parse_document(packaged_document())


def parse_document(document: dict[str, Any]) -> ExposedAttributes:
    """The library's validator, against the packaged building-type registry. Raises ValueError with a clear message."""
    return ExposedAttributes.from_dict(copy.deepcopy(document), default_registry())


def parse_stored_document(document: dict[str, Any]) -> ExposedAttributes:
    """A stored document must pass BOTH the API's size/shape limits (it may have been written before them, or outside
    the API) and the library's validator. Any failure is a ValueError; nothing else escapes."""
    try:
        ExposedAttributesDocument.model_validate(document)
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors(include_input=False)[:5]
        )
        raise ValueError(f"the document does not meet the current limits ({problems})") from None
    try:
        return parse_document(document)
    except ValueError:
        raise
    except Exception as exc:  # OverflowError, TypeError, ... from unexpected stored content
        raise ValueError(f"the document cannot be parsed ({type(exc).__name__})") from None


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
        exposed = parse_stored_document(document)
    except ValueError as exc:  # parse_stored_document turns everything into ValueError: never a 500
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
    try:
        parse_document(document)
    except (OverflowError, TypeError) as exc:  # backstop: the request schema already bounds numbers and types
        raise ValueError(str(exc)) from None
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
            _add_activation(session, row.version, subject, expected=None)
        session.commit()
    except IntegrityError:
        session.rollback()
        raise VersionConflict(
            f"another admin saved version {latest + 1} (or changed the active version) at the same time; reload the "
            "configuration and apply your change again"
        ) from None
    session.refresh(row)
    return row


def _add_activation(session: Session, version: int, subject: str, expected: int | None) -> None:
    """Append an activation whose id is the latest id + 1. Two activations made from the same state collide on the
    primary key, so the log is a strict chain (an optimistic lock without an extra column). ``expected``: the version
    the caller believes is active; a mismatch is ActivationConflict."""
    latest = _latest_activation(session)
    current = PACKAGED_VERSION if latest is None or latest.version is None else int(latest.version)
    if expected is not None and expected != current:
        raise ActivationConflict(
            f"expected_active_version is {expected}, but version {current} is active now (another admin changed it); "
            "reload and try again"
        )
    session.add(
        ExposedAttributeActivation(
            id=(latest.id if latest is not None else 0) + 1,
            version=None if version == PACKAGED_VERSION else version,
            activated_by=subject,
        )
    )
    session.flush()


def activate_version(session: Session, version: int, subject: str, expected_active_version: int | None = None) -> None:
    """Make ``version`` active (0 = packaged). Unknown: VersionNotFound; invalid stored document: ValueError.

    With ``expected_active_version`` a concurrent change is ActivationConflict (409). Without it the call is
    last-writer-wins: a collision with a concurrent activation is retried on the new state (a few times).
    """
    document, _ = get_document(session, version)
    parse_stored_document(document)  # never activate something the limits or the library would reject
    for _attempt in range(3):
        try:
            _add_activation(session, version, subject, expected_active_version)
            session.commit()
            return
        except IntegrityError:
            session.rollback()
            if expected_active_version is not None:
                raise ActivationConflict(
                    "another admin changed the active version at the same time; reload and try again"
                ) from None
    raise ActivationConflict("the active version kept changing; reload and try again")


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


class SelectionError(ValueError):
    """A selection or filter problem. ``str(exc)`` is the full detail (admins); ``public`` never names a column or a
    data value and is what other roles see (FR-ENG-11 d without leaking the configuration's internals)."""

    def __init__(self, detail: str, public: str) -> None:
        super().__init__(detail)
        self.public = public


def _column_keys(exposed: ExposedAttributes) -> list[tuple[str, str]]:
    """(column, attribute key) pairs, longest column first so a column that contains another is replaced first."""
    pairs = {(col, a.key) for a in exposed.attributes for col in a.columns.values()}
    return sorted(pairs, key=lambda p: (-len(p[0]), p))


def _redact_columns(message: str, exposed: ExposedAttributes) -> str:
    for column, key in _column_keys(exposed):
        message = message.replace(column, key)
    return message


def check_selection_categories(config: dict[str, Any], selections: dict[str, Any]) -> None:
    """Every key of ``filter_selections`` must name a category of ``composition.categories`` that has an
    ``archetype_filters.<source>`` entry to narrow, even when its list is empty (FR-ENG-3: nothing silently ignored).
    ValueError otherwise (messages name only the client's own category names)."""
    composition = config.get("composition")
    categories = composition.get("categories") if isinstance(composition, dict) else None
    if not isinstance(categories, dict):
        raise ValueError("filter_selections needs composition.categories in the config")
    af = config.get("archetype_filters")
    for name in selections:
        bounds = categories.get(name)
        if not isinstance(bounds, dict):
            raise ValueError(f"filter_selections names category {name!r}, which is not in composition.categories")
        source = bounds.get("source", "resstock")
        if source not in SOURCES:
            raise ValueError(f"category {name!r}: source must be one of {SOURCES}")
        cats = af.get(source) if isinstance(af, dict) else None
        if not isinstance(cats, dict) or not isinstance(cats.get(name), dict):
            raise ValueError(
                f"filter_selections names category {name!r}, which has no archetype_filters.{source} entry to narrow"
            )


def _failing_index(exposed: ExposedAttributes, sels: list[dict[str, Any]], source: str, bt: str | None) -> int:
    """Index of the first selection that makes ``to_filters`` fail (checked on growing prefixes)."""
    for i in range(1, len(sels) + 1):
        try:
            exposed.to_filters(sels[:i], source=source, building_type=bt)
        except (ValueError, OverflowError):
            return i - 1
    return len(sels) - 1


def apply_selections(
    config: dict[str, Any], selections: dict[str, list[dict[str, Any]]], exposed: ExposedAttributes
) -> dict[str, Any]:
    """A copy of ``config`` whose categories carry the engine filters built by ``exposed.to_filters``.

    Each selected category must be in ``composition.categories`` and already have an ``archetype_filters`` entry for
    its source (the selections narrow that pool; they never replace it). The category name is passed as the building
    type when it is a registered type; a category that is NOT a registered type may not use an attribute limited to
    particular building types (nothing says the category is one of them). Category problems are ValueError, selection
    problems SelectionError; a category with an empty list is validated but left untouched.
    """
    check_selection_categories(config, selections)
    out = copy.deepcopy(config)
    categories = out["composition"]["categories"]
    registry = default_registry()
    for name, sels in selections.items():
        if not sels:
            continue
        source = categories[name].get("source", "resstock")
        target = out["archetype_filters"][source][name]
        building_type = name if name in registry else None
        if building_type is None:
            for i, sel in enumerate(sels):
                key = sel.get("attribute")
                attr = next((a for a in exposed.attributes if a.key == key), None)
                if attr is not None and attr.building_types:
                    msg = (
                        f"category {name!r}, selection {i}: attribute {key!r} applies only to building types "
                        f"{', '.join(attr.building_types)}, and {name!r} is not a registered building type; name the "
                        "category after one of those types to use it"
                    )
                    raise SelectionError(msg, msg)
        try:
            built = exposed.to_filters(sels, source=source, building_type=building_type)
        except (ValueError, OverflowError) as exc:
            i = _failing_index(exposed, sels, source, building_type)
            key = sels[i].get("attribute") if 0 <= i < len(sels) else None
            raise SelectionError(
                f"category {name!r}: {exc}",
                f"category {name!r}, selection {i} (attribute {key!r}): {_redact_columns(str(exc), exposed)}",
            ) from None
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


_CACHE_SIZE = 4
_CacheKey = tuple[str, str, int, int]
_cache: OrderedDict[_CacheKey, Any] = OrderedDict()
_cache_lock = threading.Lock()
_key_locks: dict[_CacheKey, threading.Lock] = {}


def clear_characteristics_cache() -> None:
    with _cache_lock:
        _cache.clear()
        _key_locks.clear()


def _read_characteristics(path: str, source: str, mtime_ns: int, size: int) -> Any:
    """Read once per (path, mtime, size): concurrent cold callers wait for the first read instead of each reading the
    workbook. A replaced file has a new key and is read again. Returns a pandas DataFrame (no type stubs here)."""
    key = (path, source, mtime_ns, size)
    with _cache_lock:
        if key in _cache:
            _cache.move_to_end(key)
            return _cache[key]
        lock = _key_locks.setdefault(key, threading.Lock())
    with lock:
        with _cache_lock:
            if key in _cache:
                return _cache[key]
        try:
            loader = load_resstock_characteristics if source == "resstock" else load_comstock_characteristics
            df = loader(Path(path))
            with _cache_lock:
                _cache[key] = df
                while len(_cache) > _CACHE_SIZE:
                    _cache.popitem(last=False)
        finally:
            with _cache_lock:
                _key_locks.pop(key, None)
    return df


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


def preview(cfg: ScenarioConfig, data_root: Path, exposed: ExposedAttributes) -> dict[str, PoolPreview]:
    """Per-category pool counts for a parsed scenario whose ``data`` section was built server-side.

    A filter that does not fit the data (column missing, wrong type) is a SelectionError naming the category; its
    public message names only the exposed attribute(s) involved, never columns or data values. KeyError (the data
    lacks a column a base filter needs) and DataUnavailable propagate.
    """
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
    out: dict[str, PoolPreview] = {}
    for source, cats in (("resstock", af.resstock), ("comstock", af.comstock)):
        for name, cat_cfg in cats.items():
            one = replace(af, resstock={name: cat_cfg}, comstock={})
            if source == "comstock":
                one = replace(af, resstock={}, comstock={name: cat_cfg})
            try:
                out.update(preview_pools(res, com, one))
            except (ValueError, OverflowError) as exc:
                columns = {f.column for f in cat_cfg.attributes}
                keys = sorted({key for col, key in _column_keys(exposed) if col in columns and col in str(exc)})
                logger.warning("filters for %s/%s do not fit the data: %s", source, name, exc)
                about = f"attribute(s) {', '.join(keys)}" if keys else "its attribute filters"
                raise SelectionError(
                    f"category {name!r}: {exc}",
                    f"category {name!r}: {about} cannot be applied to the server's data; an admin must check the "
                    "exposed-attribute configuration",
                ) from None
    return out


def unsampleable_categories(cfg: ScenarioConfig, previews: dict[str, PoolPreview]) -> list[str]:
    """Categories that may receive buildings (fixed_count > 0, or max_fraction > 0 without a fixed count) but whose
    pool is empty: the engine would fail on them, so a run with such filters is refused at creation."""
    out: list[str] = []
    for key, pv in previews.items():
        source, name = key.split("/", 1)
        bounds = cfg.composition.categories.get(name)
        if bounds is None or bounds.source != source or pv.matched > 0:
            continue
        fixed = bounds.fixed_count
        if (fixed is not None and fixed > 0) or (fixed is None and bounds.max_fraction > 0):
            out.append(name)
    return out
