"""Exposed attributes (FR-ENG-11, layer 2): which building attributes users may filter on.

The list is admin-configured data (``exposed_attributes.yaml``, shipped as a default; the database copy and
its versioning belong to the API layer). This module loads and validates it, turns a user's form selections
into engine ``AttributeFilter`` objects, and finds columns that *could* be exposed (layer 1, discovery).

The engine (``community_load.synthetic``) knows none of this: it only receives ``AttributeFilter`` lists.
"""

from __future__ import annotations

import functools
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from importlib import resources
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from microgrid.community_load.registry import BuildingTypeRegistry, default_registry
from microgrid.community_load.synthetic.attribute_filters import (
    FILTER_KEYS,
    OPERATORS,
    AttributeFilter,
    parse_attribute_filter,
)
from microgrid.datasets import describe_characteristics

SOURCES = ("resstock", "comstock")
KINDS = ("numeric", "categorical", "boolean")
_OPERATORS_BY_KIND = {
    "numeric": ("equals", "in", "range", "not_null", "is_null"),
    "categorical": ("equals", "in", "not_null", "is_null"),
    "boolean": ("is_true", "is_false", "not_null", "is_null"),
}
_TOP_KEYS = {"version", "attributes"}
_SELECTION_KEYS = FILTER_KEYS - {"column"} | {"attribute"}
#: Columns that are identifiers or run bookkeeping, never useful as filters.
_NOT_CANDIDATES = {"bldg_id", "completed_status", "applicability"}


@dataclass(frozen=True)
class ExposedAttribute:
    key: str
    label: str
    help: str
    kind: str
    columns: Mapping[str, str]  # source -> characteristics column
    operators: tuple[str, ...]
    choices: tuple[str, ...] | None = None
    min: float | None = None
    max: float | None = None
    building_types: tuple[str, ...] = ()  # registry type keys; empty = every type of the listed sources
    order: int = 100
    active: bool = True


def _parse_attribute(entry: object, index: int, registry: BuildingTypeRegistry | None) -> ExposedAttribute:
    if not isinstance(entry, dict):
        raise ValueError(f"attributes[{index}]: expected a mapping")
    where = f"attributes[{index}] ({entry.get('key', '?')})"
    allowed = {f.name for f in fields(ExposedAttribute)}
    unknown = sorted(set(entry) - allowed)
    if unknown:
        raise ValueError(f"{where}: unknown field(s) {', '.join(unknown)}")
    missing = [k for k in ("key", "label", "kind", "columns", "operators") if k not in entry]
    if missing:
        raise ValueError(f"{where}: missing field(s) {', '.join(missing)}")
    for k in ("key", "label"):
        if not isinstance(entry[k], str) or not entry[k].strip():
            raise ValueError(f"{where}: {k} must be a non-empty string")
    if entry["kind"] not in KINDS:
        raise ValueError(f"{where}: kind must be one of {KINDS}, got {entry['kind']!r}")
    kind = entry["kind"]

    cols = entry["columns"]
    if not isinstance(cols, dict) or not cols:
        raise ValueError(f"{where}: columns must name at least one source ({', '.join(SOURCES)})")
    bad_sources = sorted(str(s) for s in cols if s not in SOURCES)
    if bad_sources:
        raise ValueError(f"{where}: columns has unknown source(s) {', '.join(bad_sources)} (one of {SOURCES})")
    if not all(isinstance(c, str) and c.strip() for c in cols.values()):
        raise ValueError(f"{where}: every entry in columns must be a non-empty column name")

    ops = entry["operators"]
    if not isinstance(ops, list) or not ops:
        raise ValueError(f"{where}: operators must be a non-empty list")
    for op in ops:
        if op not in OPERATORS:
            raise ValueError(f"{where}: unknown operator {op!r} (one of {', '.join(OPERATORS)})")
        if op not in _OPERATORS_BY_KIND[kind]:
            raise ValueError(f"{where}: operator {op!r} is not valid for {kind} attributes")

    choices = entry.get("choices")
    if choices is not None:
        if kind != "categorical":
            raise ValueError(f"{where}: choices only apply to categorical attributes")
        if not isinstance(choices, list) or not choices or not all(isinstance(c, str) for c in choices):
            raise ValueError(f"{where}: choices must be a non-empty list of strings")
    lo, hi = entry.get("min"), entry.get("max")
    if (lo is not None or hi is not None) and kind != "numeric":
        raise ValueError(f"{where}: min/max only apply to numeric attributes")
    for name, v in (("min", lo), ("max", hi)):
        if v is not None and (isinstance(v, bool) or not isinstance(v, int | float)):
            raise ValueError(f"{where}: {name} must be a number")
    if lo is not None and hi is not None and lo > hi:
        raise ValueError(f"{where}: min {lo} exceeds max {hi}")

    types = entry.get("building_types", [])
    if not isinstance(types, list):
        raise ValueError(f"{where}: building_types must be a list of registry type keys")
    if registry is not None:
        for t in types:
            if t not in registry:
                raise ValueError(f"{where}: building_types names {t!r}, which is not in the building-type registry")
    order = entry.get("order", 100)
    if isinstance(order, bool) or not isinstance(order, int):
        raise ValueError(f"{where}: order must be an integer")
    active = entry.get("active", True)
    if not isinstance(active, bool):
        raise ValueError(f"{where}: active must be true or false")

    return ExposedAttribute(
        key=entry["key"],
        label=entry["label"],
        help=str(entry.get("help", "")),
        kind=kind,
        columns=dict(cols),
        operators=tuple(ops),
        choices=None if choices is None else tuple(choices),
        min=lo,
        max=hi,
        building_types=tuple(types),
        order=order,
        active=active,
    )


@dataclass(frozen=True)
class ExposedAttributes:
    attributes: tuple[ExposedAttribute, ...]

    @classmethod
    def from_dict(cls, data: object, registry: BuildingTypeRegistry | None = None) -> ExposedAttributes:
        if not isinstance(data, dict):
            raise ValueError("exposed attributes: expected a mapping with 'version' and 'attributes'")
        unknown = sorted(set(data) - _TOP_KEYS)
        if unknown:
            raise ValueError(f"exposed attributes: unknown top-level key(s) {', '.join(unknown)}")
        if data.get("version") != 1:
            raise ValueError(f"exposed attributes: version must be 1, got {data.get('version')!r}")
        raws = data.get("attributes", [])
        if not isinstance(raws, list):
            raise ValueError("exposed attributes: 'attributes' must be a list")
        parsed = [_parse_attribute(e, i, registry) for i, e in enumerate(raws)]
        seen: set[str] = set()
        for a in parsed:
            if a.key in seen:
                raise ValueError(f"exposed attributes: duplicate key {a.key!r}")
            seen.add(a.key)
        return cls(tuple(parsed))

    @classmethod
    def from_yaml(cls, path: Path | str, registry: BuildingTypeRegistry | None = None) -> ExposedAttributes:
        return cls.from_dict(yaml.safe_load(Path(path).read_text()), registry)

    def get(self, key: str) -> ExposedAttribute:
        for a in self.attributes:
            if a.key == key:
                return a
        raise KeyError(f"unknown exposed attribute {key!r}; known: {', '.join(a.key for a in self.attributes)}")

    def active_for(self, source: str, building_type: str | None = None) -> list[ExposedAttribute]:
        """Attributes to offer for a source (and optionally one building type), in display order."""
        if source not in SOURCES:
            raise ValueError(f"source must be one of {SOURCES}, got {source!r}")
        out = [
            a
            for a in self.attributes
            if a.active
            and source in a.columns
            and (building_type is None or not a.building_types or building_type in a.building_types)
        ]
        return sorted(out, key=lambda a: (a.order, a.key))

    def to_filters(
        self,
        selections: Sequence[Mapping[str, Any]],
        *,
        source: str,
        building_type: str | None = None,
    ) -> list[AttributeFilter]:
        """Validate a user's selections against this list and build engine filters.

        A selection is ``{"attribute": key, "op": ..., plus value / values / min / max ...}``. Anything not
        offered (unknown, inactive, wrong source or type, operator not allowed, value out of range or not a
        listed choice) is an error.
        """
        offered = {a.key: a for a in self.active_for(source, building_type)}
        out: list[AttributeFilter] = []
        seen: set[str] = set()
        for i, sel in enumerate(selections):
            where = f"selection[{i}]"
            if not isinstance(sel, Mapping) or "attribute" not in sel:
                raise ValueError(f"{where}: expected a mapping with an `attribute` key")
            bad = sorted(str(k) for k in sel if k not in _SELECTION_KEYS)
            if bad:
                raise ValueError(f"{where}: unknown key(s) {', '.join(bad)}")
            key = sel["attribute"]
            if not any(x.key == key for x in self.attributes):
                raise ValueError(
                    f"{where}: unknown exposed attribute {key!r}; known: {', '.join(x.key for x in self.attributes)}"
                )
            attr = self.get(key)
            if key not in offered:
                raise ValueError(
                    f"{where}: attribute {key!r} does not apply to source {source!r}"
                    + (f" / building type {building_type!r}" if building_type else "")
                    + " or is not active"
                )
            if key in seen:
                raise ValueError(f"{where}: attribute {key!r} is used more than once")
            seen.add(key)
            if "op" not in sel:
                raise ValueError(f"{where}: attribute {key!r} needs an `op` (one of {', '.join(attr.operators)})")
            if sel["op"] not in attr.operators:
                raise ValueError(
                    f"{where}: operator {sel['op']!r} is not allowed for {key!r} (allowed: {', '.join(attr.operators)})"
                )
            raw = {k: v for k, v in sel.items() if k != "attribute"}
            raw["column"] = attr.columns[source]
            f = parse_attribute_filter(raw, f"{where} ({key})")
            self._check_values(attr, f, where)
            out.append(f)
        return out

    @staticmethod
    def _check_values(attr: ExposedAttribute, f: AttributeFilter, where: str) -> None:
        if attr.choices is not None:
            picked = [f.value] if f.op == "equals" else list(f.values or ())
            for v in picked:
                if v not in attr.choices:
                    raise ValueError(
                        f"{where}: {v!r} is not one of the allowed choices for {attr.key!r}: {', '.join(attr.choices)}"
                    )
        if attr.kind == "numeric":
            nums: list[tuple[str, float]] = []
            if f.op == "equals" and f.value is not None and isinstance(f.value, int | float):
                nums = [("value", float(f.value))]
            elif f.op == "in":
                nums = [("value", float(v)) for v in (f.values or ()) if isinstance(v, int | float)]
            else:
                nums = [(n, float(v)) for n, v in (("min", f.min), ("max", f.max)) if v is not None]
            for name, v in nums:
                if attr.min is not None and v < attr.min:
                    raise ValueError(
                        f"{where}: {name} {v:g} is below the allowed minimum {attr.min:g} for {attr.key!r}"
                    )
                if attr.max is not None and v > attr.max:
                    raise ValueError(
                        f"{where}: {name} {v:g} is above the allowed maximum {attr.max:g} for {attr.key!r}"
                    )


@functools.lru_cache(maxsize=1)
def default_exposed_attributes() -> ExposedAttributes:
    """The starter set shipped with the package (``exposed_attributes.yaml``)."""
    text = resources.files("microgrid.community_load").joinpath("exposed_attributes.yaml").read_text()
    return ExposedAttributes.from_dict(yaml.safe_load(text), default_registry())


def check_against_data(exposed: ExposedAttributes, df: pd.DataFrame, source: str) -> list[str]:
    """Problems for active attributes whose column is missing from ``df`` (never silently dropped)."""
    problems = []
    for a in exposed.active_for(source):
        col = a.columns[source]
        if col not in df.columns:
            problems.append(f"{source}: attribute {a.key!r} uses column {col!r}, which is not in the data")
    return problems


def discover_candidates(
    df: pd.DataFrame, exposed: ExposedAttributes | None = None, source: str | None = None
) -> list[dict[str, Any]]:
    """Columns that could be exposed as filters (layer 1, discovery).

    Uses ``datasets.describe_characteristics``; skips identifiers, run-status columns, simulation outputs
    (``out.*``) and columns with fewer than two distinct values (nothing to filter on). Each entry adds
    ``suggested_operators``; when ``exposed`` and ``source`` are given, also ``exposed_as`` (key or None).
    """
    if exposed is not None and source is None:
        raise ValueError("source is required when exposed is given")
    by_col: dict[str, str] = {}
    if exposed is not None:
        assert source is not None
        for a in exposed.attributes:
            if source in a.columns:
                by_col[a.columns[source]] = a.key
    out = []
    for row in describe_characteristics(df):
        name = row["name"]
        if name in _NOT_CANDIDATES or name.startswith("out.") or row["n_distinct"] < 2:
            continue
        cand = dict(row)
        cand["suggested_operators"] = list(_OPERATORS_BY_KIND[row["kind"]])
        if exposed is not None:
            cand["exposed_as"] = by_col.get(name)
        out.append(cand)
    return out
