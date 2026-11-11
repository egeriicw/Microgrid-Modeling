"""Attribute filters (FR-ENG-11): a small, strict language for narrowing the source-building pool.

A filter names a characteristics column and an operator. Several filters on one category are combined
with AND. Filters only ever *narrow* the pool that is sampled; they never touch sampling, convergence,
perturbation or any other methodology. The engine knows nothing about who may use filters: that is
decided above it (``community_load.permissions``).

Operators (``op``):

* ``equals``    ``value`` (one string, number or boolean)
* ``in``        ``values`` (a non-empty list of those)
* ``range``     ``min`` and/or ``max`` (numbers), ``min_inclusive`` / ``max_inclusive`` (default true)
* ``not_null`` / ``is_null``   the column has / has no value
* ``is_true`` / ``is_false``   a boolean column (real booleans or the text "True"/"False" from spreadsheets)

A filter on a column that is not in the data raises an error naming the column; nothing is ever ignored.
"""

from __future__ import annotations

import difflib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

OPERATORS = ("equals", "in", "range", "not_null", "is_null", "is_true", "is_false")
_ALLOWED_KEYS = {
    "equals": {"value"},
    "in": {"values"},
    "range": {"min", "max", "min_inclusive", "max_inclusive"},
    "not_null": set(),
    "is_null": set(),
    "is_true": set(),
    "is_false": set(),
}
FILTER_KEYS = {"column", "op", "value", "values", "min", "max", "min_inclusive", "max_inclusive"}
_DEFAULTS = {"value": None, "values": None, "min": None, "max": None, "min_inclusive": True, "max_inclusive": True}


def _is_number(v: object) -> bool:
    return isinstance(v, int | float) and not isinstance(v, bool)


def _is_scalar(v: object) -> bool:
    return isinstance(v, str | bool) or _is_number(v)


@dataclass(frozen=True)
class AttributeFilter:
    """One condition on one characteristics column. Validated on construction."""

    column: str
    op: str
    value: str | int | float | bool | None = None
    values: tuple[str | int | float | bool, ...] | None = None
    min: float | None = None
    max: float | None = None
    min_inclusive: bool = True
    max_inclusive: bool = True

    def __post_init__(self) -> None:
        where = f"attribute filter on {self.column!r}"
        if not isinstance(self.column, str) or not self.column.strip():
            raise ValueError("attribute filter: `column` must be a non-empty string")
        if self.op not in OPERATORS:
            raise ValueError(f"{where}: unknown operator {self.op!r} (one of {', '.join(OPERATORS)})")
        given = {k for k, default in _DEFAULTS.items() if getattr(self, k) != default}
        extra = sorted(given - _ALLOWED_KEYS[self.op])
        if extra:
            raise ValueError(f"{where}: operator {self.op!r} does not take {', '.join(extra)}")
        if self.op == "equals":
            if self.value is None:
                raise ValueError(f"{where}: `equals` needs `value`")
            if not _is_scalar(self.value):
                raise ValueError(f"{where}: `value` must be a single string, number or boolean")
        elif self.op == "in":
            if not isinstance(self.values, tuple | list) or len(self.values) == 0:
                raise ValueError(f"{where}: `in` needs a non-empty `values` list")
            if not all(_is_scalar(v) for v in self.values):
                raise ValueError(f"{where}: `values` must be strings, numbers or booleans")
            object.__setattr__(self, "values", tuple(self.values))
        elif self.op == "range":
            if self.min is None and self.max is None:
                raise ValueError(f"{where}: `range` needs min or max")
            for key in ("min", "max"):
                v = getattr(self, key)
                if v is not None and not _is_number(v):
                    raise ValueError(f"{where}: `{key}` must be a number, got {v!r}")
            if self.min is not None and self.max is not None and self.min > self.max:
                raise ValueError(f"{where}: min {self.min} exceeds max {self.max}")
            for key in ("min_inclusive", "max_inclusive"):
                if not isinstance(getattr(self, key), bool):
                    raise ValueError(f"{where}: `{key}` must be true or false")


def check_attribute_filter_raw(raw: object, path: str, errors: list[str]) -> None:
    """Record every problem with one raw (YAML/dict) filter, naming ``path``; never raises."""
    if not isinstance(raw, Mapping):
        errors.append(f"{path}: expected a mapping, got {type(raw).__name__}")
        return
    for key in raw:
        if key not in FILTER_KEYS:
            hint = difflib.get_close_matches(str(key), sorted(FILTER_KEYS), n=1)
            errors.append(f"{path}.{key}: unknown key" + (f" (did you mean `{hint[0]}`?)" if hint else ""))
    if any(k not in FILTER_KEYS for k in raw):
        return
    try:
        parse_attribute_filter(raw, path)
    except ValueError as exc:
        errors.append(str(exc))


def parse_attribute_filter(raw: object, path: str = "attribute filter") -> AttributeFilter:
    """Build a filter from a mapping; any problem is a ValueError that starts with ``path``."""
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path}: expected a mapping, got {type(raw).__name__}")
    unknown = [k for k in raw if k not in FILTER_KEYS]
    if unknown:
        hint = difflib.get_close_matches(str(unknown[0]), sorted(FILTER_KEYS), n=1)
        suffix = f" (did you mean `{hint[0]}`?)" if hint else ""
        raise ValueError(f"{path}.{unknown[0]}: unknown key{suffix}")
    missing = [k for k in ("column", "op") if k not in raw]
    if missing:
        raise ValueError(f"{path}: missing key(s) {', '.join(missing)}")
    kwargs: dict[str, Any] = dict(raw)
    if isinstance(kwargs.get("values"), list):
        kwargs["values"] = tuple(kwargs["values"])
    try:
        return AttributeFilter(**kwargs)
    except ValueError as exc:
        raise ValueError(f"{path}: {exc}") from None


def parse_attribute_filters(raw: object, path: str) -> list[AttributeFilter]:
    """Parse a list of raw filters (``None`` means none)."""
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(f"{path}: expected a list of filters, got {type(raw).__name__}")
    return [parse_attribute_filter(item, f"{path}[{i}]") for i, item in enumerate(raw)]


# ---------------------------------------------------------------------------
# Applying filters to a characteristics table
# ---------------------------------------------------------------------------


def _bool_series(s: pd.Series, f: AttributeFilter) -> pd.Series:
    """True / False / NA for a boolean-like column; anything else is an error."""
    if pd.api.types.is_bool_dtype(s):
        return s.astype(bool)
    if pd.api.types.is_numeric_dtype(s):
        raise ValueError(f"column {f.column!r} is numeric, but operator {f.op!r} needs a boolean column")
    text = s.astype("string").str.strip().str.lower()
    bad = text.dropna()[~text.dropna().isin(["true", "false"])]
    if len(bad):
        raise ValueError(
            f"column {f.column!r} is not boolean (e.g. value {bad.iloc[0]!r}); operator {f.op!r} needs a boolean column"
        )
    return text.map({"true": True, "false": False})


def _check_value_types(s: pd.Series, f: AttributeFilter, values: Sequence[object]) -> None:
    if pd.api.types.is_bool_dtype(s):
        ok = all(isinstance(v, bool) for v in values)
        kind = "boolean"
    elif pd.api.types.is_numeric_dtype(s):
        ok = all(_is_number(v) for v in values)
        kind = "numeric"
    else:
        ok = all(isinstance(v, str) for v in values)
        kind = "text"
    if not ok:
        raise ValueError(f"column {f.column!r} is {kind}; {f.op!r} values must be {kind}, got {list(values)!r}")


def _mask_one(df: pd.DataFrame, f: AttributeFilter) -> pd.Series:
    if f.column not in df.columns:
        hint = difflib.get_close_matches(f.column, [str(c) for c in df.columns], n=3)
        suffix = f"; similar columns: {hint}" if hint else ""
        raise ValueError(f"attribute filter column {f.column!r} is not in the characteristics data{suffix}")
    s = df[f.column]
    if f.op == "not_null":
        return s.notna()
    if f.op == "is_null":
        return s.isna()
    if f.op in ("is_true", "is_false"):
        b = _bool_series(s, f)
        return (b == (f.op == "is_true")).fillna(False).astype(bool)
    if f.op == "equals":
        _check_value_types(s, f, [f.value])
        return (s == f.value).fillna(False).astype(bool)
    if f.op == "in":
        assert f.values is not None
        _check_value_types(s, f, f.values)
        return s.isin(list(f.values))
    # range
    if pd.api.types.is_bool_dtype(s) or not pd.api.types.is_numeric_dtype(s):
        raise ValueError(f"column {f.column!r} is not numeric, so `range` cannot be applied to it")
    mask = pd.Series(True, index=df.index)
    if f.min is not None:
        mask &= (s >= f.min) if f.min_inclusive else (s > f.min)
    if f.max is not None:
        mask &= (s <= f.max) if f.max_inclusive else (s < f.max)
    return (mask & s.notna()).astype(bool)


def apply_attribute_filters(df: pd.DataFrame, filters: Sequence[AttributeFilter]) -> pd.Series:
    """Boolean row mask: True where every filter holds (AND). No filters keeps every row."""
    mask = pd.Series(True, index=df.index)
    for f in filters:
        mask &= _mask_one(df, f)
    return mask
