"""Characteristics dictionary: per-column summary used for attribute discovery (FR-DATA-7, FR-ENG-11)."""

from __future__ import annotations

from typing import Any

import pandas as pd

MAX_VALUES = 20


def _kind(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    return "categorical"


def _py(value: Any) -> Any:
    """Convert numpy scalars to plain Python so the result is JSON serialisable."""
    return value.item() if hasattr(value, "item") else value


def describe_characteristics(df: pd.DataFrame) -> list[dict[str, Any]]:
    """One dict per column, in column order.

    Keys: ``name``, ``kind`` (categorical | numeric | boolean), ``n_distinct`` (missing excluded), ``n_missing``;
    numeric columns add ``min`` and ``max``; categorical columns add ``values`` (up to 20, most frequent first,
    ties alphabetical) and ``values_truncated``.
    """
    rows: list[dict[str, Any]] = []
    for name in df.columns:
        s = df[name]
        present = s.dropna()
        kind = _kind(s)
        row: dict[str, Any] = {
            "name": str(name),
            "kind": kind,
            "n_distinct": int(present.nunique()),
            "n_missing": int(s.isna().sum()),
        }
        if kind == "numeric":
            row["min"] = _py(present.min()) if len(present) else None
            row["max"] = _py(present.max()) if len(present) else None
        elif kind == "categorical":
            counts = present.astype(str).value_counts()
            ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
            row["values"] = [v for v, _ in ordered[:MAX_VALUES]]
            row["values_truncated"] = len(ordered) > MAX_VALUES
        rows.append(row)
    return rows
