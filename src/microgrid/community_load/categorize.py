"""Assign mapped Structures to building types and derive fixed building counts.

This is the bridge between GIS and the synthetic load engine: a real neighborhood fixes the composition,
so every building type present gets a ``fixed_count``. The types, and how structures map to them, come from
the building-type registry (``registry.py``), never from code.
"""

from __future__ import annotations

from collections.abc import Sequence

from microgrid.community_load.registry import BuildingTypeRegistry, default_registry
from microgrid.geo.types import Structure


def categorize(s: Structure, registry: BuildingTypeRegistry | None = None) -> str | None:
    """Registry type key for a structure, or None when no active type models it."""
    t = (registry or default_registry()).match(s)
    return t.key if t else None


def category_counts(
    structures: Sequence[Structure],
    registry: BuildingTypeRegistry | None = None,
) -> tuple[dict[str, int], dict[str, int]]:
    """Return (counts per building type, skipped counts per UseClass value).

    ``skipped`` makes unmodeled load visible (e.g. industrial, civic) instead of silent.
    """
    registry = registry or default_registry()
    counts: dict[str, int] = {}
    skipped: dict[str, int] = {}
    for s in structures:
        cat = categorize(s, registry)
        if cat is None:
            skipped[s.use.value] = skipped.get(s.use.value, 0) + 1
        else:
            counts[cat] = counts.get(cat, 0) + 1
    return counts, skipped
