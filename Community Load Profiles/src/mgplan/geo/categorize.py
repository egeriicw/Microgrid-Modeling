"""Assign Structures to snlg archetype categories and derive fixed building counts.

This is the bridge between GIS and the synthetic load engine: a real neighborhood
fixes the composition, so every snlg category gets a `fixed_count`.
"""
from __future__ import annotations

from dataclasses import dataclass

from mgplan.core.types import Structure, UseClass


@dataclass(frozen=True)
class CategoryRules:
    """Thresholds matching configs/scenarios/dc_multifamily_baseline.yaml."""
    mf_small_max_stories: int = 3        # 1-3 stories
    mf_mid_max_stories: int = 7          # 4-7 stories; 8+ is mf_large
    commercial_category: str = "commercial_private"
    default_mf_stories: int = 3          # used only when stories unknown; counted in `assumed`


def categorize(s: Structure, rules: CategoryRules = CategoryRules()) -> str | None:
    """snlg category for a structure, or None when the engine has no archetype for it."""
    if s.use is UseClass.RESIDENTIAL_MF:
        n = s.stories if s.stories is not None else rules.default_mf_stories
        if n <= rules.mf_small_max_stories:
            return "mf_small"
        return "mf_mid" if n <= rules.mf_mid_max_stories else "mf_large"
    if s.use is UseClass.COMMERCIAL:
        return rules.commercial_category
    return None


def category_counts(
    structures: list[Structure] | tuple[Structure, ...],
    rules: CategoryRules = CategoryRules(),
) -> tuple[dict[str, int], dict[str, int]]:
    """Return (counts per snlg category, skipped counts per UseClass value).

    `skipped` makes unmodeled load visible (e.g. single-family, industrial) instead of silent.
    """
    counts: dict[str, int] = {}
    skipped: dict[str, int] = {}
    for s in structures:
        cat = categorize(s, rules)
        if cat is None:
            skipped[s.use.value] = skipped.get(s.use.value, 0) + 1
        else:
            counts[cat] = counts.get(cat, 0) + 1
    return counts, skipped
