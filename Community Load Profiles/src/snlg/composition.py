"""Composition sampling — Sections 2–3 of the methodology.

Two sampling modes (config-selectable):
  dirichlet      — draw from Dirichlet, project onto [p_min, p_max] ∩ simplex
  uniform_renorm — draw p_i ~ Uniform(p_min, p_max), renormalize

Both produce integer building counts that sum exactly to total_buildings.
"""
from __future__ import annotations

import numpy as np

from snlg._types import ArchetypePool, BuildingId, BuildingSelection, NeighborhoodComposition
from snlg.config import ScenarioConfig


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _project_simplex_box(
    p: np.ndarray,
    p_min: np.ndarray,
    p_max: np.ndarray,
    max_iter: int = 200,
) -> np.ndarray:
    """Project p onto [p_min, p_max] ∩ {Σp_i = 1}.

    Uses capacity-proportional redistribution: at each step, the deficit
    (1 - Σp) is distributed proportionally to each component's available
    headroom (toward p_max if deficit > 0, toward p_min if deficit < 0).
    This converges for all well-conditioned bounds where sum(p_min) ≤ 1 ≤ sum(p_max).
    """
    p = np.clip(p.astype(float), p_min, p_max)
    for _ in range(max_iter):
        s = p.sum()
        deficit = 1.0 - s
        if abs(deficit) < 1e-12:
            break
        if deficit > 0:
            capacity = p_max - p
        else:
            capacity = p - p_min
        total_capacity = capacity.sum()
        if total_capacity < 1e-14:
            break  # infeasible — bounds don't allow reaching sum=1
        p += deficit * (capacity / total_capacity)
        p = np.clip(p, p_min, p_max)
    return p


def _proportions_to_counts(props: np.ndarray, total: int) -> np.ndarray:
    """Convert float proportions to integer counts summing exactly to total."""
    counts_f = props * total
    counts = np.floor(counts_f).astype(int)
    remainder = total - int(counts.sum())
    if remainder > 0:
        # Distribute remainder to the categories with largest fractional parts
        frac = counts_f - counts
        top = np.argsort(-frac)[:remainder]
        counts[top] += 1
    return counts


# ---------------------------------------------------------------------------
# Sampling strategies
# ---------------------------------------------------------------------------

def _sample_dirichlet(
    categories: list[str],
    p_min: np.ndarray,
    p_max: np.ndarray,
    concentration: float,
    total: int,
    rng: np.random.Generator,
) -> dict[str, int]:
    """Bounded Dirichlet sampling with simplex-box projection."""
    # Centre the Dirichlet prior at the midpoint of each category's bounds
    midpoints = (p_min + p_max) / 2.0
    alpha = np.maximum(concentration * midpoints, 1e-6)
    raw_p = rng.dirichlet(alpha)
    p = _project_simplex_box(raw_p, p_min, p_max)
    counts = _proportions_to_counts(p, total)
    return dict(zip(categories, counts.tolist()))


def _sample_uniform_renorm(
    categories: list[str],
    p_min: np.ndarray,
    p_max: np.ndarray,
    total: int,
    rng: np.random.Generator,
) -> dict[str, int]:
    """Uniform draw in [p_min, p_max] per category, renormalized to sum=1."""
    raw_p = rng.uniform(p_min, p_max)
    p = _project_simplex_box(raw_p, p_min, p_max)
    counts = _proportions_to_counts(p, total)
    return dict(zip(categories, counts.tolist()))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sample_composition(
    pool: ArchetypePool,
    cfg: ScenarioConfig,
    rng: np.random.Generator,
) -> NeighborhoodComposition:
    """Sample a valid integer composition from the archetype pool.

    Handles fixed_count categories (removed from stochastic sampling),
    then applies the configured method (dirichlet or uniform_renorm) to
    the remaining categories.
    """
    comp_cfg = cfg.composition
    all_cats = list(comp_cfg.categories.keys())

    # Separate fixed-count categories from variable ones
    fixed: dict[str, int] = {}
    variable_cats: list[str] = []
    for cat in all_cats:
        cat_cfg = comp_cfg.categories[cat]
        if cat_cfg.fixed_count is not None:
            fixed[cat] = cat_cfg.fixed_count
        else:
            variable_cats.append(cat)

    fixed_total = sum(fixed.values())
    variable_total = comp_cfg.total_buildings - fixed_total

    counts: dict[str, int] = dict(fixed)

    if variable_cats and variable_total > 0:
        p_min = np.array([comp_cfg.categories[c].min_fraction for c in variable_cats])
        p_max = np.array([comp_cfg.categories[c].max_fraction for c in variable_cats])

        if comp_cfg.method == "dirichlet":
            var_counts = _sample_dirichlet(
                variable_cats, p_min, p_max,
                comp_cfg.dirichlet_concentration, variable_total, rng
            )
        else:
            var_counts = _sample_uniform_renorm(
                variable_cats, p_min, p_max, variable_total, rng
            )
        counts.update(var_counts)
    elif variable_cats:
        # variable_total == 0: all buildings are fixed-count
        for cat in variable_cats:
            counts[cat] = 0

    total = sum(counts.values())
    proportions = {cat: (cnt / total if total > 0 else 0.0) for cat, cnt in counts.items()}

    return NeighborhoodComposition(counts=counts, proportions=proportions)


def sample_buildings(
    composition: NeighborhoodComposition,
    pool: ArchetypePool,
    cfg: ScenarioConfig,
    rng: np.random.Generator,
    resstock_chars: "pd.DataFrame | None" = None,
) -> list[BuildingSelection]:
    """Draw building instances for each category using Layer 2 RNG.

    For each category, samples `count` buildings with replacement from
    the filtered pool. Resolves multifamily unit multipliers.
    """
    import pandas as pd

    selections: list[BuildingSelection] = []
    comp_cfg = cfg.composition

    for cat, count in composition.counts.items():
        if count == 0:
            continue

        cat_cfg = comp_cfg.categories.get(cat)
        source = cat_cfg.source if cat_cfg else "resstock"

        if source == "resstock":
            bldg_pool = pool.residential.get(cat, [])
        else:
            bldg_pool = pool.commercial.get(cat, [])

        if not bldg_pool:
            raise ValueError(
                f"Category '{cat}' has no buildings in the filtered pool. "
                "Check archetype_filters in your config."
            )

        chosen_ids: list[BuildingId] = rng.choice(
            bldg_pool, size=count, replace=True
        ).tolist()

        for bldg_id in chosen_ids:
            unit_multiplier = 1.0
            if source == "resstock" and resstock_chars is not None:
                match = resstock_chars.loc[
                    resstock_chars["bldg_id"] == bldg_id,
                    "in.geometry_building_number_units_mf",
                ].values
                if len(match) > 0 and not pd.isna(match[0]):
                    unit_multiplier = float(match[0])

            selections.append(BuildingSelection(
                bldg_id=bldg_id,
                category=cat,
                source=source,
                unit_multiplier=unit_multiplier,
            ))

    return selections
