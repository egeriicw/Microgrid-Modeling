"""Synthetic load for a GIS-built Neighborhood via the microgrid.community_load.synthetic engine.

The real structure counts fix the composition (every category gets a fixed_count);
microgrid.community_load.synthetic's remaining randomness (archetype draw, perturbation) yields the P10/P50/P90 band.
"""
from __future__ import annotations

import copy
from pathlib import Path

from microgrid.core.types import LoadResult, Neighborhood
from microgrid.community_load.geo.categorize import CategoryRules, category_counts
from microgrid.community_load.synthetic.config import CategoryBounds, ScenarioConfig, load_config


def scenario_for_neighborhood(
    base: ScenarioConfig, neighborhood: Neighborhood, rules: CategoryRules = CategoryRules()
) -> tuple[ScenarioConfig, dict[str, int]]:
    """Copy `base`, replacing its composition with the neighborhood's fixed counts.

    Returns (config, skipped) where `skipped` counts structures with no microgrid.community_load.synthetic archetype.
    Raises ValueError if the neighborhood has a category the base config has no filters for,
    or has no modelable structures at all.
    """
    counts, skipped = category_counts(neighborhood.structures, rules)
    if not counts:
        raise ValueError(f"Neighborhood '{neighborhood.name}' has no structures microgrid.community_load.synthetic can model")
    known = base.composition.categories
    missing = sorted(c for c in counts if c not in known)
    if missing:
        raise ValueError(f"Base scenario has no category definition for: {', '.join(missing)}")
    cfg = copy.deepcopy(base)
    cfg.name = f"{base.name} :: {neighborhood.name}"
    cfg.composition.total_buildings = sum(counts.values())
    cfg.composition.categories = {
        c: CategoryBounds(fixed_count=n, source=known[c].source) for c, n in counts.items()
    }
    return cfg, skipped


class SnlgEnsembleSource:
    """Runs an microgrid.community_load.synthetic ensemble for the neighborhood. Needs ResStock/ComStock data on disk."""

    def __init__(self, base_config: Path | str, rules: CategoryRules = CategoryRules()):
        self.base_config, self.rules = Path(base_config), rules

    def profile(self, neighborhood: Neighborhood) -> LoadResult:
        # Imported here so microgrid.community_load.geo / microgrid.core work without parquet data or pyarrow.
        from microgrid.community_load.synthetic import archetype, ensemble

        base = load_config(self.base_config)
        cfg, skipped = scenario_for_neighborhood(base, neighborhood, self.rules)
        bg = Path(cfg.data.background_dir)
        pool = archetype.build_archetype_pool(
            bg / cfg.data.resstock_chars_file, bg / cfg.data.comstock_chars_file, cfg
        )
        result = ensemble.run_ensemble(
            pool,
            cfg,
            {"resstock": Path(cfg.data.resstock_timeseries_dir),
             "comstock": Path(cfg.data.comstock_timeseries_dir)},
        )
        env = result.percentile_envelopes
        if "p50" not in env:
            raise RuntimeError("microgrid.community_load.synthetic produced no feasible runs; cannot form a P50 profile")
        return LoadResult(
            profile_kw=env["p50"],
            p10_kw=env.get("p10"),
            p90_kw=env.get("p90"),
            source="snlg-ensemble",
            metadata={
                "neighborhood": neighborhood.name,
                "n_feasible_runs": len(result.runs),
                "converged": result.converged,
                "unmodeled_structures": skipped,
            },
        )
