"""Client compositions -> engine scenarios (FR-ENG-1, FR-ENG-2, FR-WEB-1).

A client describes a neighbourhood by building-type keys from the registry, each with a fixed count or a fraction
range, plus run settings. The SERVER turns that into a full engine scenario: composition categories named after the
types, each type's default sampling filters from the registry (the library's ``with_default_filters``), and the
ensemble settings. The client never sends filters, columns or paths.

The scenario is produced with the library's own ``config_to_dict`` so every engine default is written out explicitly
(nothing is left to a hidden default) and the result round-trips through ``config_from_dict``. Server-owned parts are
removed: `data` and `output.base_dir` (the run router adds the server-built ones) and `rng` (seeds are resolved when
the run is stored).
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from microgrid.community_load.registry import BuildingTypeRegistry, with_default_filters
from microgrid.community_load.synthetic.config import EnsembleConfig, ScenarioConfig, config_from_dict, config_to_dict

#: The engine's default ``ensemble.min_runs`` (read from the engine, not copied): with convergence on, a run needs
#: at least this many runs unless the client sets ``min_runs``.
DEFAULT_MIN_RUNS = EnsembleConfig().min_runs


class CompositionError(ValueError):
    """The composition cannot be turned into a scenario. Messages name only the client's own inputs."""


@dataclass(frozen=True)
class Entry:
    building_type: str
    fixed_count: int | None
    min_fraction: float | None
    max_fraction: float | None


@dataclass(frozen=True)
class Composition:
    name: str
    total_buildings: int
    entries: tuple[Entry, ...]
    max_runs: int
    convergence: bool = False
    convergence_rule: str | None = None
    min_runs: int | None = None


_TOL = 1e-9


def check_counts(comp: Composition) -> None:
    """FR-ENG-2 feasibility, stated in the client's terms before the engine sees it."""
    check_category_counts(comp.total_buildings, comp.entries)


def check_config_counts(cfg: ScenarioConfig) -> None:
    """The same rules for a parsed engine scenario, whatever the run input (`config`, `composition_id`,
    `composition`). The engine itself does not check fixed counts against the total (engine fix pending the owner),
    so the API does, so that no run input can sample more or fewer buildings than total_buildings."""
    entries = tuple(
        Entry(
            name,
            b.fixed_count,
            None if b.fixed_count is not None else b.min_fraction,
            None if b.fixed_count is not None else b.max_fraction,
        )
        for name, b in cfg.composition.categories.items()
    )
    check_category_counts(cfg.composition.total_buildings, entries)


def check_category_counts(total: int, entries: tuple[Entry, ...]) -> None:
    """Fixed counts are taken first; the ranged categories share the remainder by fraction. So: fixed counts may not
    exceed the total; with no ranged category they must equal it (otherwise the counts could not sum to the total);
    and the ranged fractions must be able to sum to 1 (sum of minimums <= 1 <= sum of maximums)."""
    fixed = sum(e.fixed_count for e in entries if e.fixed_count is not None)
    ranged = [e for e in entries if e.fixed_count is None]
    if fixed > total:
        raise CompositionError(f"the fixed counts add up to {fixed}, more than total_buildings ({total})")
    if not ranged and fixed != total:
        raise CompositionError(
            f"every type has a fixed count and they add up to {fixed}, not total_buildings ({total}); "
            "make them add up or give at least one type a fraction range"
        )
    if ranged:
        lo = sum(e.min_fraction or 0.0 for e in ranged)
        hi = sum(1.0 if e.max_fraction is None else e.max_fraction for e in ranged)
        if lo > 1.0 + _TOL:
            raise CompositionError(
                f"the minimum fractions of the ranged types add up to {lo:.4g}, more than 1; no mix can satisfy them"
            )
        if hi < 1.0 - _TOL:
            raise CompositionError(
                f"the maximum fractions of the ranged types add up to {hi:.4g}, less than 1; no mix can satisfy them"
            )


def build_scenario(comp: Composition, registry: BuildingTypeRegistry) -> dict[str, Any]:
    """The engine scenario for ``comp`` (no `data`, no `output.base_dir`, no `rng`). CompositionError on an unknown,
    inactive or repeated type, or counts/fractions that cannot work."""
    seen: set[str] = set()
    categories: dict[str, dict[str, Any]] = {}
    for e in comp.entries:
        if e.building_type in seen:
            raise CompositionError(f"building type {e.building_type!r} is listed more than once")
        seen.add(e.building_type)
        if e.building_type not in registry:
            raise CompositionError(
                f"unknown building type {e.building_type!r}; see GET /building-types for the catalogue"
            )
        t = registry.get(e.building_type)
        if not t.active:
            raise CompositionError(f"building type {e.building_type!r} is not active yet and cannot be modelled")
        bounds: dict[str, Any] = {"source": t.source}
        if e.fixed_count is not None:
            bounds["fixed_count"] = e.fixed_count
        else:
            bounds["min_fraction"] = 0.0 if e.min_fraction is None else e.min_fraction
            bounds["max_fraction"] = 1.0 if e.max_fraction is None else e.max_fraction
        categories[t.key] = bounds
    check_counts(comp)
    if comp.convergence:
        min_runs = DEFAULT_MIN_RUNS if comp.min_runs is None else comp.min_runs
        if min_runs > comp.max_runs:
            raise CompositionError(
                f"with convergence on, max_runs must be at least min_runs ({DEFAULT_MIN_RUNS} unless you set it); "
                f"got max_runs {comp.max_runs}"
            )

    ensemble: dict[str, Any] = {"mode": "adaptive" if comp.convergence else "fixed", "max_runs": comp.max_runs}
    if comp.min_runs is not None:
        ensemble["min_runs"] = comp.min_runs
    raw: dict[str, Any] = {
        "scenario": {"name": comp.name},
        "composition": {"total_buildings": comp.total_buildings, "categories": categories},
        "ensemble": ensemble,
    }
    if comp.convergence_rule is not None:
        raw["convergence"] = {"rule": comp.convergence_rule}
    try:
        parsed = config_from_dict(copy.deepcopy(raw), source="<composition>")
        parsed = with_default_filters(parsed, registry)
    except ValueError as exc:
        raise CompositionError(str(exc)) from None
    scenario = config_to_dict(parsed)
    for key in ("data", "rng"):
        scenario.pop(key, None)
    scenario["output"] = {k: v for k, v in scenario["output"].items() if k != "base_dir"}
    return scenario
