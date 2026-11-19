"""QA (W1-COMPOSE review): engine count rules for fixed counts vs ``total_buildings`` (FR-ENG-2, FR-ENG-3).

The API's composition path refuses fixed counts above the total and all-fixed counts that do not add up to the
total; the engine's own parser does not. These are library defects for the engine role, recorded strictly.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from microgrid.community_load.synthetic._types import ArchetypePool
from microgrid.community_load.synthetic.composition import sample_composition
from microgrid.community_load.synthetic.config import config_from_dict


def _cfg(total: int, categories: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "scenario": {"name": "qa"},
        "composition": {"total_buildings": total, "categories": categories},
        "ensemble": {"mode": "fixed", "max_runs": 2},
    }


OVER_TOTAL = {
    "a": {"fixed_count": 6, "source": "resstock"},
    "b": {"min_fraction": 0.0, "max_fraction": 1.0, "source": "resstock"},
}
ALL_FIXED_SHORT = {"a": {"fixed_count": 1, "source": "resstock"}, "b": {"fixed_count": 1, "source": "resstock"}}


def _pool() -> ArchetypePool:
    return ArchetypePool(residential={}, commercial={}, resstock_chars=pd.DataFrame(), comstock_chars=pd.DataFrame())


@pytest.mark.parametrize("categories", [OVER_TOTAL, ALL_FIXED_SHORT], ids=["fixed above total", "all fixed short"])
def test_engine_today_samples_a_neighbourhood_of_the_wrong_size(categories):
    """Evidence: the parser accepts these and sample_composition returns counts that do not sum to the total."""
    cfg = config_from_dict(_cfg(4, categories), source="<qa>")
    counts = sample_composition(_pool(), cfg, np.random.default_rng(0)).counts
    assert sum(counts.values()) != 4, counts


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (MEDIUM, engine): config_from_dict accepts fixed counts that add up to more than "
    "total_buildings; sample_composition then returns more buildings than total_buildings (the ranged categories "
    "get 0). Fix in synthetic/config.py validation: sum(fixed_count) must not exceed total_buildings.",
)
def test_engine_refuses_fixed_counts_above_total():
    with pytest.raises(ValueError, match="total_buildings"):
        config_from_dict(_cfg(4, OVER_TOTAL), source="<qa>")


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (MEDIUM, engine): config_from_dict accepts an all-fixed composition whose counts do not add up "
    "to total_buildings; the sampled neighbourhood then has sum(fixed_count) buildings, not total_buildings "
    "(FR-ENG-2 AC: counts sum exactly to total_buildings). Fix: require equality when no category is ranged.",
)
def test_engine_refuses_all_fixed_counts_not_equal_to_total():
    with pytest.raises(ValueError, match="total_buildings"):
        config_from_dict(_cfg(4, ALL_FIXED_SHORT), source="<qa>")
