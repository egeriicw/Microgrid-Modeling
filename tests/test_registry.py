"""Building-type registry (FR-ENG-1, Sprint 1 story 1.5): types are data, not code."""

from __future__ import annotations

import pandas as pd
import pytest

from microgrid.community_load.categorize import category_counts
from microgrid.community_load.registry import (
    BuildingTypeRegistry,
    default_registry,
    with_default_filters,
)
from microgrid.community_load.synthetic.archetype import _filter_resstock_category
from microgrid.community_load.synthetic.config import CategoryBounds, CompositionConfig, ScenarioConfig
from microgrid.geo.types import Structure, UseClass

MVP_TYPES = {
    "sf_detached",
    "sf_attached",
    "mf_small",
    "mf_mid",
    "mf_large",
    "commercial_private",
    "commercial_warehouse",
    "institutional_school",
    "institutional_hospital",
}


def _s(use, stories=None, subtype=None, i=0):
    return Structure(f"s{i}", use, 100.0, (-77.0, 38.9), stories=stories, subtype=subtype)


def _key(structure, registry=None):
    counts, _ = category_counts([structure], registry or default_registry())
    return next(iter(counts), None)


# --- contents -------------------------------------------------------------------------------


def test_default_registry_contains_the_mvp_types():
    reg = default_registry()
    assert MVP_TYPES <= {t.key for t in reg.active_types()}


def test_civic_is_listed_but_inactive_until_its_comstock_mapping_is_decided():
    reg = default_registry()
    assert reg.get("civic").active is False


def test_every_active_type_defines_how_to_filter_its_source_data():
    for t in default_registry().active_types():
        assert t.resstock_building_type_heights or t.comstock_building_types, t.key


# --- categorisation -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "structure,expected",
    [
        (_s(UseClass.RESIDENTIAL_SF), "sf_detached"),
        (_s(UseClass.RESIDENTIAL_SFA), "sf_attached"),
        (_s(UseClass.RESIDENTIAL_MF, 1), "mf_small"),
        (_s(UseClass.RESIDENTIAL_MF, 3), "mf_small"),
        (_s(UseClass.RESIDENTIAL_MF, 4), "mf_mid"),
        (_s(UseClass.RESIDENTIAL_MF, 7), "mf_mid"),
        (_s(UseClass.RESIDENTIAL_MF, 8), "mf_large"),
        (_s(UseClass.COMMERCIAL), "commercial_private"),
        (_s(UseClass.WAREHOUSE), "commercial_warehouse"),
        (_s(UseClass.INSTITUTIONAL, subtype="school"), "institutional_school"),
        (_s(UseClass.INSTITUTIONAL, subtype="hospital"), "institutional_hospital"),
        (_s(UseClass.INDUSTRIAL), None),
        (_s(UseClass.OTHER), None),
        (_s(UseClass.UNKNOWN), None),
        (_s(UseClass.INSTITUTIONAL, subtype="government"), None),  # civic is inactive
        (_s(UseClass.INSTITUTIONAL), None),  # unknown institutional subtype is never guessed
    ],
)
def test_structures_map_to_registry_types(structure, expected):
    assert _key(structure) == expected


def test_unmodeled_structures_are_reported_not_dropped_silently():
    structures = [_s(UseClass.INDUSTRIAL, i=1), _s(UseClass.INDUSTRIAL, i=2), _s(UseClass.RESIDENTIAL_SF, i=3)]
    counts, skipped = category_counts(structures)
    assert counts == {"sf_detached": 1}
    assert skipped == {"industrial": 2}


def test_unknown_stories_use_the_registry_default():
    reg = default_registry().with_assumed_stories(6)
    assert _key(_s(UseClass.RESIDENTIAL_MF), reg) == "mf_mid"


# --- extensibility: a new type needs no engine change ---------------------------------------


def _with_micro() -> BuildingTypeRegistry:
    return default_registry().extended(
        {
            "key": "mf_micro",
            "label": "Single-storey small multifamily",
            "source": "resstock",
            "use": "residential_mf",
            "min_stories": 1,
            "max_stories": 1,
            "resstock_building_type_heights": ["Multi-Family with 2 - 4 Units"],
        }
    )


def test_a_new_type_is_picked_up_by_categorisation_without_touching_engine_code():
    reg = _with_micro()
    # more specific (narrower story range) wins over the broad mf_small
    assert _key(_s(UseClass.RESIDENTIAL_MF, 1), reg) == "mf_micro"
    assert _key(_s(UseClass.RESIDENTIAL_MF, 2), reg) == "mf_small"


def test_default_filters_are_filled_in_for_categories_that_have_none():
    reg = _with_micro()
    cfg = ScenarioConfig(
        name="x",
        composition=CompositionConfig(
            total_buildings=3,
            categories={"mf_micro": CategoryBounds(fixed_count=3, source="resstock")},
        ),
    )
    filled = with_default_filters(cfg, reg)
    assert filled.archetype_filters.resstock["mf_micro"].building_type_heights == ["Multi-Family with 2 - 4 Units"]
    assert "mf_micro" not in cfg.archetype_filters.resstock  # input is not mutated


def test_explicit_filters_in_the_config_win_over_registry_defaults():
    from microgrid.community_load.synthetic.config import ResidentialFilterConfig

    cfg = ScenarioConfig(
        name="x",
        composition=CompositionConfig(
            total_buildings=3, categories={"mf_small": CategoryBounds(fixed_count=3, source="resstock")}
        ),
    )
    cfg.archetype_filters.resstock["mf_small"] = ResidentialFilterConfig(building_type_heights=["custom"])
    filled = with_default_filters(cfg, default_registry())
    assert filled.archetype_filters.resstock["mf_small"].building_type_heights == ["custom"]


def test_unknown_category_with_no_filters_is_a_clear_error():
    cfg = ScenarioConfig(
        name="x",
        composition=CompositionConfig(
            total_buildings=3, categories={"nope": CategoryBounds(fixed_count=3, source="resstock")}
        ),
    )
    with pytest.raises(ValueError, match="nope"):
        with_default_filters(cfg, default_registry())


def test_registry_defaults_drive_the_engines_own_filter_function():
    reg = _with_micro()
    cfg = ScenarioConfig(
        name="x",
        composition=CompositionConfig(
            total_buildings=1, categories={"sf_detached": CategoryBounds(fixed_count=1, source="resstock")}
        ),
    )
    f = with_default_filters(cfg, reg).archetype_filters.resstock["sf_detached"]
    df = pd.DataFrame(
        {
            "bldg_id": [1, 2],
            "in.geometry_building_type_height": ["Single-Family Detached", "Single-Family Attached"],
            "completed_status": ["Success", "Success"],
            "applicability": [True, True],
        }
    )
    assert _filter_resstock_category(df, f) == [1]


# --- validation of registry data ------------------------------------------------------------


def test_duplicate_keys_are_rejected():
    base = {
        "key": "a",
        "label": "A",
        "source": "resstock",
        "use": "residential_sf",
        "resstock_building_type_heights": ["x"],
    }
    with pytest.raises(ValueError, match="duplicate"):
        BuildingTypeRegistry.from_dict({"building_types": [base, dict(base)]})


@pytest.mark.parametrize(
    "patch,needle",
    [
        ({"source": "other"}, "source"),
        ({"use": "spaceship"}, "use"),
        ({"min_stories": 5, "max_stories": 2}, "stories"),
        ({"bogus": 1}, "bogus"),
        ({"resstock_building_type_heights": []}, "filter"),
    ],
)
def test_invalid_registry_entries_are_rejected_with_a_reason(patch, needle):
    entry = {
        "key": "a",
        "label": "A",
        "source": "resstock",
        "use": "residential_sf",
        "resstock_building_type_heights": ["x"],
        **patch,
    }
    with pytest.raises(ValueError, match=needle):
        BuildingTypeRegistry.from_dict({"building_types": [entry]})
