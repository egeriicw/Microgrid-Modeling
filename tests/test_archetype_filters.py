"""Residential archetype filtering, including the completeness gate (FR-ENG-3 / FR-ENG-11 groundwork)."""

from __future__ import annotations

import pandas as pd
import pytest

from microgrid.community_load.synthetic.archetype import _filter_resstock_category
from microgrid.community_load.synthetic.config import ResidentialFilterConfig

MF = "Multi-Family with 5+ Units, 1-3 Stories"


@pytest.fixture
def chars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5],
            "in.geometry_building_type_height": [MF] * 5,
            "completed_status": ["Success", "Success", "Fail", "Invalid", "Success"],
            "applicability": [True, True, True, False, False],
            "in.sqft..ft2": [800, 900, 1000, 1100, 1200],
        }
    )


def _ids(chars, **kw) -> list[int]:
    cfg = ResidentialFilterConfig(building_type_heights=[MF], **kw)
    return _filter_resstock_category(chars, cfg)


def test_complete_gate_keeps_only_successful_applicable_buildings(chars):
    assert _ids(chars, require_data_complete=True) == [1, 2]


def test_gate_off_keeps_everything(chars):
    assert _ids(chars, require_data_complete=False) == [1, 2, 3, 4, 5]


def test_gate_defaults_on(chars):
    assert _ids(chars) == [1, 2]


def test_gate_combines_with_other_filters(chars):
    assert _ids(chars, require_data_complete=True, max_sqft=850) == [1]


def test_status_match_is_case_insensitive_and_applicability_may_be_text():
    df = pd.DataFrame(
        {
            "bldg_id": [1, 2],
            "in.geometry_building_type_height": [MF, MF],
            "completed_status": ["success", "SUCCESS"],
            "applicability": ["True", "False"],
        }
    )
    assert _ids(df) == [1]


def test_missing_status_columns_fail_loudly_when_the_gate_is_on(chars):
    bare = chars.drop(columns=["completed_status", "applicability"])
    with pytest.raises(ValueError, match="completed_status"):
        _ids(bare, require_data_complete=True)
    assert _ids(bare, require_data_complete=False) == [1, 2, 3, 4, 5]
