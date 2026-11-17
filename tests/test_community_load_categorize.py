"""Bridge from geography to building types (see also test_registry.py)."""

from __future__ import annotations

from microgrid.community_load.categorize import category_counts
from microgrid.community_load.registry import default_registry
from microgrid.geo.types import Structure, UseClass


def _s(i, use, stories=None):
    return Structure(f"s{i}", use, 100.0, (-77.0, 38.9), stories=stories)


def test_category_counts_follow_story_thresholds_and_report_skipped():
    structures = [
        _s(0, UseClass.RESIDENTIAL_MF, 2),
        _s(1, UseClass.RESIDENTIAL_MF, 5),
        _s(2, UseClass.RESIDENTIAL_MF, 12),
        _s(3, UseClass.COMMERCIAL),
        _s(4, UseClass.RESIDENTIAL_SF),
        _s(5, UseClass.INDUSTRIAL),
    ]
    counts, skipped = category_counts(structures)
    assert counts == {"mf_small": 1, "mf_mid": 1, "mf_large": 1, "commercial_private": 1, "sf_detached": 1}
    assert skipped == {"industrial": 1}


def test_unknown_mf_stories_use_documented_default():
    s = _s(0, UseClass.RESIDENTIAL_MF)
    assert category_counts([s], default_registry().with_assumed_stories(6))[0] == {"mf_mid": 1}
