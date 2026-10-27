"""GIS layer: geometry, classification, neighborhood build, microgrid.community_load.synthetic category bridge."""
from __future__ import annotations

import json

import pytest

from microgrid.core.types import UseClass
from microgrid.community_load.geo import (
    CategoryRules, build_neighborhood, category_counts, classify_use, read_structures,
)
from microgrid.community_load.geo.geometry import point_in_ring, ring_area_m2, ring_centroid

LAT, LON, D = 38.90, -77.00, 0.0001  # D degrees ~ 11.1 m N-S, ~8.6 m E-W at this latitude


def _square(lon, lat, side=D):
    return [[lon, lat], [lon + side, lat], [lon + side, lat + side], [lon, lat + side], [lon, lat]]


def _feature(fid, lon, lat, **props):
    return {"type": "Feature", "properties": {"id": fid, **props},
            "geometry": {"type": "Polygon", "coordinates": [_square(lon, lat)]}}


@pytest.fixture
def layer(tmp_path):
    feats = [
        _feature("a", LON, LAT, building="apartments", **{"building:levels": "2"}),
        _feature("b", LON + 0.001, LAT, building="apartments", **{"building:levels": "5"}),
        _feature("c", LON + 0.002, LAT, building="apartments", **{"building:levels": "12"}),
        _feature("d", LON + 0.003, LAT, building="retail"),
        _feature("e", LON + 0.004, LAT, building="house"),
        _feature("far", LON + 0.5, LAT, building="retail"),            # outside boundary
        {"type": "Feature", "properties": {"id": "pt"},
         "geometry": {"type": "Point", "coordinates": [LON, LAT]}},    # skipped
    ]
    p = tmp_path / "structures.geojson"
    p.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    return p


BOUNDARY = ((LON - 0.001, LAT - 0.001), (LON + 0.01, LAT - 0.001),
            (LON + 0.01, LAT + 0.01), (LON - 0.001, LAT + 0.01))


def test_ring_area_close_to_expected_metres():
    area = ring_area_m2(tuple(map(tuple, _square(LON, LAT))))
    assert 90 < area < 102  # 11.1 m x 8.65 m


def test_centroid_and_point_in_ring():
    ring = tuple(map(tuple, _square(LON, LAT)))
    cx, cy = ring_centroid(ring)
    assert cx == pytest.approx(LON + D / 2, abs=1e-7)
    assert cy == pytest.approx(LAT + D / 2, abs=1e-7)
    assert point_in_ring((cx, cy), ring)
    assert not point_in_ring((LON + 1, LAT), ring)


@pytest.mark.parametrize("raw,expected", [
    ("Apartments", UseClass.RESIDENTIAL_MF), ("house", UseClass.RESIDENTIAL_SF),
    ("retail", UseClass.COMMERCIAL), ("warehouse", UseClass.INDUSTRIAL),
    ("school", UseClass.INSTITUTIONAL), ("garage", UseClass.OTHER),
    ("spaceship", UseClass.UNKNOWN), (None, UseClass.UNKNOWN),
])
def test_classify_use(raw, expected):
    assert classify_use(raw) is expected


def test_read_structures_skips_points_and_parses_attributes(layer):
    got = {s.id: s for s in read_structures(layer)}
    assert "pt" not in got and len(got) == 6
    assert got["a"].stories == 2 and got["a"].use is UseClass.RESIDENTIAL_MF
    assert got["d"].stories is None


def test_build_neighborhood_clips_by_centroid(layer):
    n = build_neighborhood("test", layer, BOUNDARY)
    assert {s.id for s in n.structures} == {"a", "b", "c", "d", "e"}
    assert n.count_by_use()[UseClass.RESIDENTIAL_MF] == 3


def test_build_neighborhood_rejects_degenerate_boundary(layer):
    with pytest.raises(ValueError):
        build_neighborhood("bad", layer, BOUNDARY[:2])


def test_category_counts_follow_story_thresholds_and_report_skipped(layer):
    n = build_neighborhood("test", layer, BOUNDARY)
    counts, skipped = category_counts(n.structures)
    assert counts == {"mf_small": 1, "mf_mid": 1, "mf_large": 1, "commercial_private": 1}
    assert skipped == {"residential_sf": 1}


def test_unknown_mf_stories_use_documented_default(layer):
    from microgrid.core.types import Structure
    s = Structure("x", UseClass.RESIDENTIAL_MF, 100.0, (LON, LAT), stories=None)
    assert category_counts([s], CategoryRules(default_mf_stories=6))[0] == {"mf_mid": 1}
