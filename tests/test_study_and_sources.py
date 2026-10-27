"""Load sources, microgrid.community_load.synthetic scenario bridge, and Study pipeline."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from microgrid.geo.types import Neighborhood, Structure, UseClass
from microgrid.core.types import LoadResult
from microgrid.der.base import DERResult
from microgrid.community_load.sources import MeasuredLoadSource, scenario_for_neighborhood
from microgrid.study import Study
from microgrid.community_load.synthetic.config import load_config

BASELINE = Path(__file__).parent.parent / "configs/scenarios/dc_multifamily_baseline.yaml"


def _nbhd(*specs):
    ss = tuple(Structure(f"s{i}", use, 100.0, (-77.0, 38.9), stories=st)
               for i, (use, st) in enumerate(specs))
    return Neighborhood("n", ((0, 0), (1, 0), (1, 1)), ss)


def test_scenario_bridge_fixes_every_category():
    n = _nbhd((UseClass.RESIDENTIAL_MF, 2), (UseClass.RESIDENTIAL_MF, 2),
              (UseClass.RESIDENTIAL_MF, 9), (UseClass.COMMERCIAL, None),
              (UseClass.INDUSTRIAL, None))
    cfg, skipped = scenario_for_neighborhood(load_config(BASELINE), n)
    cats = cfg.composition.categories
    assert {c: b.fixed_count for c, b in cats.items()} == {
        "mf_small": 2, "mf_large": 1, "commercial_private": 1}
    assert cats["commercial_private"].source == "comstock"
    assert cfg.composition.total_buildings == 4
    assert skipped == {"industrial": 1}


def test_scenario_bridge_does_not_mutate_base():
    base = load_config(BASELINE)
    before = (base.composition.total_buildings, dict(base.composition.categories))
    scenario_for_neighborhood(base, _nbhd((UseClass.RESIDENTIAL_MF, 2)))
    assert (base.composition.total_buildings, dict(base.composition.categories)) == before


def test_scenario_bridge_rejects_unmodelable_neighborhood():
    with pytest.raises(ValueError, match="no structures"):
        scenario_for_neighborhood(load_config(BASELINE), _nbhd((UseClass.RESIDENTIAL_SF, 1)))


def test_measured_source_resamples_to_hourly(tmp_path):
    ts = pd.date_range("2023-01-01", periods=8, freq="30min")
    csv = tmp_path / "m.csv"
    pd.DataFrame({"timestamp": ts, "kw": [10, 20, 30, 40, 50, 60, 70, 80]}).to_csv(csv, index=False)
    r = MeasuredLoadSource(csv).profile(_nbhd((UseClass.COMMERCIAL, None)))
    assert list(r.profile_kw) == [15.0, 35.0, 55.0, 75.0]
    assert r.peak_kw == 75.0 and r.source == "measured"


def test_measured_source_rejects_missing_values(tmp_path):
    csv = tmp_path / "m.csv"
    csv.write_text("timestamp,kw\n2023-01-01 00:00,1\n2023-01-01 01:00,\n")
    with pytest.raises(ValueError, match="missing kW"):
        MeasuredLoadSource(csv).profile(_nbhd((UseClass.COMMERCIAL, None)))


class _Flat:
    def __init__(self, idx): self.idx = idx
    def profile(self, n): return LoadResult(pd.Series(100.0, index=self.idx), "flat")


class _Shave:
    name = "shave"
    def __init__(self, idx): self.idx = idx
    def simulate(self, n, load): return DERResult("shave", pd.Series(-30.0, index=self.idx))


def test_study_sums_der_into_net_load():
    idx = pd.date_range("2023-01-01", periods=24, freq="1h")
    res = Study(_nbhd((UseClass.COMMERCIAL, None)), _Flat(idx), [_Shave(idx)]).run()
    assert res.load.peak_kw == 100.0 and res.net_peak_kw == 70.0


def test_study_rejects_misaligned_der():
    idx = pd.date_range("2023-01-01", periods=24, freq="1h")
    other = pd.date_range("2024-01-01", periods=24, freq="1h")
    with pytest.raises(ValueError, match="index"):
        Study(_nbhd((UseClass.COMMERCIAL, None)), _Flat(idx), [_Shave(other)]).run()
