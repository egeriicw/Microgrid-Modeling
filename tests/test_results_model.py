"""FR-ENG-5 / FR-ENG-7: the structured results model (envelopes, splits, load-duration curve, seasonal profiles)."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from microgrid.community_load.synthetic import loader, run
from microgrid.community_load.synthetic._types import (
    BuildingSelection,
    EnsembleResult,
    NeighborhoodComposition,
    NeighborhoodResult,
)
from microgrid.community_load.synthetic.results import SCHEMA_VERSION, build_results
from tests.test_run_facade import build_tiny_config

H_IDX = pd.date_range("2018-01-01", periods=8760, freq="h")
Q_IDX = pd.date_range("2018-01-01", periods=35040, freq="15min")


def _nr(
    i: int,
    res_q: np.ndarray | None,
    com_q: np.ndarray | None,
    cats: dict[str, tuple[str, np.ndarray]] | None = None,
    feasible: bool = True,
) -> NeighborhoodResult:
    """A run built from 15-minute kWh arrays; hourly = sum of 4 quarters (the engine's own relation)."""
    n = 35040
    rq = np.zeros(n) if res_q is None else res_q
    cq = np.zeros(n) if com_q is None else com_q
    res_h = pd.Series(rq.reshape(-1, 4).sum(axis=1), index=H_IDX)
    com_h = pd.Series(cq.reshape(-1, 4).sum(axis=1), index=H_IDX)
    tot = res_h + com_h
    cats = cats or {}
    return NeighborhoodResult(
        run_index=i,
        residential_profile=res_h,
        commercial_profile=com_h,
        total_profile=tot,
        composition=NeighborhoodComposition(counts={}, proportions={}),
        selections=[
            BuildingSelection(bldg_id=1, category=c, source=src, unit_multiplier=1.0) for c, (src, _) in cats.items()
        ],
        peak_kw=float(tot.max()),
        annual_energy_kwh=float(tot.sum()),
        load_factor=float(tot.sum() / (tot.max() * 8760)) if tot.max() > 0 else 0.0,
        feasible=feasible,
        soft_score=1.0,
        residential_profile_15min=pd.Series(rq, index=Q_IDX),
        commercial_profile_15min=pd.Series(cq, index=Q_IDX),
        category_profiles={c: pd.Series(a.reshape(-1, 4).sum(axis=1), index=H_IDX) for c, (_, a) in cats.items()},
    )


def _ens(runs: list[NeighborhoodResult]) -> EnsembleResult:
    feasible = [r for r in runs if r.feasible]
    return EnsembleResult(runs=feasible, converged=True, n_attempted=len(runs), n_rejected=len(runs) - len(feasible))


def _const_runs(values=(1.0, 2.0, 3.0, 4.0, 5.0)):
    """Run j has a constant 15-minute load of values[j] kWh per quarter (i.e. 4*values[j] kW)."""
    return [_nr(j, np.full(35040, v), None) for j, v in enumerate(values)]


# ---- known answers ---------------------------------------------------------------------------------------------


def test_known_answer_envelopes_in_kw_at_both_resolutions():
    res = build_results(_ens(_const_runs())).to_dict()
    assert res["schema_version"] == SCHEMA_VERSION
    q = np.array([4.0, 8.0, 12.0, 16.0, 20.0])  # kW per run
    exp = {
        "mean_kw": q.mean(),
        "p10_kw": np.percentile(q, 10),
        "p50_kw": np.percentile(q, 50),
        "p90_kw": np.percentile(q, 90),
    }
    for resolution, n in (("hourly", 8760), ("15min", 35040)):
        env = res["envelopes"][resolution]["total"]
        for key, value in exp.items():
            assert len(env[key]) == n
            assert env[key][0] == pytest.approx(value) and env[key][-1] == pytest.approx(value)
    assert exp["p10_kw"] < exp["p50_kw"] < exp["p90_kw"]  # ordering is not accidentally swapped
    assert res["time"]["hourly"] == {"start": "2018-01-01T00:00:00", "step_minutes": 60, "count": 8760}
    assert res["time"]["15min"]["step_minutes"] == 15 and res["time"]["15min"]["count"] == 35040


def test_mean_and_p50_differ_for_skewed_data():
    res = build_results(_ens(_const_runs((1.0, 1.0, 1.0, 1.0, 11.0)))).to_dict()
    env = res["envelopes"]["hourly"]["total"]
    assert env["mean_kw"][0] == pytest.approx(4 * 3.0)
    assert env["p50_kw"][0] == pytest.approx(4 * 1.0)


def test_units_are_labeled():
    res = build_results(_ens(_const_runs())).to_dict()
    assert res["units"] == {"power": "kW", "energy": "kWh"}
    assert set(res["envelopes"]["hourly"]["total"]) == {"mean_kw", "p10_kw", "p50_kw", "p90_kw"}


def test_scalar_statistics_known_answer():
    res = build_results(_ens(_const_runs())).to_dict()
    peaks = np.array([4.0, 8.0, 12.0, 16.0, 20.0])
    s = res["scalars"]["peak_kw"]
    assert s["mean"] == pytest.approx(peaks.mean())
    assert s["p10"] == pytest.approx(np.percentile(peaks, 10)) and s["p90"] == pytest.approx(np.percentile(peaks, 90))
    assert s["min"] == 4.0 and s["max"] == 20.0 and s["n"] == 5
    assert s["std"] == pytest.approx(peaks.std(ddof=1))
    e = res["scalars"]["annual_energy_kwh"]
    assert e["mean"] == pytest.approx(np.mean([v * 35040 for v in (1, 2, 3, 4, 5)]))


# ---- energy conservation -----------------------------------------------------------------------------------------


def _random_runs(seed=0, k=4):
    rng = np.random.default_rng(seed)
    runs = []
    for j in range(k):
        r = rng.gamma(2.0, 0.5, 35040)
        c = rng.gamma(2.0, 0.8, 35040)
        runs.append(_nr(j, r, c, {"mf": ("resstock", r), "office": ("comstock", c)}))
    return runs


def test_fifteen_minute_energy_equals_hourly_energy_in_the_envelopes():
    res = build_results(_ens(_random_runs())).to_dict()
    hourly = np.array(res["envelopes"]["hourly"]["total"]["mean_kw"])
    quarter = np.array(res["envelopes"]["15min"]["total"]["mean_kw"])
    assert hourly.sum() * 1.0 == pytest.approx(quarter.sum() * 0.25, rel=1e-12)  # kWh
    # each hour equals the average of its four 15-minute kW values
    assert np.allclose(hourly, quarter.reshape(-1, 4).mean(axis=1))
    assert res["scalars"]["annual_energy_kwh"]["mean"] == pytest.approx(hourly.sum(), rel=1e-12)


def test_residential_plus_commercial_equals_total_at_both_resolutions():
    res = build_results(_ens(_random_runs())).to_dict()
    for resolution in ("hourly", "15min"):
        e = res["envelopes"][resolution]
        assert np.allclose(
            np.array(e["residential"]["mean_kw"]) + np.array(e["commercial"]["mean_kw"]), e["total"]["mean_kw"]
        )


def test_building_types_sum_to_total_and_carry_their_source():
    res = build_results(_ens(_random_runs())).to_dict()
    bt = res["by_building_type"]
    assert set(bt) == {"mf", "office"}
    assert bt["mf"]["source"] == "resstock" and bt["office"]["source"] == "comstock"
    summed = np.array(bt["mf"]["hourly"]["mean_kw"]) + np.array(bt["office"]["hourly"]["mean_kw"])
    assert np.allclose(summed, res["envelopes"]["hourly"]["total"]["mean_kw"])


def test_a_mismatch_between_hourly_and_fifteen_minute_energy_fails_loudly():
    runs = _random_runs()
    runs[1].residential_profile_15min = runs[1].residential_profile_15min * 1.01
    with pytest.raises(ValueError, match="energy"):
        build_results(_ens(runs))


# ---- load-duration curve and seasonal profiles ------------------------------------------------------------------


def test_load_duration_curve_is_sorted_descending_and_starts_at_the_peak():
    res = build_results(_ens(_random_runs())).to_dict()
    ldc = res["load_duration"]["hourly"]["total"]
    for key in ("mean_kw", "p10_kw", "p50_kw", "p90_kw"):
        arr = np.array(ldc[key])
        assert len(arr) == 8760 and np.all(np.diff(arr) <= 1e-12)
    assert ldc["mean_kw"][0] == pytest.approx(res["scalars"]["peak_kw"]["mean"])
    assert ldc["p10_kw"][0] <= ldc["p50_kw"][0] <= ldc["p90_kw"][0]


def test_load_duration_known_answer():
    # one run, load = hour index mod 10 -> LDC is the sorted values, descending
    q = np.repeat((np.arange(8760) % 10).astype(float), 4) / 4.0  # kWh per quarter so hourly kW = hour%10
    res = build_results(_ens([_nr(0, q, None)])).to_dict()
    expected = np.sort((np.arange(8760) % 10).astype(float))[::-1]
    assert np.allclose(res["load_duration"]["hourly"]["total"]["p50_kw"], expected)


def test_seasonal_daily_profile_known_answer():
    hours = np.repeat(np.tile(np.arange(24), 365).astype(float), 4)  # kW = hour of day, every day
    q = hours / 4.0
    res = build_results(_ens([_nr(0, q, None)])).to_dict()
    sd = res["seasonal_daily"]
    assert sd["hours"] == list(range(24))
    assert sd["seasons"] == {
        "winter": ["Dec", "Jan", "Feb"],
        "spring": ["Mar", "Apr", "May"],
        "summer": ["Jun", "Jul", "Aug"],
        "fall": ["Sep", "Oct", "Nov"],
    }
    for season in sd["seasons"]:
        assert np.allclose(sd["profiles"]["total"][season]["mean_kw"], np.arange(24))


def test_seasonal_profiles_separate_seasons_and_load_types():
    month = H_IDX.month.to_numpy()
    summer_only = np.where(np.isin(month, [6, 7, 8]), 2.0, 0.0)  # kW
    res_q = np.repeat(summer_only, 4) / 4.0
    com_q = np.repeat(np.full(8760, 1.0), 4) / 4.0
    sd = build_results(_ens([_nr(0, res_q, com_q)])).to_dict()["seasonal_daily"]["profiles"]
    assert np.allclose(sd["residential"]["summer"]["mean_kw"], 2.0)
    assert np.allclose(sd["residential"]["winter"]["mean_kw"], 0.0)
    assert np.allclose(sd["commercial"]["winter"]["mean_kw"], 1.0)
    assert np.allclose(sd["total"]["summer"]["mean_kw"], 3.0)


# ---- schema, determinism, hostile input -------------------------------------------------------------------------


def test_result_is_strictly_json_serializable_with_stable_top_level_keys():
    d = build_results(_ens(_random_runs())).to_dict()
    json.dumps(d, allow_nan=False)
    assert set(d) == {
        "schema_version",
        "units",
        "n_runs",
        "percentiles",
        "time",
        "envelopes",
        "by_building_type",
        "load_duration",
        "seasonal_daily",
        "scalars",
        "energy_check",
    }
    assert d["percentiles"] == [10, 50, 90]


def test_building_result_is_deterministic():
    a = build_results(_ens(_random_runs(3))).to_dict()
    b = build_results(_ens(_random_runs(3))).to_dict()
    assert a == b


def test_no_feasible_runs_yields_an_empty_but_valid_result():
    d = build_results(_ens([_nr(0, np.ones(35040), None, feasible=False)])).to_dict()
    json.dumps(d, allow_nan=False)
    assert d["n_runs"] == 0 and d["envelopes"] is None and d["scalars"] is None


def test_runs_without_fifteen_minute_data_omit_that_resolution():
    runs = _random_runs()
    for r in runs:
        r.residential_profile_15min = None
        r.commercial_profile_15min = None
    d = build_results(_ens(runs)).to_dict()
    assert d["time"]["15min"] is None and d["envelopes"]["15min"] is None
    assert d["envelopes"]["hourly"] is not None


def test_nan_in_a_profile_fails_loudly():
    runs = _random_runs()
    runs[2].total_profile = runs[2].total_profile.copy()
    runs[2].total_profile.iloc[10] = np.nan
    with pytest.raises(ValueError, match="NaN|finite"):
        build_results(_ens(runs))


def test_runs_with_different_time_axes_fail_loudly():
    runs = _random_runs()
    runs[1].total_profile = runs[1].total_profile.iloc[:100]
    with pytest.raises(ValueError, match="time axis|length"):
        build_results(_ens(runs))


# ---- through the real engine (synthetic dataset) -------------------------------------------------------------


@pytest.fixture(scope="module")
def tiny_result(synthetic_dataset):
    cfg = build_tiny_config(synthetic_dataset)
    cfg["ensemble"] = {"mode": "fixed", "max_runs": 4}
    cfg["archetype_filters"]["comstock"] = {"office": {"building_types": ["SmallOffice"]}}
    cfg["composition"]["categories"]["office"] = {"fixed_count": 1, "source": "comstock"}
    return run.run_scenario(cfg)


def test_engine_run_has_fifteen_minute_envelopes_and_conserves_energy(tiny_result):
    d = tiny_result.to_dict()
    assert d["schema_version"] == SCHEMA_VERSION
    r = d["results"]
    hourly = np.array(r["envelopes"]["hourly"]["total"]["mean_kw"])
    quarter = np.array(r["envelopes"]["15min"]["total"]["mean_kw"])
    assert hourly.sum() == pytest.approx(quarter.sum() * 0.25, rel=1e-6)
    assert hourly.sum() == pytest.approx(np.mean([m["annual_energy_kwh"] for m in d["metrics"]]), rel=1e-6)
    assert r["energy_check"]["max_relative_difference"] < 1e-5
    json.dumps(d, allow_nan=False)


def test_engine_per_run_hourly_is_the_sum_of_its_four_quarters(tiny_result):
    for run_ in tiny_result.ensemble.runs:
        q = (run_.residential_profile_15min.add(run_.commercial_profile_15min, fill_value=0.0)).to_numpy()
        h = run_.total_profile.to_numpy()
        assert np.allclose(q.reshape(-1, 4).sum(axis=1), h, rtol=1e-5, atol=1e-6)
        assert q.sum() == pytest.approx(run_.annual_energy_kwh, rel=1e-5)


def test_engine_has_both_residential_and_commercial_building_types(tiny_result):
    bt = tiny_result.to_dict()["results"]["by_building_type"]
    assert {v["source"] for v in bt.values()} == {"resstock", "comstock"}
    assert "office" in bt and bt["office"]["source"] == "comstock"
    assert set(bt) <= {"mf_small", "mf_mid", "office"}
    total = np.array(tiny_result.to_dict()["results"]["envelopes"]["hourly"]["total"]["mean_kw"])
    assert np.allclose(sum(np.array(v["hourly"]["mean_kw"]) for v in bt.values()), total, rtol=1e-6)


def test_loader_fifteen_minute_series_matches_source_energy(synthetic_dataset):
    import pandas as pd

    cfg = run.config_from_dict(build_tiny_config(synthetic_dataset))
    dirs = {"resstock": synthetic_dataset.resstock_dir, "comstock": synthetic_dataset.comstock_dir}
    bid = int(pd.read_excel(synthetic_dataset.resstock_chars, sheet_name="building_characteristics")["bldg_id"].iloc[0])
    sel = BuildingSelection(bldg_id=bid, category="x", source="resstock", unit_multiplier=1.0)
    prof = loader.load_and_scale_profile(sel, dirs, cfg)
    raw = loader.load_building_profile(bid, "resstock", dirs, cfg.data.upgrade, cfg.data.state)
    src = float(raw[loader.RESSTOCK_KWH_COL].astype("float64").sum())
    assert prof.kwh_15min is not None and len(prof.kwh_15min) == 35040
    assert float(prof.kwh_15min.sum()) == pytest.approx(src, rel=1e-6)
    assert float(prof.hourly_kwh.sum()) == pytest.approx(src, rel=1e-6)
    assert np.allclose(
        prof.kwh_15min.to_numpy().reshape(-1, 4).sum(axis=1), prof.hourly_kwh.to_numpy(), rtol=1e-5, atol=1e-6
    )


def test_temporal_shift_keeps_hourly_and_fifteen_minute_series_consistent():
    from microgrid.community_load.synthetic._types import PerturbedProfile
    from microgrid.community_load.synthetic.config import PerturbationConfig
    from microgrid.community_load.synthetic.perturbation import perturb_building

    q = pd.Series(np.random.default_rng(1).gamma(2.0, 1.0, 35040), index=Q_IDX)
    h = pd.Series(q.to_numpy().reshape(-1, 4).sum(axis=1), index=H_IDX)
    base = PerturbedProfile(1, "x", "resstock", h, float(h.sum()), float(h.sum()), kwh_15min=q)
    shifted_any = False
    for seed in range(6):
        out = perturb_building(
            base, PerturbationConfig(enabled=True, max_temporal_shift_hours=3), np.random.default_rng(seed)
        )
        assert out.kwh_15min is not None
        assert np.allclose(out.kwh_15min.to_numpy().reshape(-1, 4).sum(axis=1), out.hourly_kwh.to_numpy())
        assert out.kwh_15min.sum() == pytest.approx(q.sum())
        shifted_any |= "shift_hours" in out.perturbation_log
    assert shifted_any
