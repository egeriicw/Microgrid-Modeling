"""Independent QA of the Sprint 2 engine work (FR-ENG-5/6/7): adaptive convergence, results model, schema.

Tests marked ``xfail(strict=True)`` document real defects found in review; when the defect is fixed the
strict xfail turns into a failure that tells the fixer to remove the marker.
"""

from __future__ import annotations

import dataclasses
import json

import numpy as np
import pandas as pd
import pytest

from microgrid.community_load.synthetic import config as cfg_mod
from microgrid.community_load.synthetic import ensemble, run
from microgrid.community_load.synthetic.convergence import ConvergenceTracker
from microgrid.community_load.synthetic.results import build_results
from tests.test_convergence_prd import IDX, _cc, _fake_single, _raw, _run, stub_pool  # noqa: F401
from tests.test_results_model import _ens, _nr
from tests.test_run_facade import build_tiny_config

# ---- convergence ----------------------------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT: the check fires after every attempt while n_feasible is a multiple of check_every, so an "
    "infeasible (rejected) run re-checks the identical ensemble; rel_change is 0 and the pass counter climbs "
    "without any new information, giving false convergence",
)
def test_rejected_runs_do_not_trigger_extra_convergence_checks(stub_pool, monkeypatch):  # noqa: F811
    base = _fake_single(noise=0.0)

    def fake(run_index, pool, cfg, run_rng, data_dirs):
        r = base(run_index, pool, cfg, run_rng, data_dirs)
        return dataclasses.replace(r, feasible=run_index < 4)  # 4 feasible runs, then everything is rejected

    monkeypatch.setattr(ensemble, "_run_single", fake)
    res = run.run_scenario(_raw(max_runs=30), pool=stub_pool, data_dirs={})
    # Only n_feasible == 4 (== min_runs) is a legitimate check point; the first check can never pass.
    assert len(res.ensemble.convergence_checks) == 1
    assert res.converged is False


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT: ConvergenceCheck.to_dict() can carry NaN/Infinity for non-finite input, which is not strict "
    "JSON (the results model rejects non-finite input loudly; the tracker should too)",
)
def test_check_dict_is_strict_json_for_non_finite_profiles():
    runs = [_run(0, profile=np.zeros(24)), _run(1, profile=np.full(24, np.inf))]
    chk = ConvergenceTracker(_cc()).check(runs, 2)
    json.dumps(chk.to_dict(), allow_nan=False)


def test_zero_variance_ensemble_is_not_a_divide_by_zero_and_converges():
    t = ConvergenceTracker(_cc())
    runs = [_run(i) for i in range(40)]
    checks = [t.check(runs[:k], k) for k in range(4, 41, 4)]
    assert all(c.rel_se_p50 == 0.0 for c in checks)
    assert [c.consecutive_passes for c in checks[:5]] == [0, 1, 2, 3, 4]
    assert [c.converged for c in checks[:5]] == [False, False, False, True, True]


def test_all_zero_profiles_do_not_divide_by_zero():
    t = ConvergenceTracker(_cc())
    runs = [_run(i, energy=0.0, peak=0.0, profile=np.zeros(24)) for i in range(10)]
    c = [t.check(runs[:k], k) for k in (5, 10)][-1]
    assert c.passed and c.rel_se_p50 == 0.0


def test_single_run_check_has_no_se_and_cannot_pass():
    c = ConvergenceTracker(_cc()).check([_run(0)], 1)
    assert c.rel_se_p50 is None and not c.passed


def test_se_uses_sample_std_and_median_factor_exactly():
    # two known runs: values 1 and 3 everywhere -> s = sqrt(2); P50 = 2; SE = 1.2533*sqrt(2)/sqrt(2)
    runs = [_run(0, profile=np.full(24, 1.0)), _run(1, profile=np.full(24, 3.0))]
    c = ConvergenceTracker(_cc()).check(runs, 2)
    assert c.rel_se_p50 == pytest.approx(np.sqrt(np.pi / 2) / 2.0, rel=1e-12)


def test_equality_at_threshold_does_not_pass():
    # relative change exactly epsilon must not pass (strict inequality in the PRD wording "below")
    t = ConvergenceTracker(_cc(epsilon=0.5, se_target=10.0))
    t.check([_run(0, energy=100.0), _run(1, energy=100.0)], 2)
    c = t.check([_run(0, energy=100.0), _run(1, energy=100.0), _run(2, energy=100.0 + 0.0)], 3)
    assert c.passed  # sanity: zero change passes
    t2 = ConvergenceTracker(_cc(epsilon=0.5, se_target=10.0))
    t2.check([_run(0, energy=100.0)], 1)
    c2 = t2.check([_run(0, energy=100.0), _run(1, energy=200.0)], 2)  # mean 100 -> 150: change exactly 0.5
    assert c2.rel_change["annual_energy_kwh"] == pytest.approx(0.5) and not c2.passed


def test_adaptive_prefix_property_with_prd_rule(stub_pool, monkeypatch):  # noqa: F811
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(noise=1.0))
    ad = run.run_scenario(_raw(), pool=stub_pool, data_dirs={})
    fx = run.run_scenario(_raw(max_runs=ad.ensemble.n_attempted, mode="fixed"), pool=stub_pool, data_dirs={})
    a = [r.annual_energy_kwh for r in ad.ensemble.runs]
    f = [r.annual_energy_kwh for r in fx.ensemble.runs]
    assert a == f
    again = run.run_scenario(_raw(), pool=stub_pool, data_dirs={})
    assert [c.to_dict() for c in again.ensemble.convergence_checks] == [
        c.to_dict() for c in ad.ensemble.convergence_checks
    ]


def test_adaptive_min_runs_above_max_runs_is_rejected_loudly():
    with pytest.raises(ValueError, match="min_runs"):
        cfg_mod.config_from_dict({"ensemble": {"mode": "adaptive", "min_runs": 50, "max_runs": 10}})


# ---- results model --------------------------------------------------------------------------------------------


def test_hourly_equals_sum_of_four_quarters_per_interval_in_a_real_run(synthetic_dataset):
    c = build_tiny_config(synthetic_dataset)
    c["perturbation"] = {"enabled": True, "max_temporal_shift_hours": 3}
    c["ensemble"] = {"mode": "fixed", "max_runs": 8}
    res = run.run_scenario(c)
    for r in res.ensemble.runs:
        for h, q in (
            (r.residential_profile, r.residential_profile_15min),
            (r.commercial_profile, r.commercial_profile_15min),
        ):
            if q is None or len(q) == 0:
                continue
            np.testing.assert_allclose(q.to_numpy().reshape(-1, 4).sum(axis=1), h.to_numpy(), rtol=1e-6, atol=1e-6)


def test_real_run_document_is_strict_json_versioned_and_ordered(synthetic_dataset):
    c = build_tiny_config(synthetic_dataset)
    c["ensemble"] = {"mode": "fixed", "max_runs": 8}
    d = run.run_scenario(c).to_dict()
    text = json.dumps(d, allow_nan=False)
    assert d["schema_version"] == d["results"]["schema_version"]
    for res in ("hourly", "15min"):
        e = d["results"]["envelopes"][res]["total"]
        p10, p50, p90, mean = (np.array(e[k]) for k in ("p10_kw", "p50_kw", "p90_kw", "mean_kw"))
        assert (p10 <= p50 + 1e-9).all() and (p50 <= p90 + 1e-9).all()
        assert (mean >= p10.min() - 1e-9).all() and (mean <= p90.max() + 1e-9).all()
    for load_type in d["results"]["load_duration"]["hourly"].values():
        for k in ("mean_kw", "p10_kw", "p50_kw", "p90_kw"):
            assert (np.diff(np.array(load_type[k])) <= 1e-9).all()
    # size tripwire: full 15-minute document for the synthetic fixture (measured about 10.5 MB at review)
    assert len(text) < 16_000_000


def test_compact_summary_stays_small(synthetic_dataset):
    c = build_tiny_config(synthetic_dataset)
    c["ensemble"] = {"mode": "fixed", "max_runs": 6}
    assert len(json.dumps(run.run_scenario(c).to_dict(include_results=False))) < 20_000


def test_zero_runs_and_single_run_and_identical_runs_serialize_strictly():
    json.dumps(build_results(_ens([])).to_dict(), allow_nan=False)
    one = build_results(_ens([_nr(0, np.full(35040, 1.0), None)])).to_dict()
    assert one["scalars"]["peak_kw"]["std"] == 0.0
    same = build_results(_ens([_nr(i, np.full(35040, 1.0), None) for i in range(4)])).to_dict()
    json.dumps(same, allow_nan=False)
    e = same["envelopes"]["hourly"]["total"]
    assert e["p10_kw"] == e["p50_kw"] == e["p90_kw"] == e["mean_kw"]


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_non_finite_profile_fails_loudly(bad):
    a = np.ones(35040)
    a[7] = bad
    with pytest.raises(ValueError, match="non-finite|NaN"):
        build_results(_ens([_nr(0, a, None), _nr(1, np.ones(35040), None)]))


def test_energy_mismatch_between_resolutions_fails_loudly():
    good = _nr(0, np.ones(35040), None)
    bad = dataclasses.replace(good, residential_profile_15min=good.residential_profile_15min * 1.01)
    with pytest.raises(ValueError, match="energy not conserved"):
        build_results(_ens([bad]))


def _seasonal_run(index_h: pd.DatetimeIndex, value_by_month: bool = True):
    n = len(index_h)
    h = pd.Series(index_h.month.to_numpy(dtype=float), index=index_h)
    z = pd.Series(np.zeros(n), index=index_h)
    base = _nr(0, np.zeros(35040), None)
    return dataclasses.replace(
        base,
        residential_profile=h,
        commercial_profile=z,
        total_profile=h,
        residential_profile_15min=None,
        commercial_profile_15min=None,
        category_profiles={},
        peak_kw=float(h.max()),
        annual_energy_kwh=float(h.sum()),
    )


@pytest.mark.parametrize(
    ("start", "periods"),
    [("2018-01-01", 8760), ("2020-01-01", 8784)],  # normal year and a leap year
)
def test_seasonal_grouping_at_month_boundaries_and_leap_year(start, periods):
    idx = pd.date_range(start, periods=periods, freq="h")
    r = _seasonal_run(idx)
    doc = build_results(_ens([r, dataclasses.replace(r, run_index=1)])).to_dict()
    prof = doc["seasonal_daily"]["profiles"]["total"]
    want = {"winter": (12 + 1 + 2) / 3, "spring": 4.0, "summer": 7.0, "fall": 10.0}
    for season, ms in (("winter", (12, 1, 2)), ("spring", (3, 4, 5)), ("summer", (6, 7, 8)), ("fall", (9, 10, 11))):
        # hour-weighted mean of the month numbers in that season, per hour of day
        mask = idx.month.isin(ms)
        exp = [float(np.mean(idx.month[mask & (idx.hour == h)])) for h in range(24)]
        np.testing.assert_allclose(prof[season]["mean_kw"], exp, rtol=1e-12)
    assert want["spring"] == pytest.approx(np.mean(prof["spring"]["mean_kw"]), rel=0.02)


def test_dst_like_timezone_index_does_not_crash_and_keeps_24_hour_buckets():
    idx = pd.date_range("2018-01-01", periods=8760, freq="h", tz="UTC").tz_convert("America/New_York")
    # a zone-aware index has uneven local spacing around DST only if wall-clock; absolute spacing is even
    r = _seasonal_run(idx)
    doc = build_results(_ens([r])).to_dict()
    assert len(doc["seasonal_daily"]["profiles"]["total"]["summer"]["mean_kw"]) == 24
    json.dumps(doc, allow_nan=False)


# ---- tests added to kill mutants that survived the author's suite ----------------------------------------------


def test_se_equal_to_target_does_not_pass():
    runs = [_run(0, profile=np.full(24, 1.0)), _run(1, profile=np.full(24, 3.0)), _run(2, profile=np.full(24, 2.0))]
    probe = ConvergenceTracker(_cc()).check(runs, 3).rel_se_p50
    assert probe is not None
    t = ConvergenceTracker(_cc(epsilon=10.0, se_target=probe))
    t.check(runs[:2], 2)
    assert not t.check(runs, 3).passed  # exactly at the target is not "below" it


@pytest.mark.parametrize(("e1", "e2"), [(200.0, 100.0), (100.0, 200.0)])
def test_large_change_in_either_direction_blocks_convergence(e1, e2):
    t = ConvergenceTracker(_cc(epsilon=0.01, se_target=10.0))
    t.check([_run(0, energy=e1, peak=e1)], 1)
    c = t.check([_run(0, energy=e1, peak=e1), _run(1, energy=e2 * 2 - e1, peak=e2 * 2 - e1)], 2)
    assert c.rel_change["annual_energy_kwh"] > 0.01 and not c.passed


def test_se_is_relative_to_the_median_profile_not_the_mean():
    runs = [_run(0, profile=np.full(24, 1.0)), _run(1, profile=np.full(24, 1.0)), _run(2, profile=np.full(24, 10.0))]
    s = np.std([1.0, 1.0, 10.0], ddof=1)
    expect = np.sqrt(np.pi / 2) * s / np.sqrt(3) * np.sqrt(24) / (1.0 * np.sqrt(24))  # median is 1, mean is 4
    assert ConvergenceTracker(_cc()).check(runs, 3).rel_se_p50 == pytest.approx(expect, rel=1e-12)
