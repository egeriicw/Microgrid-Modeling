"""FR-ENG-6: the approved provisional convergence rule (relative change of mean annual energy, mean peak and
mean hourly profile below epsilon for M consecutive checks, plus a standard-error target on the P50 profile)."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from microgrid.community_load.synthetic import config as cfg_mod
from microgrid.community_load.synthetic import ensemble, run
from microgrid.community_load.synthetic._types import ArchetypePool, NeighborhoodComposition, NeighborhoodResult
from microgrid.community_load.synthetic.config import ConvergenceConfig
from microgrid.community_load.synthetic.convergence import ConvergenceTracker

IDX = pd.date_range("2018-01-01", periods=24, freq="h")


def _run(i: int, energy: float = 100.0, peak: float = 10.0, profile: np.ndarray | None = None) -> NeighborhoodResult:
    prof = pd.Series(np.ones(24) if profile is None else profile, index=IDX)
    return NeighborhoodResult(
        run_index=i,
        residential_profile=prof,
        commercial_profile=pd.Series(dtype=float),
        total_profile=prof,
        composition=NeighborhoodComposition(counts={}, proportions={}),
        selections=[],
        peak_kw=peak,
        annual_energy_kwh=energy,
        load_factor=0.5,
        feasible=True,
        soft_score=1.0,
    )


def _cc(**kw) -> ConvergenceConfig:
    base = {"rule": "prd_default", "epsilon": 0.01, "consecutive_checks": 3, "se_target": 0.05}
    return ConvergenceConfig(**{**base, **kw})


def _drive(tracker, runs, every):
    """Feed runs and check every ``every`` runs; return the list of checks."""
    return [tracker.check(runs[:k], k) for k in range(every, len(runs) + 1, every)]


def test_constant_ensemble_converges_after_exactly_m_passing_checks():
    runs = [_run(i) for i in range(40)]
    checks = _drive(ConvergenceTracker(_cc()), runs, 2)
    flags = [c.converged for c in checks]
    # check 1 has no previous check to compare with, so it cannot pass; passes at checks 2,3,4 -> converged at the 4th
    assert flags.index(True) == 3
    assert [c.consecutive_passes for c in checks[:4]] == [0, 1, 2, 3]
    assert checks[3].n_feasible == 8


def test_first_check_never_passes_even_for_constant_data():
    c = ConvergenceTracker(_cc(consecutive_checks=1)).check([_run(0), _run(1)], 2)
    assert c.passed is False and c.converged is False


def _two_checks(first, second, **cfg):
    t = ConvergenceTracker(_cc(consecutive_checks=1, **cfg))
    t.check(first, len(first))
    return t.check(second, len(second))


def test_annual_energy_change_is_strict_less_than_epsilon():
    first = [_run(0, energy=100.0), _run(1, energy=100.0)]
    second = first + [_run(2, energy=200.0), _run(3, energy=200.0)]  # mean 100 -> 150: change exactly 0.5
    assert _two_checks(first, second, epsilon=0.5).passed is False  # equal is NOT below epsilon
    ok = _two_checks(first, second, epsilon=0.5000001)
    assert ok.passed is True
    assert ok.rel_change["annual_energy_kwh"] == pytest.approx(0.5)


def test_peak_change_alone_blocks_convergence():
    first = [_run(0, peak=10.0), _run(1, peak=10.0)]
    second = first + [_run(2, peak=30.0), _run(3, peak=30.0)]  # mean 10 -> 20
    c = _two_checks(first, second, epsilon=0.5)
    assert c.rel_change["peak_kw"] == pytest.approx(1.0)
    assert c.rel_change["annual_energy_kwh"] == 0.0
    assert c.passed is False


def test_mean_profile_change_alone_blocks_convergence():
    first = [_run(0), _run(1)]
    bumped = np.ones(24)
    bumped[5:9] = 5.0
    second = first + [_run(2, profile=bumped), _run(3, profile=bumped)]
    c = _two_checks(first, second, epsilon=0.1, se_target=100.0)
    assert c.rel_change["annual_energy_kwh"] == 0.0 and c.rel_change["peak_kw"] == 0.0
    assert c.rel_change["mean_profile"] > 0.1
    assert c.passed is False


def test_se_target_alone_blocks_convergence():
    rng = np.random.default_rng(0)
    runs = [_run(i, profile=100 + rng.normal(0, 30, 24)) for i in range(8)]
    c = _two_checks(runs[:4], runs, epsilon=1e9, se_target=0.001)
    assert c.rel_se_p50 is not None and c.rel_se_p50 > 0.001
    assert c.passed is False
    assert _two_checks(runs[:4], runs, epsilon=1e9, se_target=10.0).passed is True


def test_a_failing_check_resets_the_consecutive_counter():
    t = ConvergenceTracker(_cc(consecutive_checks=3, epsilon=0.1))
    runs = [_run(0), _run(1)]
    t.check(runs, 2)
    runs += [_run(2), _run(3)]
    assert t.check(runs, 4).consecutive_passes == 1
    runs += [_run(4, energy=1000.0), _run(5, energy=1000.0)]  # big jump
    c = t.check(runs, 6)
    assert c.passed is False and c.consecutive_passes == 0
    runs += [_run(6, energy=1000.0), _run(7, energy=1000.0)]
    assert t.check(runs, 8).consecutive_passes <= 1  # restarts from zero, never carries over


def test_known_variance_ensemble_converges_near_the_expected_run_count():
    # Hourly load = 100 + N(0, sigma=10). SE of the median ~ 1.2533*sigma/sqrt(K), relative to ~100:
    # target 0.01 => K ~ (1.2533*0.1/0.01)^2 ~ 157.
    rng = np.random.default_rng(42)
    runs = [_run(i, profile=100 + rng.normal(0, 10, 24)) for i in range(600)]
    t = ConvergenceTracker(_cc(epsilon=0.5, consecutive_checks=1, se_target=0.01))
    checks = _drive(t, runs, 10)
    k = next(c.n_feasible for c in checks if c.converged)
    assert 130 <= k <= 200, k
    assert all(not c.converged for c in checks if c.n_feasible < 130)


def test_incomparable_profile_lengths_fail_loudly():
    short = _run(9)
    short.total_profile = pd.Series(np.ones(5), index=IDX[:5])
    with pytest.raises(ValueError, match="same length"):
        ConvergenceTracker(_cc()).check([_run(0), _run(1), short], 3)


def test_check_dict_is_plain_data():
    c = _two_checks([_run(0), _run(1)], [_run(i) for i in range(4)])
    json.dumps(c.to_dict())
    assert {"n_attempted", "n_feasible", "rule", "rel_change", "rel_se_p50", "passed", "converged"} <= set(c.to_dict())


# ---- engine-level: stopping point, determinism, bounds -------------------------------------------------------


@pytest.fixture
def stub_pool() -> ArchetypePool:
    return ArchetypePool(residential={}, commercial={}, resstock_chars=pd.DataFrame(), comstock_chars=pd.DataFrame())


def _fake_single(noise: float = 0.0):
    def fake(run_index, pool, cfg, run_rng, data_dirs):
        base = 100.0 + (float(run_rng.composition.normal(0, noise)) if noise else 0.0)
        s = pd.Series(np.full(24, base), index=IDX)
        return NeighborhoodResult(
            run_index=run_index,
            residential_profile=s,
            commercial_profile=pd.Series(dtype=float),
            total_profile=s,
            composition=NeighborhoodComposition(counts={}, proportions={}),
            selections=[],
            peak_kw=base,
            annual_energy_kwh=base * 24,
            load_factor=1.0,
            feasible=True,
            soft_score=1.0,
        )

    return fake


def _raw(max_runs=200, mode="adaptive", **conv):
    return {
        "scenario": {"name": "stub"},
        "rng": {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3},
        "ensemble": {"mode": mode, "max_runs": max_runs, "min_runs": 4, "check_every": 2},
        "convergence": {"rule": "prd_default", "epsilon": 0.05, "consecutive_checks": 3, "se_target": 0.2, **conv},
    }


def test_engine_stops_early_when_rule_met_and_reports_it(stub_pool, monkeypatch):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(noise=1.0))
    res = run.run_scenario(_raw(), pool=stub_pool, data_dirs={})
    assert res.converged is True
    assert res.ensemble.n_attempted < 200
    last = res.ensemble.convergence_checks[-1]
    assert last.converged and last.n_attempted == res.ensemble.n_attempted
    assert res.to_dict()["convergence"]["checks"][-1]["converged"] is True


def test_engine_respects_max_runs_when_never_converging(stub_pool, monkeypatch):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(noise=30.0))
    res = run.run_scenario(_raw(max_runs=12, epsilon=1e-9), pool=stub_pool, data_dirs={})
    assert res.converged is False and res.ensemble.n_attempted == 12


def test_same_seeds_replay_identical_stopping_point_and_checks(stub_pool, monkeypatch):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(noise=1.0))
    a = run.run_scenario(_raw(), pool=stub_pool, data_dirs={})
    b = run.run_scenario(_raw(), pool=stub_pool, data_dirs={})
    assert a.ensemble.n_attempted == b.ensemble.n_attempted
    assert [c.to_dict() for c in a.ensemble.convergence_checks] == [c.to_dict() for c in b.ensemble.convergence_checks]
    assert a.metrics == b.metrics


def test_adaptive_runs_are_a_prefix_of_a_longer_fixed_run(stub_pool, monkeypatch):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(noise=1.0))
    a = run.run_scenario(_raw(), pool=stub_pool, data_dirs={})
    n = a.ensemble.n_attempted
    fixed_raw = _raw(max_runs=n + 5, mode="fixed")
    fixed_raw["convergence"] = {}
    fixed = run.run_scenario(fixed_raw, pool=stub_pool, data_dirs={})
    assert fixed.metrics[:n] == a.metrics


def test_progress_events_carry_convergence_status(stub_pool, monkeypatch):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(noise=1.0))
    events: list[run.ProgressEvent] = []
    run.run_scenario(_raw(), pool=stub_pool, data_dirs={}, progress=events.append)
    checks = [e for e in events if e.stage == "checking_convergence"]
    assert checks and all(e.convergence is not None and e.convergence["rule"] == "prd_default" for e in checks)
    assert checks[-1].convergence["converged"] is True
    running = [e for e in events if e.stage == "running"]
    assert running[0].convergence is None  # nothing checked yet
    assert running[-1].convergence is not None  # the latest status rides on later run events


def test_legacy_rule_is_still_the_default_and_unchanged(stub_pool, monkeypatch):
    assert cfg_mod.ConvergenceConfig().rule == "legacy_window"
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(noise=1.0))
    raw = {
        "rng": {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3},
        "ensemble": {"mode": "adaptive", "max_runs": 30, "min_runs": 2, "check_every": 2},
        "convergence": {"lookback_window": 1, "min_stable_window": 1, "epsilon": 0.5},
    }
    res = run.run_scenario(raw, pool=stub_pool, data_dirs={})
    assert res.converged and res.ensemble.convergence_checks[-1].rule == "legacy_window"


def test_no_check_happens_before_min_runs_and_checks_fall_on_the_cadence(stub_pool, monkeypatch):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(noise=1.0))
    res = run.run_scenario(_raw(), pool=stub_pool, data_dirs={})  # min_runs=4, check_every=2
    counts = [c.n_feasible for c in res.ensemble.convergence_checks]
    assert counts[0] == 4
    assert all(b - a == 2 for a, b in zip(counts, counts[1:], strict=False))
