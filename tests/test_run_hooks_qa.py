"""QA review of story 1.9: run_ensemble hooks only observe; cancellation and callback-failure semantics."""

from __future__ import annotations

import copy
import pickle

import pandas as pd
import pytest

from microgrid.community_load.synthetic import config as cfg_mod
from microgrid.community_load.synthetic import ensemble, run
from microgrid.community_load.synthetic._types import (
    ArchetypePool,
    NeighborhoodComposition,
    NeighborhoodResult,
)


@pytest.fixture
def stub_pool() -> ArchetypePool:
    return ArchetypePool(residential={}, commercial={}, resstock_chars=pd.DataFrame(), comstock_chars=pd.DataFrame())


def _raw(max_runs: int, mode: str, epsilon: float = 0.5) -> dict:
    return {
        "scenario": {"name": "qa"},
        "rng": {"composition_seed": 11, "archetype_seed": 22, "perturbation_seed": 33},
        "ensemble": {"mode": mode, "max_runs": max_runs, "min_runs": 2, "check_every": 2},
        "convergence": {"lookback_window": 1, "min_stable_window": 1, "epsilon": epsilon},
    }


def _fake_single(infeasible: frozenset[int] = frozenset(), draws: list | None = None):
    def fake(run_index, pool, cfg, run_rng, data_dirs):
        # Consume from every stream so a hook that disturbed RNG state would change later numbers.
        a = float(run_rng.composition.random())
        b = float(run_rng.archetype.random())
        c = float(run_rng.perturbation.random())
        if draws is not None:
            draws.append((a, b, c))
        peak = 100.0 + a * 50 + b * 10 + c
        s = pd.Series([peak, peak / 2], index=pd.date_range("2018-01-01", periods=2, freq="h"))
        return NeighborhoodResult(
            run_index=run_index,
            residential_profile=s,
            commercial_profile=pd.Series(dtype=float),
            total_profile=s,
            composition=NeighborhoodComposition(counts={}, proportions={}),
            selections=[],
            peak_kw=peak,
            annual_energy_kwh=peak * 10,
            load_factor=0.4,
            feasible=run_index not in infeasible,
            soft_score=0.5,
        )

    return fake


def _digest(res) -> tuple:
    return (
        res.seeds,
        res.converged,
        res.n_attempted,
        res.n_rejected,
        [(r.run_index, r.peak_kw, r.annual_energy_kwh, r.load_factor, r.soft_score, r.feasible) for r in res.runs],
        {k: float(v) for k, v in res.diversity_metrics.items()},
        list(res.total_profile_p50.round(9)) if hasattr(res, "total_profile_p50") else None,
    )


@pytest.mark.parametrize(
    ("mode", "max_runs", "infeasible", "epsilon"),
    [("fixed", 5, frozenset(), 0.5), ("adaptive", 12, frozenset({1}), 0.5), ("adaptive", 8, frozenset(), 1e-12)],
    ids=["fixed", "adaptive_converges", "adaptive_never_converges"],
)
def test_hooks_do_not_change_numbers(monkeypatch, stub_pool, mode, max_runs, infeasible, epsilon):
    cfg = cfg_mod.config_from_dict(_raw(max_runs, mode, epsilon))
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(infeasible))
    plain = ensemble.run_ensemble(stub_pool, cfg, {})

    log: list[tuple] = []
    hooked = ensemble.run_ensemble(
        stub_pool,
        copy.deepcopy(cfg),
        {},
        before_run=lambda i, na, nf: log.append(("before", i, na, nf)),
        after_run=lambda r, na, nf: log.append(("after", r.run_index, na, nf)),
        on_check=lambda c, na, nf: log.append(("check", c, na, nf)),
    )
    assert _digest(plain) == _digest(hooked)
    assert plain.seeds == {"composition_seed": 11, "archetype_seed": 22, "perturbation_seed": 33}

    # ordering and counts: before(i) -> after(i) -> [check]; counters are the pre/post-run values
    befores = [e for e in log if e[0] == "before"]
    afters = [e for e in log if e[0] == "after"]
    assert len(befores) == len(afters) == hooked.n_attempted
    assert [b[1] for b in befores] == list(range(hooked.n_attempted))
    assert [b[2] for b in befores] == list(range(hooked.n_attempted))  # n_attempted before run i == i
    nfeas = 0
    pos = 0
    for i in range(hooked.n_attempted):
        assert log[pos] == ("before", i, i, nfeas)
        nfeas += 0 if i in infeasible else 1
        assert log[pos + 1] == ("after", i, i + 1, nfeas)
        pos += 2
        if mode == "adaptive" and nfeas >= 2 and nfeas % 2 == 0:
            assert log[pos][0] == "check" and log[pos][2:] == (i + 1, nfeas)
            pos += 1
    assert pos == len(log)
    checks = [e for e in log if e[0] == "check"]
    if mode == "fixed":
        assert not checks
    elif epsilon > 0.1:
        assert checks[-1][1] is True and hooked.n_attempted < max_runs  # convergence break stops the loop
        assert all(c[1] is False for c in checks[:-1])
    else:
        assert hooked.n_attempted == max_runs and not hooked.converged and all(c[1] is False for c in checks)


def test_raising_hook_aborts_without_touching_inputs(monkeypatch, stub_pool):
    cfg = cfg_mod.config_from_dict(_raw(5, "fixed"))
    before = copy.deepcopy(cfg)
    draws: list = []
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(draws=draws))

    def boom(i, na, nf):
        if i == 2:
            raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        ensemble.run_ensemble(stub_pool, cfg, {}, before_run=boom)
    assert len(draws) == 2 and cfg == before  # run 2 never executed; config untouched


def test_run_cancelled_survives_pickle():
    """Celery moves exceptions across processes; RunCancelled must round-trip with its partial counts."""
    exc = run.RunCancelled(3, 2)
    back = pickle.loads(pickle.dumps(exc))
    assert isinstance(back, run.RunCancelled)
    assert (back.n_attempted, back.n_feasible) == (3, 2)
    assert str(back) == str(exc)


def test_run_cancelled_from_before_run_carries_counts_with_infeasible(monkeypatch, stub_pool):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(frozenset({0})))
    n = {"c": 0}

    def cancel() -> bool:
        n["c"] += 1
        return n["c"] == 4

    with pytest.raises(run.RunCancelled) as exc:
        run.run_scenario(_raw(9, "fixed"), pool=stub_pool, data_dirs={}, should_cancel=cancel)
    assert (exc.value.n_attempted, exc.value.n_feasible) == (3, 2)


def test_progress_callback_exception_propagates_and_next_call_is_clean(monkeypatch, stub_pool):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single())

    def bad(e: run.ProgressEvent) -> None:
        if e.stage == "running" and e.run_index == 1:
            raise ValueError("ui gone")

    cfg = cfg_mod.config_from_dict(_raw(4, "fixed"))
    snapshot = copy.deepcopy(cfg)
    with pytest.raises(ValueError, match="ui gone"):
        run.run_scenario(cfg, pool=stub_pool, data_dirs={}, progress=bad)
    assert cfg == snapshot  # no shared state mutated; a retry gives the full normal result
    again = run.run_scenario(cfg, pool=stub_pool, data_dirs={})
    assert again.ensemble.n_attempted == 4 and len(again.metrics) == 4


def test_exception_from_progress_is_not_swallowed_at_finish(monkeypatch, stub_pool):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single())

    def bad(e: run.ProgressEvent) -> None:
        if e.stage == "finished":
            raise ValueError("late")

    with pytest.raises(ValueError, match="late"):
        run.run_scenario(_raw(2, "fixed"), pool=stub_pool, data_dirs={}, progress=bad)


def test_finished_event_counts_with_infeasible_runs(monkeypatch, stub_pool):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(frozenset({0, 2})))
    events: list[run.ProgressEvent] = []
    res = run.run_scenario(_raw(5, "fixed"), pool=stub_pool, data_dirs={}, progress=events.append)
    last = events[-1]
    assert last.stage == "finished"
    assert (last.n_attempted, last.n_feasible, last.run_index) == (5, 3, 4)
    assert (last.n_attempted, last.n_feasible) == (res.ensemble.n_attempted, len(res.metrics))
    assert res.to_dict()["n_rejected"] == 2


def test_check_events_report_false_before_true(monkeypatch, stub_pool):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single())
    events: list[run.ProgressEvent] = []
    run.run_scenario(_raw(8, "adaptive", 1e-12), pool=stub_pool, data_dirs={}, progress=events.append)
    checks = [e for e in events if e.stage == "checking_convergence"]
    assert [c.n_feasible for c in checks] == [2, 4, 6, 8]
    assert all(c.converged is False for c in checks)
    assert events[-1].stage == "finished" and events[-1].converged is False
