"""Facade tests that need no real data: stub pool + stubbed single run (FR-ENG-8)."""

from __future__ import annotations

import json

import pandas as pd
import pytest
import yaml

from microgrid import __version__
from microgrid.community_load.synthetic import config as cfg_mod
from microgrid.community_load.synthetic import ensemble, run
from microgrid.community_load.synthetic._types import (
    ArchetypePool,
    NeighborhoodComposition,
    NeighborhoodResult,
)


@pytest.fixture
def stub_pool() -> ArchetypePool:
    return ArchetypePool(
        residential={},
        commercial={},
        resstock_chars=pd.DataFrame(),
        comstock_chars=pd.DataFrame(),
    )


def _raw(max_runs: int = 4, mode: str = "fixed", seeds: bool = True) -> dict:
    raw: dict = {
        "scenario": {"name": "stub"},
        "ensemble": {"mode": mode, "max_runs": max_runs, "min_runs": 2, "check_every": 2},
        "convergence": {"lookback_window": 1, "min_stable_window": 1, "epsilon": 0.5},
    }
    if seeds:
        raw["rng"] = {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3}
    return raw


def _fake_single(infeasible: frozenset[int] = frozenset()):
    """Stand-in for ensemble._run_single; numbers depend only on the per-run rng."""

    def fake(run_index, pool, cfg, run_rng, data_dirs):
        peak = 100.0 + float(run_rng.composition.integers(1000)) / 10.0
        idx = pd.date_range("2018-01-01", periods=3, freq="h")
        s = pd.Series([peak, peak / 2, peak / 4], index=idx)
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


@pytest.fixture(autouse=True)
def _patch(monkeypatch):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single())


def test_progress_events_in_order_with_counts(stub_pool):
    events: list[run.ProgressEvent] = []
    run.run_scenario(_raw(4), pool=stub_pool, data_dirs={}, progress=events.append)
    assert [e.stage for e in events] == ["building_pool", "running", "running", "running", "running", "finished"]
    runs = [e for e in events if e.stage == "running"]
    assert [e.run_index for e in runs] == [0, 1, 2, 3]
    assert [e.n_attempted for e in runs] == [1, 2, 3, 4]
    assert [e.n_feasible for e in runs] == [1, 2, 3, 4]
    assert all(e.max_runs == 4 for e in events)
    assert events[-1].converged is True and events[-1].n_attempted == 4
    assert all(isinstance(e.message, str) and e.message for e in events)


def test_infeasible_runs_counted_separately(stub_pool, monkeypatch):
    monkeypatch.setattr(ensemble, "_run_single", _fake_single(frozenset({1})))
    events: list[run.ProgressEvent] = []
    res = run.run_scenario(_raw(3), pool=stub_pool, data_dirs={}, progress=events.append)
    runs = [e for e in events if e.stage == "running"]
    assert [e.n_feasible for e in runs] == [1, 1, 2]
    assert [e.n_attempted for e in runs] == [1, 2, 3]
    assert len(res.metrics) == 2  # only feasible runs are ensemble members


def test_convergence_check_events(stub_pool):
    events: list[run.ProgressEvent] = []
    res = run.run_scenario(_raw(10, "adaptive"), pool=stub_pool, data_dirs={}, progress=events.append)
    checks = [e for e in events if e.stage == "checking_convergence"]
    assert checks, "adaptive mode must emit convergence-check events"
    assert checks[-1].converged is True
    assert res.ensemble.converged is True
    assert events[-1].stage == "finished" and events[-1].converged is True
    # a check event directly follows the run event that triggered it
    i = events.index(checks[0])
    assert events[i - 1].stage == "running"
    assert events[i - 1].n_attempted == checks[0].n_attempted


def test_cancel_between_runs_reports_partial_counts(stub_pool):
    calls = {"n": 0}

    def should_cancel() -> bool:
        calls["n"] += 1
        return calls["n"] >= 3  # allow 2 runs, cancel before the 3rd

    with pytest.raises(run.RunCancelled) as exc:
        run.run_scenario(_raw(6), pool=stub_pool, data_dirs={}, should_cancel=should_cancel)
    assert exc.value.n_attempted == 2
    assert exc.value.n_feasible == 2


def test_cancel_before_first_run(stub_pool):
    with pytest.raises(run.RunCancelled) as exc:
        run.run_scenario(_raw(3), pool=stub_pool, data_dirs={}, should_cancel=lambda: True)
    assert exc.value.n_attempted == 0 and exc.value.n_feasible == 0


def test_dict_config_typo_key_fails_like_yaml(tmp_path, stub_pool):
    raw = _raw()
    raw["rng"]["compostion_seed"] = 5
    raw["ensamble"] = {"max_runs": 3}
    with pytest.raises(ValueError, match="unknown key") as dict_exc:
        run.run_scenario(raw, pool=stub_pool, data_dirs={})
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="unknown key") as yaml_exc:
        cfg_mod.load_config(p)
    for exc in (dict_exc, yaml_exc):
        assert "compostion_seed" in str(exc.value) and "ensamble" in str(exc.value)
    assert str(dict_exc.value).startswith("<dict>:")


def test_dict_config_invariants_validated(stub_pool):
    raw = _raw()
    raw["ensemble"]["mode"] = "sometimes"
    with pytest.raises(ValueError, match="ensemble.mode"):
        run.run_scenario(raw, pool=stub_pool, data_dirs={})


def test_config_from_dict_matches_load_config(tmp_path):
    raw = _raw()
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(raw))
    assert cfg_mod.config_from_dict(raw, default_name="c") == cfg_mod.load_config(p)


def test_accepts_scenarioconfig_instance(stub_pool):
    cfg = cfg_mod.config_from_dict(_raw(2))
    res = run.run_scenario(cfg, pool=stub_pool, data_dirs={})
    assert len(res.metrics) == 2


def test_rejects_wrong_config_type(stub_pool):
    with pytest.raises(TypeError, match="ScenarioConfig or a dict"):
        run.run_scenario(["not", "a", "config"], pool=stub_pool, data_dirs={})  # type: ignore[arg-type]


def test_fixed_seeds_recorded_and_replay_reproduces(stub_pool):
    a = run.run_scenario(_raw(4), pool=stub_pool, data_dirs={})
    assert a.seeds == {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3}
    assert a.engine_version == __version__
    replay = _raw(4)
    replay["rng"] = dict(a.seeds)
    assert run.run_scenario(replay, pool=stub_pool, data_dirs={}).metrics == a.metrics
    other = _raw(4)
    other["rng"]["composition_seed"] = 999
    assert run.run_scenario(other, pool=stub_pool, data_dirs={}).metrics != a.metrics


def test_unresolved_seeds_are_recorded_and_replayable(stub_pool):
    a = run.run_scenario(_raw(4, seeds=False), pool=stub_pool, data_dirs={})
    assert set(a.seeds) == {"composition_seed", "archetype_seed", "perturbation_seed"}
    assert all(isinstance(v, int) for v in a.seeds.values())
    # the snapshot holds the seeds actually used, so it alone can replay the run
    assert a.config_snapshot["rng"] == a.seeds
    replay = _raw(4, seeds=False)
    replay["rng"] = dict(a.seeds)
    assert run.run_scenario(replay, pool=stub_pool, data_dirs={}).metrics == a.metrics


def test_metrics_shape(stub_pool):
    res = run.run_scenario(_raw(2), pool=stub_pool, data_dirs={})
    assert set(res.metrics[0]) >= {
        "run_index",
        "peak_kw",
        "annual_energy_kwh",
        "load_factor",
        "soft_score",
        "feasible",
    }


def test_to_dict_is_json_serializable(stub_pool):
    res = run.run_scenario(_raw(3), pool=stub_pool, data_dirs={})
    d = json.loads(json.dumps(res.to_dict()))
    assert set(d) >= {"metrics", "seeds", "engine_version", "config_snapshot", "converged", "n_attempted"}
    assert d["config_snapshot"]["rng"]["composition_seed"] == 1
    assert len(d["metrics"]) == 3
    assert d["engine_version"] == __version__


def test_pool_built_from_cfg_paths_when_not_given(monkeypatch, tmp_path, stub_pool):
    seen: dict = {}

    def fake_build(res_path, com_path, cfg):
        seen["paths"] = (res_path, com_path)
        return stub_pool

    monkeypatch.setattr(run.archetype, "build_archetype_pool", fake_build)
    raw = _raw(1)
    raw["data"] = {
        "resstock_chars_file": "r.xlsx",
        "comstock_chars_file": "c.xlsx",
        "background_dir": str(tmp_path),
    }
    events: list[run.ProgressEvent] = []
    run.run_scenario(raw, data_dirs={}, progress=events.append)
    assert [p.name for p in seen["paths"]] == ["r.xlsx", "c.xlsx"]
    assert events[0].stage == "building_pool"


def test_data_dirs_default_from_cfg(monkeypatch, stub_pool):
    got: dict = {}

    def spy(run_index, pool, cfg, run_rng, data_dirs):
        got["dirs"] = data_dirs
        return _fake_single()(run_index, pool, cfg, run_rng, data_dirs)

    monkeypatch.setattr(ensemble, "_run_single", spy)
    raw = _raw(1)
    raw["data"] = {"resstock_timeseries_dir": "/x/res", "comstock_timeseries_dir": "/x/com"}
    run.run_scenario(raw, pool=stub_pool)
    assert {k: str(v) for k, v in got["dirs"].items()} == {"resstock": "/x/res", "comstock": "/x/com"}


def test_ensemble_hooks_default_to_noop(stub_pool):
    cfg = cfg_mod.config_from_dict(_raw(3))
    res = ensemble.run_ensemble(stub_pool, cfg, {})
    assert res.n_attempted == 3
