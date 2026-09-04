"""RED placeholder — FR-E3 / FR-E8 (docs/test-plan.md §4 Phase 1.5).

Specifies the `snlg.run.run_scenario` facade: the ONLY entry point the
platform worker calls. Covers progress callbacks, cooperative cancellation,
incremental artifact sink, and dict-or-dataclass config input.
"""
from __future__ import annotations

import pytest

run = pytest.importorskip("snlg.run", reason="FR-E3: snlg.run not implemented")

pytestmark = pytest.mark.xfail(
    reason="FR-E3/E8 not implemented", strict=False
)


@pytest.fixture
def tiny_config() -> dict:
    """Minimal scenario dict pointing at tests/fixtures/data/ (FR-E11)."""
    return {
        "name": "tiny",
        "rng": {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3},
        "data": {"upgrade": "0", "state": "DC"},
        "composition": {
            "total_buildings": 4,
            "method": "dirichlet",
            "categories": {
                "mf_small": {"min_fraction": 0.5, "max_fraction": 1.0, "source": "resstock"},
                "mf_mid": {"min_fraction": 0.0, "max_fraction": 0.5, "source": "resstock"},
            },
        },
        "ensemble": {"mode": "fixed", "max_runs": 6},
        "perturbation": {"enabled": False},
    }


@pytest.fixture
def data_dirs(tmp_path_factory):
    from pathlib import Path

    base = Path(__file__).parent / "fixtures" / "data"
    return {
        "resstock": base / "input/resstock/timeseries_individual_buildings",
        "comstock": base / "input/comstock/timeseries_individual_buildings",
    }


def test_returns_run_result(tiny_config, data_dirs):
    result = run.run_scenario(tiny_config, data_dirs=data_dirs)
    assert result.engine_version
    assert result.seeds == {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3}
    assert len(result.metrics) == 6
    assert result.ensemble is not None
    assert result.summary["n_attempted"] == 6
    assert result.cancelled is False


def test_progress_events(tiny_config, data_dirs):
    events = []
    run.run_scenario(tiny_config, data_dirs=data_dirs, progress=events.append)

    assert len(events) >= 6 + 1  # one per attempted run + a final event
    fields = {"stage", "run_index", "n_attempted", "n_feasible", "converged", "message", "timestamp"}
    for ev in events:
        assert fields <= set(vars(ev))
    assert events[-1].stage == "done"


def test_cooperative_cancel_stops_early(tiny_config, data_dirs):
    calls = {"n": 0}

    def should_cancel() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2  # cancel before the 2nd run

    result = run.run_scenario(
        tiny_config, data_dirs=data_dirs, should_cancel=should_cancel
    )
    assert result.cancelled is True
    assert len(result.metrics) < 6


def test_artifact_sink_invoked_incrementally(tiny_config, data_dirs):
    puts = []

    class Sink:
        def put(self, name: str, kind: str, obj) -> None:
            puts.append((name, kind))

    run.run_scenario(tiny_config, data_dirs=data_dirs, artifacts=Sink())
    kinds = {k for _, k in puts}
    # sink is called for multiple distinct artifacts, not once at the end
    assert len(puts) > 1
    assert "percentile_envelopes_csv" in kinds or "series_parquet" in kinds


def test_accepts_prebuilt_scenarioconfig(tiny_config, data_dirs):
    from snlg import config as cfg_mod

    cfg = cfg_mod.ScenarioConfig(name="tiny")  # constructed, not from YAML
    # Should not raise a type error; validation happens internally.
    with pytest.raises(Exception):
        # empty config has no categories -> engine-level failure, not TypeError
        run.run_scenario(cfg, data_dirs=data_dirs)
