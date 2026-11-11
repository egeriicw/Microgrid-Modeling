"""Engine facade on the committed fixture dataset (FR-ENG-8; data-free tests are in test_run_facade_core.py).

Tests that need real parquet/Excel inputs skip until the synthetic fixture dataset described in
tests/fixtures/data/README.md exists (needs background/chars_resstock.xlsx, background/chars_comstock.xlsx and
input/{resstock,comstock}/timeseries_individual_buildings/upgrade=0/state=DC/*.parquet). They un-skip
automatically when those files appear.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from microgrid.community_load.synthetic import config as cfg_mod
from microgrid.community_load.synthetic import run

FIXTURES = Path(__file__).parent / "fixtures" / "data"
HAVE_FIXTURES = (
    (FIXTURES / "background" / "chars_resstock.xlsx").exists()
    and (FIXTURES / "background" / "chars_comstock.xlsx").exists()
    and any((FIXTURES / "input" / "resstock").rglob("*.parquet"))
)
needs_fixtures = pytest.mark.skipif(
    not HAVE_FIXTURES,
    reason="needs the synthetic fixture dataset (tests/fixtures/data: chars_*.xlsx + parquet timeseries), "
    "being built by the data-engineer; not yet merged",
)


@pytest.fixture
def tiny_config() -> dict:
    return build_tiny_config()


def build_tiny_config() -> dict:
    """Minimal scenario dict pointing at tests/fixtures/data/."""
    return {
        "scenario": {"name": "tiny"},
        "rng": {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3},
        "data": {
            "background_dir": str(FIXTURES / "background"),
            "resstock_chars_file": "chars_resstock.xlsx",
            "comstock_chars_file": "chars_comstock.xlsx",
            "resstock_timeseries_dir": str(FIXTURES / "input/resstock/timeseries_individual_buildings"),
            "comstock_timeseries_dir": str(FIXTURES / "input/comstock/timeseries_individual_buildings"),
        },
        "composition": {
            "total_buildings": 4,
            "method": "dirichlet",
            "categories": {
                "mf_small": {"min_fraction": 0.5, "max_fraction": 1.0, "source": "resstock"},
                "mf_mid": {"min_fraction": 0.0, "max_fraction": 0.5, "source": "resstock"},
            },
        },
        "archetype_filters": {
            "resstock": {
                "mf_small": {
                    "building_type_heights": [
                        "Multi-Family with 2 - 4 Units",
                        "Multi-Family with 5+ Units, 1-3 Stories",
                    ]
                },
                "mf_mid": {"building_type_heights": ["Multi-Family with 5+ Units, 4-7 Stories"]},
            }
        },
        "ensemble": {"mode": "fixed", "max_runs": 6},
        "perturbation": {"enabled": False},
    }


def test_prebuilt_scenarioconfig_with_missing_inputs_fails_loudly(tmp_path):
    cfg = cfg_mod.config_from_dict({"data": {"background_dir": str(tmp_path)}})
    with pytest.raises(FileNotFoundError):
        run.run_scenario(cfg)


@needs_fixtures
def test_returns_run_result(tiny_config):
    result = run.run_scenario(tiny_config)
    assert result.engine_version
    assert result.seeds == {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3}
    assert 1 <= len(result.metrics) <= 6
    assert result.ensemble.n_attempted == 6
    assert result.to_dict()["n_attempted"] == 6


@needs_fixtures
def test_progress_events(tiny_config):
    events: list[run.ProgressEvent] = []
    run.run_scenario(tiny_config, progress=events.append)
    assert [e.stage for e in events].count("running") == 6
    assert events[0].stage == "building_pool" and events[-1].stage == "finished"


@needs_fixtures
def test_cooperative_cancel_stops_early(tiny_config):
    calls = {"n": 0}

    def should_cancel() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2  # cancel before the 2nd run

    with pytest.raises(run.RunCancelled) as exc:
        run.run_scenario(tiny_config, should_cancel=should_cancel)
    assert exc.value.n_attempted == 1
