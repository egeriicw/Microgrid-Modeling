"""Engine facade on the generated synthetic dataset (FR-ENG-8; data-free tests are in test_run_facade_core.py).

The dataset comes from the session fixture ``synthetic_dataset`` (tests/fixtures/synthetic_data.py), which writes
the real on-disk layout under a temp directory; no committed data files are needed.
"""

from __future__ import annotations

import pytest

from microgrid.community_load.synthetic import config as cfg_mod
from microgrid.community_load.synthetic import run


@pytest.fixture
def tiny_config(synthetic_dataset) -> dict:
    return build_tiny_config(synthetic_dataset)


def build_tiny_config(ds) -> dict:
    """Minimal scenario dict pointing at the generated synthetic dataset ``ds`` (read-only use)."""
    return {
        "scenario": {"name": "tiny"},
        "rng": {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3},
        "data": ds.data,
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


def test_returns_run_result(tiny_config):
    result = run.run_scenario(tiny_config)
    assert result.engine_version
    assert result.seeds == {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3}
    assert 1 <= len(result.metrics) <= 6
    assert result.ensemble.n_attempted == 6
    assert result.to_dict()["n_attempted"] == 6


def test_progress_events(tiny_config):
    events: list[run.ProgressEvent] = []
    run.run_scenario(tiny_config, progress=events.append)
    assert [e.stage for e in events].count("running") == 6
    assert events[0].stage == "building_pool" and events[-1].stage == "finished"


def test_cooperative_cancel_stops_early(tiny_config):
    calls = {"n": 0}

    def should_cancel() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2  # cancel before the 2nd run

    with pytest.raises(run.RunCancelled) as exc:
        run.run_scenario(tiny_config, should_cancel=should_cancel)
    assert exc.value.n_attempted == 1


def test_tiny_config_pool_is_nonempty_and_excludes_bad_rows(synthetic_dataset, tiny_config):
    """Guards the tiny config against fixture drift: every category has candidates after the completeness filter
    and the deliberately bad rows are gone."""
    from pathlib import Path

    from microgrid.community_load.synthetic import archetype

    cfg = cfg_mod.config_from_dict(tiny_config)
    bg = Path(cfg.data.background_dir)
    pool = archetype.build_archetype_pool(bg / cfg.data.resstock_chars_file, bg / cfg.data.comstock_chars_file, cfg)
    for category in ("mf_small", "mf_mid"):
        assert pool.residential[category], category
    in_pool = {int(b) for ids in pool.residential.values() for b in ids}
    assert not in_pool & set(synthetic_dataset.bad_resstock_ids)
    assert any(m["feasible"] for m in run.run_scenario(tiny_config).metrics)
