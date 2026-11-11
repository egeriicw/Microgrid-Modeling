"""Determinism contract (FR-ENG-4 / NFR-1): same config + same 3 seeds + same engine_version
=> identical scalar metrics, checked against a committed golden file.

The golden file is a regression tripwire for the numbers themselves, computed on the generated synthetic dataset
(tests/fixtures/synthetic_data.py, default seed), so it runs in normal CI and needs no real data. If it fails, the
engine's numbers changed. That is only acceptable as an intentional methodology change (owner-approved) together
with an engine_version bump; then regenerate with `pytest tests/test_determinism.py --update-golden` and commit
the diff. Never regenerate it to make a failing test pass. Floats are compared with a tight relative tolerance
(1e-9) so that last-bit platform differences do not cause false alarms.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from microgrid.community_load.synthetic import run
from tests.test_run_facade import build_tiny_config

GOLDEN = Path(__file__).parent / "golden" / "dc_multifamily_tiny.json"
SCALAR_KEYS = ("peak_kw", "annual_energy_kwh", "load_factor", "soft_score", "feasible")


@pytest.fixture
def tiny_config(synthetic_dataset) -> dict:  # the facade's tiny config, with the golden's seeds
    cfg = build_tiny_config(synthetic_dataset)
    cfg["scenario"]["name"] = "dc_multifamily_tiny"
    cfg["rng"] = {"composition_seed": 1001, "archetype_seed": 2002, "perturbation_seed": 3003}
    return cfg


def _scalars(result):
    return [{k: (bool(m[k]) if k == "feasible" else float(m[k])) for k in SCALAR_KEYS} for m in result.metrics]


def test_two_runs_are_identical(tiny_config):
    assert _scalars(run.run_scenario(tiny_config)) == _scalars(run.run_scenario(tiny_config))


def test_changing_a_seed_changes_a_metric(tiny_config):
    base = _scalars(run.run_scenario(tiny_config))
    tiny_config["rng"]["composition_seed"] = 9999
    assert base != _scalars(run.run_scenario(tiny_config))


def test_matches_golden(tiny_config, request):
    result = run.run_scenario(tiny_config)
    actual = {"engine_version": result.engine_version, "runs": _scalars(result)}

    if request.config.getoption("--update-golden", default=False):
        GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN.write_text(json.dumps(actual, indent=2))
        pytest.skip("golden regenerated")

    assert GOLDEN.exists(), "golden file missing: run with --update-golden"
    expected = json.loads(GOLDEN.read_text())
    assert expected["engine_version"] == actual["engine_version"], (
        "engine_version changed; regenerate golden intentionally"
    )
    assert len(expected["runs"]) == len(actual["runs"])
    for exp, act in zip(expected["runs"], actual["runs"], strict=True):
        assert exp["feasible"] == act["feasible"]
        for k in SCALAR_KEYS:
            if k != "feasible":
                assert act[k] == pytest.approx(exp[k], rel=1e-9), k
