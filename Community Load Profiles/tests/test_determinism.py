"""RED placeholder — FR-E4 / NFR-1 (docs/test-plan.md §4 Phase 1.6 + §8).

The determinism contract: same config + same 3 seeds + same engine_version
=> byte-identical scalar metrics, checked against a committed golden file.
Regenerate the golden ONLY via `--update-golden` alongside an intentional
methodology change + a version bump (see test-plan.md §8).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

run = pytest.importorskip("snlg.run", reason="FR-E3: snlg.run not implemented")

pytestmark = pytest.mark.xfail(reason="FR-E4 not implemented", strict=False)

GOLDEN = Path(__file__).parent / "golden" / "dc_multifamily_tiny.json"
SCALAR_KEYS = ("peak_kw", "annual_energy_kwh", "load_factor", "soft_score", "feasible")


@pytest.fixture
def tiny_config() -> dict:
    return {
        "name": "dc_multifamily_tiny",
        "rng": {"composition_seed": 1001, "archetype_seed": 2002, "perturbation_seed": 3003},
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
def data_dirs():
    base = Path(__file__).parent / "fixtures" / "data"
    return {
        "resstock": base / "input/resstock/timeseries_individual_buildings",
        "comstock": base / "input/comstock/timeseries_individual_buildings",
    }


def _scalars(result):
    return [
        {k: (bool(m[k]) if k == "feasible" else float(m[k])) for k in SCALAR_KEYS}
        for m in result.metrics
    ]


def test_two_runs_are_identical(tiny_config, data_dirs):
    a = run.run_scenario(tiny_config, data_dirs=data_dirs)
    b = run.run_scenario(tiny_config, data_dirs=data_dirs)
    assert _scalars(a) == _scalars(b)


def test_changing_a_seed_changes_a_metric(tiny_config, data_dirs):
    base = _scalars(run.run_scenario(tiny_config, data_dirs=data_dirs))
    tiny_config["rng"]["composition_seed"] = 9999
    other = _scalars(run.run_scenario(tiny_config, data_dirs=data_dirs))
    assert base != other


@pytest.mark.integration
def test_matches_golden(tiny_config, data_dirs, request):
    result = run.run_scenario(tiny_config, data_dirs=data_dirs)
    actual = {"engine_version": result.engine_version, "runs": _scalars(result)}

    if request.config.getoption("--update-golden", default=False):
        GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN.write_text(json.dumps(actual, indent=2))
        pytest.skip("golden regenerated")

    assert GOLDEN.exists(), "golden file missing — run with --update-golden"
    expected = json.loads(GOLDEN.read_text())
    assert expected["engine_version"] == actual["engine_version"], (
        "engine_version changed; regenerate golden intentionally (test-plan §8)"
    )
    assert expected["runs"] == actual["runs"]
