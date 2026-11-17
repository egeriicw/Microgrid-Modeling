"""Seed robustness of the full ensemble (PRD FR-ENG-4(c)) on the generated synthetic dataset.

Different seed sets sample different neighborhoods, so their ensemble results differ by sampling noise only. The
test checks that the differences are consistent with that noise, rather than with seed-dependent bias.

Statistics: for seed set j, the ensemble mean of a metric is m_j with standard error SE_j = sd_j / sqrt(n_j)
(sd_j = sample standard deviation across that ensemble's feasible runs). The means of independent ensembles from
the same distribution scatter around the common mean with standard deviation about SE. We assert every seed set's
mean is within Z * SE_pooled of the grand mean of all seed sets, with Z = 4 and SE_pooled the root-mean-square of
the SE_j. Z = 4 makes a false alarm a roughly one-in-ten-thousand event per comparison under normal sampling, and
the seeds are fixed, so the outcome is deterministic: it can only change if the engine's numbers change. For the
median profile we use the annual mean of the per-timestep 50th-percentile profile; the standard error of a median
is about 1.2533 times that of a mean (normal case), so its band is widened by that factor.
"""

from __future__ import annotations

import numpy as np
import pytest

from microgrid.community_load.synthetic import run
from tests.test_run_facade import build_tiny_config

SEED_SETS = [
    {"composition_seed": 11, "archetype_seed": 12, "perturbation_seed": 13},
    {"composition_seed": 21, "archetype_seed": 22, "perturbation_seed": 23},
    {"composition_seed": 31, "archetype_seed": 32, "perturbation_seed": 33},
    {"composition_seed": 41, "archetype_seed": 42, "perturbation_seed": 43},
    {"composition_seed": 51, "archetype_seed": 52, "perturbation_seed": 53},
]
Z = 4.0
MEDIAN_SE_FACTOR = 1.2533
N_RUNS = 20


def _config(ds, seeds: dict[str, int], max_runs: int = N_RUNS) -> dict:
    cfg = build_tiny_config(ds)
    cfg["rng"] = dict(seeds)
    cfg["ensemble"] = {"mode": "fixed", "max_runs": max_runs}
    return cfg


def _summary(result) -> dict[str, float]:
    energy = np.array([m["annual_energy_kwh"] for m in result.metrics])
    peak = np.array([m["peak_kw"] for m in result.metrics])
    p50 = result.ensemble.percentile_envelopes["p50"]
    return {
        "n": float(len(energy)),
        "energy_mean": float(energy.mean()),
        "energy_sd": float(energy.std(ddof=1)),
        "peak_mean": float(peak.mean()),
        "peak_sd": float(peak.std(ddof=1)),
        "median_profile_mean": float(p50.mean()),
    }


@pytest.fixture(scope="module")
def results(synthetic_dataset):
    return [run.run_scenario(_config(synthetic_dataset, s)) for s in SEED_SETS]


def _assert_consistent(means, sds, ns, factor=1.0, what=""):
    means, sds, ns = map(np.asarray, (means, sds, ns))
    se_pooled = float(np.sqrt(np.mean((sds / np.sqrt(ns)) ** 2))) * factor
    grand = float(means.mean())
    assert se_pooled > 0, what
    assert np.all(np.abs(means - grand) <= Z * se_pooled), (what, means, grand, se_pooled)


def test_seed_sets_agree_within_sampling_error(results):
    s = [_summary(r) for r in results]
    assert all(x["n"] >= 10 for x in s)  # enough feasible runs for the standard error to mean something
    ns = [x["n"] for x in s]
    _assert_consistent([x["energy_mean"] for x in s], [x["energy_sd"] for x in s], ns, what="annual energy")
    _assert_consistent([x["peak_mean"] for x in s], [x["peak_sd"] for x in s], ns, what="peak")
    # The median profile level is on the scale of annual energy / 8760 (kW); reuse the energy spread in kW.
    sd_kw = [x["energy_sd"] / 8760.0 for x in s]
    _assert_consistent([x["median_profile_mean"] for x in s], sd_kw, ns, factor=MEDIAN_SE_FACTOR, what="median profile")


def test_replaying_result_seeds_reproduces_metrics(synthetic_dataset, results):
    first = results[0]
    replay = run.run_scenario(_config(synthetic_dataset, first.seeds))
    assert replay.seeds == first.seeds
    assert replay.metrics == first.metrics
    assert replay.ensemble.percentile_envelopes["p50"].equals(first.ensemble.percentile_envelopes["p50"])


def test_unseeded_run_records_seeds_that_replay_it(synthetic_dataset):
    first = run.run_scenario(_config(synthetic_dataset, {}, max_runs=4))
    assert set(first.seeds) == {"composition_seed", "archetype_seed", "perturbation_seed"}
    assert run.run_scenario(_config(synthetic_dataset, first.seeds, max_runs=4)).metrics == first.metrics
