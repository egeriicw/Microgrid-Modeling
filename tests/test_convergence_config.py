"""FR-ENG-6: convergence criteria are configurable, strictly validated, and every problem is reported at once."""

from __future__ import annotations

import pytest

from microgrid.community_load.synthetic.config import config_from_dict, config_to_dict


def _bad(raw: dict) -> str:
    with pytest.raises(ValueError) as exc:
        config_from_dict(raw)
    return str(exc.value)


def test_prd_rule_parses_with_its_own_keys():
    cfg = config_from_dict(
        {"convergence": {"rule": "prd_default", "epsilon": 0.02, "consecutive_checks": 4, "se_target": 0.03}}
    )
    c = cfg.convergence
    assert (c.rule, c.epsilon, c.consecutive_checks, c.se_target) == ("prd_default", 0.02, 4, 0.03)


def test_adaptive_max_runs_defaults_to_1000_but_fixed_default_is_unchanged():
    assert config_from_dict({"ensemble": {"mode": "adaptive"}}).ensemble.max_runs == 1000
    assert config_from_dict({"ensemble": {"mode": "fixed"}}).ensemble.max_runs == 50
    assert config_from_dict({}).ensemble.max_runs == 50
    assert config_from_dict({"ensemble": {"mode": "adaptive", "max_runs": 77}}).ensemble.max_runs == 77


def test_snapshot_round_trips_for_both_rules():
    for conv in (
        {"rule": "prd_default", "epsilon": 0.02, "consecutive_checks": 2, "se_target": 0.01},
        {"rule": "legacy_window", "lookback_window": 3, "min_stable_window": 2, "epsilon": 0.05},
        {},
    ):
        cfg = config_from_dict({"convergence": conv})
        assert config_from_dict(config_to_dict(cfg)) == cfg


def test_all_problems_are_reported_together():
    msg = _bad(
        {
            "ensemble": {"mode": "adaptive", "check_every": 0, "min_runs": "ten"},
            "convergence": {
                "rule": "prd_default",
                "epsilon": -1,
                "consecutive_checks": 0,
                "se_target": 0,
                "lookback_window": 5,
            },
        }
    )
    for needle in ("check_every", "min_runs", "epsilon", "consecutive_checks", "se_target", "lookback_window"):
        assert needle in msg, (needle, msg)


@pytest.mark.parametrize(
    ("conv", "needle"),
    [
        ({"rule": "magic"}, "convergence.rule"),
        ({"epsilon": "0.01"}, "convergence.epsilon"),
        ({"epsilon": True}, "convergence.epsilon"),
        ({"epsilon": float("nan")}, "convergence.epsilon"),
        ({"epsilon": float("inf")}, "convergence.epsilon"),
        ({"epsilon": 0}, "epsilon"),
        ({"lookback_window": 2.5}, "convergence.lookback_window"),
        ({"min_stable_window": 0}, "min_stable_window"),
        ({"metrics": []}, "convergence.metrics"),
        ({"metrics": ["peak_kw", "bogus"]}, "convergence.metrics"),
        ({"metrics": "peak_kw"}, "convergence.metrics"),
        ({"rule": "prd_default", "consecutive_checks": 1.5}, "consecutive_checks"),
        ({"rule": "prd_default", "se_target": -0.1}, "se_target"),
        ({"rule": "prd_default", "metrics": ["peak_kw"]}, "only used by rule 'legacy_window'"),
        ({"se_target": 0.1}, "only used by rule 'prd_default'"),
        ({"consecutive_checks": 2}, "only used by rule 'prd_default'"),
        ({"epsilno": 0.1}, "did you mean"),
    ],
)
def test_bad_convergence_values_are_rejected_loudly(conv, needle):
    assert needle in _bad({"convergence": conv})


@pytest.mark.parametrize(
    ("ens", "needle"),
    [
        ({"check_every": 0}, "check_every"),
        ({"check_every": True}, "ensemble.check_every"),
        ({"max_runs": "50"}, "ensemble.max_runs"),
        ({"min_runs": 0}, "min_runs"),
        ({"mode": "adaptive", "min_runs": 30, "max_runs": 10}, "never check convergence"),
        ({"mode": "sometimes"}, "ensemble.mode"),
    ],
)
def test_bad_ensemble_values_are_rejected_loudly(ens, needle):
    assert needle in _bad({"ensemble": ens})
