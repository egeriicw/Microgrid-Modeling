"""FR-ENG-13 (b): building-level hourly and 15-minute load is retrievable per sampled building, not only as totals.

``ensemble.building_profiles`` rebuilds each sampled building's profile of one run (same loading, multifamily
scaling and recorded perturbation as the run itself). Known answer: the buildings sum exactly to the run's totals.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from microgrid.community_load.synthetic import archetype, ensemble, loader, run
from microgrid.community_load.synthetic.config import config_from_dict
from microgrid.community_load.synthetic.results import ENERGY_RTOL
from tests.test_run_facade import build_tiny_config


def _setup(ds, perturb: dict):
    raw = build_tiny_config(ds)
    raw["perturbation"] = perturb
    cfg = config_from_dict(raw)
    bg = Path(cfg.data.background_dir)
    pool = archetype.build_archetype_pool(bg / cfg.data.resstock_chars_file, bg / cfg.data.comstock_chars_file, cfg)
    return cfg, pool, run.run_scenario(cfg, pool=pool)


@pytest.mark.parametrize(
    "perturb",
    [{"enabled": False}, {"enabled": True, "max_temporal_shift_hours": 3}],
    ids=["no_perturbation", "temporal_shift"],
)
def test_buildings_sum_to_the_run_totals(synthetic_dataset, perturb):
    loader.clear_cache()
    cfg, pool, result = _setup(synthetic_dataset, perturb)
    assert result.ensemble.runs
    shifted = False
    for r in result.ensemble.runs:
        profiles = ensemble.building_profiles(r, pool, cfg, synthetic_dataset.data_dirs)
        assert [(p.bldg_id, p.category, p.source) for p in profiles] == [
            (s.bldg_id, s.category, s.source) for s in r.selections
        ]
        shifted |= any(p.perturbation_log.get("shift_hours") for p in profiles)
        for p in profiles:
            assert len(p.hourly_kwh) == 8760
            assert p.kwh_15min is not None and len(p.kwh_15min) == 35040
            # hourly is summed from float32 source values: the engine's own conservation tolerance applies
            assert p.kwh_15min.sum() == pytest.approx(p.hourly_kwh.sum(), rel=ENERGY_RTOL)
        total_h = sum(p.hourly_kwh.to_numpy() for p in profiles)
        np.testing.assert_allclose(total_h, r.total_profile.to_numpy(), rtol=1e-12, atol=0)
        res_q = sum(p.kwh_15min.to_numpy() for p in profiles if p.source == "resstock")
        np.testing.assert_allclose(res_q, r.residential_profile_15min.to_numpy(), rtol=1e-12, atol=0)
    if perturb["enabled"]:
        assert shifted, "the shift case must actually exercise a non-zero recorded shift"


def test_unreplayable_perturbation_log_fails_loudly(synthetic_dataset):
    cfg, pool, result = _setup(synthetic_dataset, {"enabled": False})
    r = result.ensemble.runs[0]
    r.perturbation_logs[0] = {"end_use_factors": {"hvac": 1.1}}
    with pytest.raises(NotImplementedError, match="end_use_factors"):
        ensemble.building_profiles(r, pool, cfg, synthetic_dataset.data_dirs)
