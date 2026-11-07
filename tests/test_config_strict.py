"""Strict configuration (FR-ENG-3, Sprint 1 story 1.3): nothing is silently ignored."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from microgrid.community_load.synthetic.config import load_config

SCENARIOS = Path(__file__).resolve().parents[1] / "configs" / "scenarios"

BASE = """
scenario:
  name: strict
composition:
  total_buildings: 10
  categories:
    mf_small: {min_fraction: 0.5, max_fraction: 1.0, source: resstock}
    mf_mid: {min_fraction: 0.0, max_fraction: 0.5, source: resstock}
ensemble: {mode: fixed, max_runs: 5}
"""


def _write(tmp_path, extra: str = "", base: str = BASE) -> Path:
    p = tmp_path / "cfg.yaml"
    p.write_text(textwrap.dedent(base) + textwrap.dedent(extra))
    return p


def _err(tmp_path, extra="", base=BASE) -> str:
    with pytest.raises(ValueError) as exc:
        load_config(_write(tmp_path, extra, base))
    return str(exc.value)


# --- unknown keys ---------------------------------------------------------------------------


def test_unknown_top_level_section_is_rejected(tmp_path):
    msg = _err(tmp_path, "ensamble: {max_runs: 3}\n")
    assert "ensamble" in msg


def test_unknown_nested_key_reports_its_path(tmp_path):
    msg = _err(tmp_path, base=BASE.replace("max_runs: 5", "max_run: 5"))
    assert "ensemble.max_run" in msg


def test_all_problems_are_reported_together(tmp_path):
    msg = _err(tmp_path, "bogus_a: 1\nrng: {composition_sed: 4}\n")
    assert "bogus_a" in msg and "rng.composition_sed" in msg


def test_error_names_the_file(tmp_path):
    assert "cfg.yaml" in _err(tmp_path, "bogus: 1\n")


@pytest.mark.parametrize(
    "key", ["arealimit", "garage", "heatpump", "electric_vehicle", "solar_pv", "require_data_complete"]
)
def test_legacy_ignored_keys_are_rejected_with_a_helpful_message(tmp_path, key):
    cfg = (
        BASE
        + f"""
archetype_filters:
  resstock:
    mf_small:
      building_type_heights: ["Multi-Family with 2 - 4 Units"]
      {key}: 1
"""
    )
    msg = _err(tmp_path, base=cfg)
    assert key in msg and "not supported" in msg


def test_supported_filter_keys_still_load(tmp_path):
    cfg = (
        BASE
        + """
archetype_filters:
  resstock:
    mf_small:
      building_type_heights: ["Multi-Family with 2 - 4 Units"]
      max_sqft: 2000
      require_no_garage: true
  comstock:
    commercial_private: {building_types: [SmallOffice], include_public: false}
"""
    )
    loaded = load_config(_write(tmp_path, base=cfg))
    assert loaded.archetype_filters.resstock["mf_small"].max_sqft == 2000


# --- scenario name / description ------------------------------------------------------------


def test_scenario_name_and_description_lists_are_normalised_to_strings(tmp_path):
    cfg = """
scenario:
  name:
    - "DC Multifamily Baseline"
  description:
    - "First sentence."
    - "Second sentence."
composition:
  total_buildings: 4
  categories:
    mf_small: {min_fraction: 0.0, max_fraction: 1.0, source: resstock}
"""
    loaded = load_config(_write(tmp_path, base=cfg))
    assert loaded.name == "DC Multifamily Baseline"
    assert loaded.description == "First sentence. Second sentence."


# --- enumerations ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "old,new,needle",
    [
        ("mode: fixed", "mode: adpative", "ensemble.mode"),
        ("total_buildings: 10", "total_buildings: 10\n  method: dirchlet", "composition.method"),
        ("source: resstock}\n    mf_mid", "source: restock}\n    mf_mid", "source"),
    ],
)
def test_enumerated_values_are_checked(tmp_path, old, new, needle):
    assert needle in _err(tmp_path, base=BASE.replace(old, new, 1))


# --- numeric sanity -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "old,new,needle",
    [
        ("total_buildings: 10", "total_buildings: 0", "total_buildings"),
        ("max_runs: 5", "max_runs: 0", "max_runs"),
        ("min_fraction: 0.5, max_fraction: 1.0", "min_fraction: 0.9, max_fraction: 0.2", "mf_small"),
        ("min_fraction: 0.5, max_fraction: 1.0", "min_fraction: -0.1, max_fraction: 1.0", "mf_small"),
    ],
)
def test_numeric_values_are_sanity_checked(tmp_path, old, new, needle):
    assert needle in _err(tmp_path, base=BASE.replace(old, new, 1))


def test_negative_fixed_count_is_rejected(tmp_path):
    cfg = """
composition:
  total_buildings: 4
  categories:
    a: {fixed_count: -1, source: resstock}
"""
    assert "fixed_count" in _err(tmp_path, base=cfg)


def test_percentiles_must_be_between_zero_and_one(tmp_path):
    assert "percentiles" in _err(tmp_path, "output: {percentiles: [0.1, 1.5]}\n")


# --- seeds and shipped configs --------------------------------------------------------------


def test_omitted_rng_section_means_fresh_seeds(tmp_path):
    loaded = load_config(_write(tmp_path))
    assert loaded.rng.composition_seed is None


@pytest.mark.parametrize("name", sorted(p.name for p in SCENARIOS.glob("*.yaml")))
def test_shipped_scenarios_load_cleanly(name):
    assert load_config(SCENARIOS / name).composition.categories
