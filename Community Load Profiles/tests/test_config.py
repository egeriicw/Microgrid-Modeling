"""Tests for config loading and validation."""
from pathlib import Path

import pytest

from snlg.config import (
    CategoryBounds,
    CompositionConfig,
    ScenarioConfig,
    load_config,
    validate_config,
)


def test_validate_config_passes_with_valid_bounds(minimal_cfg):
    validate_config(minimal_cfg)  # should not raise


def test_validate_config_fails_when_min_sum_exceeds_one(minimal_cfg):
    minimal_cfg.composition.categories = {
        "a": CategoryBounds(min_fraction=0.7, max_fraction=0.8, source="resstock"),
        "b": CategoryBounds(min_fraction=0.7, max_fraction=0.8, source="resstock"),
    }
    with pytest.raises(ValueError, match="Sum of min_fraction"):
        validate_config(minimal_cfg)


def test_validate_config_fails_when_max_sum_below_one(minimal_cfg):
    minimal_cfg.composition.categories = {
        "a": CategoryBounds(min_fraction=0.0, max_fraction=0.3, source="resstock"),
        "b": CategoryBounds(min_fraction=0.0, max_fraction=0.3, source="resstock"),
    }
    with pytest.raises(ValueError, match="Sum of max_fraction"):
        validate_config(minimal_cfg)


def test_fixed_count_categories_excluded_from_fraction_check(minimal_cfg):
    """fixed_count categories should not count toward the simplex bounds check."""
    minimal_cfg.composition.categories = {
        "commercial": CategoryBounds(fixed_count=4, source="comstock"),
        "mf_small": CategoryBounds(min_fraction=0.5, max_fraction=1.0, source="resstock"),
        "mf_mid": CategoryBounds(min_fraction=0.0, max_fraction=0.5, source="resstock"),
    }
    validate_config(minimal_cfg)  # should not raise


def test_load_config_roundtrip(tmp_path):
    """Write a minimal YAML and load it back."""
    yaml_content = """
scenario:
  name: test_scenario
rng:
  composition_seed: 111
  archetype_seed: 222
  perturbation_seed: 333
composition:
  total_buildings: 20
  method: uniform_renorm
  categories:
    mf_small:
      min_fraction: 0.5
      max_fraction: 1.0
      source: resstock
    mf_mid:
      min_fraction: 0.0
      max_fraction: 0.5
      source: resstock
ensemble:
  mode: fixed
  max_runs: 10
"""
    cfg_file = tmp_path / "test.yaml"
    cfg_file.write_text(yaml_content)
    cfg = load_config(cfg_file)
    assert cfg.name == "test_scenario"
    assert cfg.rng.composition_seed == 111
    assert cfg.composition.total_buildings == 20
    assert cfg.composition.method == "uniform_renorm"
    assert cfg.ensemble.max_runs == 10
