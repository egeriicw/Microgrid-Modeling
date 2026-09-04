"""Tests for composition sampling."""
import numpy as np
import pytest

from snlg.composition import (
    _project_simplex_box,
    _proportions_to_counts,
    _sample_dirichlet,
    _sample_uniform_renorm,
    sample_composition,
)
from snlg.config import CategoryBounds


# ---------------------------------------------------------------------------
# _project_simplex_box
# ---------------------------------------------------------------------------

def test_project_simplex_box_identity():
    p = np.array([0.5, 0.5])
    p_min = np.array([0.0, 0.0])
    p_max = np.array([1.0, 1.0])
    result = _project_simplex_box(p, p_min, p_max)
    np.testing.assert_allclose(result.sum(), 1.0, atol=1e-9)


def test_project_simplex_box_respects_bounds():
    rng = np.random.default_rng(0)
    tested = 0
    for _ in range(200):
        p_min = rng.uniform(0.0, 0.2, 3)
        p_max = p_min + rng.uniform(0.15, 0.4, 3)
        # Only test well-conditioned cases: feasible region has meaningful width
        if p_min.sum() > 1.0 - 0.05 or p_max.sum() < 1.0 + 0.05:
            continue
        raw_p = rng.dirichlet(np.ones(3))
        result = _project_simplex_box(raw_p, p_min, p_max)
        assert np.all(result >= p_min - 1e-9), f"p_min violated: {result} < {p_min}"
        assert np.all(result <= p_max + 1e-9), f"p_max violated: {result} > {p_max}"
        np.testing.assert_allclose(result.sum(), 1.0, atol=1e-8)
        tested += 1
        if tested >= 30:
            break


# ---------------------------------------------------------------------------
# _proportions_to_counts
# ---------------------------------------------------------------------------

def test_counts_sum_exactly_to_total():
    rng = np.random.default_rng(42)
    for total in [10, 50, 100, 73]:
        props = rng.dirichlet(np.ones(5))
        counts = _proportions_to_counts(props, total)
        assert counts.sum() == total, f"Expected {total}, got {counts.sum()}"
        assert np.all(counts >= 0)


# ---------------------------------------------------------------------------
# _sample_dirichlet
# ---------------------------------------------------------------------------

def test_dirichlet_counts_sum_to_total():
    cats = ["a", "b", "c"]
    p_min = np.array([0.2, 0.1, 0.0])
    p_max = np.array([0.8, 0.6, 0.5])
    rng = np.random.default_rng(0)
    for _ in range(20):
        counts = _sample_dirichlet(cats, p_min, p_max, 5.0, 50, rng)
        assert sum(counts.values()) == 50


def test_dirichlet_counts_non_negative():
    cats = ["a", "b"]
    p_min = np.array([0.3, 0.3])
    p_max = np.array([0.7, 0.7])
    rng = np.random.default_rng(0)
    for _ in range(20):
        counts = _sample_dirichlet(cats, p_min, p_max, 5.0, 20, rng)
        assert all(v >= 0 for v in counts.values())


# ---------------------------------------------------------------------------
# _sample_uniform_renorm
# ---------------------------------------------------------------------------

def test_uniform_renorm_counts_sum_to_total():
    cats = ["a", "b", "c"]
    p_min = np.array([0.1, 0.1, 0.1])
    p_max = np.array([0.6, 0.6, 0.6])
    rng = np.random.default_rng(1)
    for _ in range(20):
        counts = _sample_uniform_renorm(cats, p_min, p_max, 30, rng)
        assert sum(counts.values()) == 30


# ---------------------------------------------------------------------------
# sample_composition (public API)
# ---------------------------------------------------------------------------

def test_sample_composition_dirichlet(minimal_cfg, rng_bundle):
    for _ in range(10):
        comp = sample_composition(None, minimal_cfg, rng_bundle.composition)
        assert sum(comp.counts.values()) == minimal_cfg.composition.total_buildings
        assert all(v >= 0 for v in comp.counts.values())


def test_sample_composition_uniform_renorm(minimal_cfg, rng_bundle):
    minimal_cfg.composition.method = "uniform_renorm"
    for _ in range(10):
        comp = sample_composition(None, minimal_cfg, rng_bundle.composition)
        assert sum(comp.counts.values()) == minimal_cfg.composition.total_buildings


def test_sample_composition_fixed_count(minimal_cfg, rng_bundle):
    """Fixed-count categories should not be sampled stochastically."""
    from snlg.config import CategoryBounds
    minimal_cfg.composition.categories["commercial"] = CategoryBounds(
        fixed_count=3, source="comstock"
    )
    comp = sample_composition(None, minimal_cfg, rng_bundle.composition)
    assert comp.counts["commercial"] == 3
    assert sum(comp.counts.values()) == minimal_cfg.composition.total_buildings


def test_sample_composition_degenerate_fixed_fractions(minimal_cfg, rng_bundle):
    """When min==max for all variable categories, counts are deterministic."""
    for cat_cfg in minimal_cfg.composition.categories.values():
        cat_cfg.min_fraction = 0.5
        cat_cfg.max_fraction = 0.5
    comp = sample_composition(None, minimal_cfg, rng_bundle.composition)
    assert sum(comp.counts.values()) == minimal_cfg.composition.total_buildings
