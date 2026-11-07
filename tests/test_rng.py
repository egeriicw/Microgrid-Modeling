"""Tests for the layered RNG architecture."""

from microgrid.community_load.synthetic.rng import derive_run_rng, make_rng_bundle


def test_same_seeds_produce_identical_sequences():
    b1 = make_rng_bundle(1, 2, 3)
    b2 = make_rng_bundle(1, 2, 3)
    assert b1.composition.integers(1000) == b2.composition.integers(1000)
    assert b1.archetype.integers(1000) == b2.archetype.integers(1000)
    assert b1.perturbation.integers(1000) == b2.perturbation.integers(1000)


def test_different_seeds_produce_different_sequences():
    b1 = make_rng_bundle(1, 2, 3)
    b2 = make_rng_bundle(4, 5, 6)
    # Overwhelmingly likely to differ
    vals1 = b1.composition.integers(0, 2**31, size=10).tolist()
    vals2 = b2.composition.integers(0, 2**31, size=10).tolist()
    assert vals1 != vals2


def test_layers_are_independent():
    """The composition layer advancing should not affect the archetype layer."""
    b = make_rng_bundle(42, 43, 44)
    # Drain the composition generator
    _ = b.composition.integers(0, 2**31, size=100)
    val_archetype = b.archetype.integers(2**31)

    b2 = make_rng_bundle(42, 43, 44)
    val_archetype2 = b2.archetype.integers(2**31)
    assert val_archetype == val_archetype2


def test_derived_run_bundles_differ_by_index():
    root = make_rng_bundle(1, 2, 3)
    r0 = derive_run_rng(root, 0)
    root2 = make_rng_bundle(1, 2, 3)
    r1 = derive_run_rng(root2, 1)
    assert r0.composition.integers(2**31) != r1.composition.integers(2**31)


def test_derived_run_bundles_reproducible():
    """Same root seeds + same call sequence produces identical per-run bundles."""
    root1 = make_rng_bundle(1, 2, 3)
    root2 = make_rng_bundle(1, 2, 3)

    # Derive 6 bundles from each root in the same order
    vals1, vals2 = [], []
    for i in range(6):
        r1 = derive_run_rng(root1, i)
        r2 = derive_run_rng(root2, i)
        vals1.append(int(r1.archetype.integers(2**31)))
        vals2.append(int(r2.archetype.integers(2**31)))

    assert vals1 == vals2


# --- Story 1.4 / FR-ENG-4: stateless derivation, distinct streams, fresh seeds ---------------

import numpy as np  # noqa: E402

from microgrid.community_load.synthetic.config import RNGConfig  # noqa: E402
from microgrid.community_load.synthetic.rng import resolve_seeds  # noqa: E402


def _first(bundle, layer="composition"):
    return int(getattr(bundle, layer).integers(2**62))


def test_run_streams_do_not_depend_on_call_order():
    a = make_rng_bundle(1, 2, 3)
    b = make_rng_bundle(1, 2, 3)
    for i in (0, 1, 2, 3):  # derive others first on `a`
        derive_run_rng(a, i)
    assert _first(derive_run_rng(a, 5)) == _first(derive_run_rng(b, 5))


def test_deriving_does_not_consume_the_root():
    root = make_rng_bundle(1, 2, 3)
    expected = int(make_rng_bundle(1, 2, 3).composition.integers(2**62))
    derive_run_rng(root, 0)
    derive_run_rng(root, 1)
    assert int(root.composition.integers(2**62)) == expected


def test_ten_thousand_run_indices_give_distinct_streams():
    root = make_rng_bundle(1001, 2002, 3003)
    firsts = {_first(derive_run_rng(root, i)) for i in range(10_000)}
    assert len(firsts) == 10_000


def test_layers_are_independent_even_with_identical_seeds():
    run = derive_run_rng(make_rng_bundle(7, 7, 7), 0)
    comp, arch, pert = (_first(run, layer) for layer in ("composition", "archetype", "perturbation"))
    assert len({comp, arch, pert}) == 3


def test_neighbouring_seeds_do_not_collide_across_runs():
    """The old scheme (draw + run_index) could make (seed A, run i+1) equal (seed B, run i)."""
    seen = set()
    for seed in range(50):
        root = make_rng_bundle(seed, seed, seed)
        for i in range(50):
            seen.add(_first(derive_run_rng(root, i)))
    assert len(seen) == 50 * 50


def test_resolve_seeds_keeps_fixed_and_fills_missing_with_fresh_values():
    fixed = resolve_seeds(RNGConfig(composition_seed=1, archetype_seed=2, perturbation_seed=3))
    assert (fixed.composition_seed, fixed.archetype_seed, fixed.perturbation_seed) == (1, 2, 3)

    a = resolve_seeds(RNGConfig())
    b = resolve_seeds(RNGConfig())
    for cfg in (a, b):
        assert all(isinstance(s, int) for s in (cfg.composition_seed, cfg.archetype_seed, cfg.perturbation_seed))
    assert (a.composition_seed, a.archetype_seed, a.perturbation_seed) != (
        b.composition_seed,
        b.archetype_seed,
        b.perturbation_seed,
    )


def test_resolve_seeds_can_fill_only_some():
    cfg = resolve_seeds(RNGConfig(composition_seed=42))
    assert cfg.composition_seed == 42
    assert cfg.archetype_seed is not None and cfg.perturbation_seed is not None


def test_composition_statistics_do_not_depend_on_which_seeds_are_used():
    """Seed-robustness at the sampling level (full-ensemble version arrives with the data fixtures).

    Two disjoint sets of seeds must give statistically equivalent composition draws.
    """
    from microgrid.community_load.synthetic._types import ArchetypePool
    from microgrid.community_load.synthetic.composition import sample_composition
    from microgrid.community_load.synthetic.config import CategoryBounds, CompositionConfig, ScenarioConfig

    cfg = ScenarioConfig(
        name="robust",
        composition=CompositionConfig(
            total_buildings=40,
            method="dirichlet",
            categories={
                "a": CategoryBounds(min_fraction=0.2, max_fraction=0.8, source="resstock"),
                "b": CategoryBounds(min_fraction=0.2, max_fraction=0.8, source="resstock"),
            },
        ),
    )
    pool = ArchetypePool(residential={}, commercial={}, resstock_chars=None, comstock_chars=None)  # type: ignore[arg-type]

    def mean_a(seeds):
        draws = [
            sample_composition(pool, cfg, derive_run_rng(make_rng_bundle(s, s + 1, s + 2), 0).composition).counts["a"]
            for s in seeds
        ]
        return float(np.mean(draws)), float(np.std(draws, ddof=1)), len(draws)

    m1, s1, n1 = mean_a(range(0, 400))
    m2, s2, n2 = mean_a(range(10_000, 10_400))
    standard_error = np.sqrt(s1**2 / n1 + s2**2 / n2)
    assert abs(m1 - m2) < 4 * standard_error


def test_seed_triple_requires_resolved_seeds():
    import pytest

    from microgrid.community_load.synthetic.rng import seed_triple

    with pytest.raises(ValueError):
        seed_triple(RNGConfig(composition_seed=1))
    assert seed_triple(resolve_seeds(RNGConfig(composition_seed=1)))[0] == 1
