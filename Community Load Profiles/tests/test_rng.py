"""Tests for the layered RNG architecture."""
import numpy as np
import pytest

from snlg.rng import derive_run_rng, make_rng_bundle


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
