"""Layered RNG architecture (Section 10 of the methodology).

Three independent numpy Generator streams, one per stochastic layer:
  Layer 1 (composition)  — category mix, building counts
  Layer 2 (archetype)    — building instance selection
  Layer 3 (perturbation) — schedule shifts, end-use variability

Each layer has its own seed so they never interfere. Every run index
forks a fresh sub-bundle from the root, ensuring full reproducibility
from the three root seeds.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class RNGBundle:
    """Three independent numpy random Generators."""
    composition: np.random.Generator
    archetype: np.random.Generator
    perturbation: np.random.Generator


def make_rng_bundle(
    composition_seed: int,
    archetype_seed: int,
    perturbation_seed: int,
) -> RNGBundle:
    """Create root RNG bundle from three independent seeds."""
    return RNGBundle(
        composition=np.random.default_rng(composition_seed),
        archetype=np.random.default_rng(archetype_seed),
        perturbation=np.random.default_rng(perturbation_seed),
    )


def derive_run_rng(root: RNGBundle, run_index: int) -> RNGBundle:
    """Fork a per-run bundle from the root.

    Each run_index produces a unique, reproducible sub-bundle.
    The root generators advance deterministically so the overall
    sequence across runs is also reproducible.
    """
    return RNGBundle(
        composition=np.random.default_rng(
            int(root.composition.integers(2**31)) + run_index
        ),
        archetype=np.random.default_rng(
            int(root.archetype.integers(2**31)) + run_index
        ),
        perturbation=np.random.default_rng(
            int(root.perturbation.integers(2**31)) + run_index
        ),
    )
