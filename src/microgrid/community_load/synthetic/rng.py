"""Layered RNG architecture (Section 10 of the methodology).

Three independent numpy Generator streams, one per stochastic layer:
  Layer 1 (composition)  - category mix, building counts
  Layer 2 (archetype)    - building instance selection
  Layer 3 (perturbation) - schedule shifts, end-use variability

Every stream is a pure function of (seed, layer, run index) built with
``numpy.random.SeedSequence``. Deriving a run's streams therefore:
  * does not depend on the order or number of earlier derivations,
  * never consumes the root generators,
  * cannot collide across neighbouring seeds or run indices (unlike seed + index arithmetic),
  * gives independent layers even when two seeds are equal.

Seeds are random by default (``resolve_seeds``) and are always recorded with a run, so any run can
be replayed exactly by supplying the recorded seeds.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass, replace

import numpy as np

from microgrid.community_load.synthetic.config import RNGConfig

# Fixed layer identifiers: part of the stream definition; never renumber (changes all results).
_LAYER_COMPOSITION, _LAYER_ARCHETYPE, _LAYER_PERTURBATION = 0, 1, 2


@dataclass
class RNGBundle:
    """Three independent numpy random Generators plus the seeds they came from."""

    composition: np.random.Generator
    archetype: np.random.Generator
    perturbation: np.random.Generator
    seeds: tuple[int, int, int] | None = None


def _stream(seed: int, layer: int, run_index: int | None = None) -> np.random.Generator:
    key = (layer,) if run_index is None else (layer, run_index)
    return np.random.default_rng(np.random.SeedSequence(entropy=seed, spawn_key=key))


def make_rng_bundle(composition_seed: int, archetype_seed: int, perturbation_seed: int) -> RNGBundle:
    """Create the root RNG bundle from three seeds."""
    return RNGBundle(
        composition=_stream(composition_seed, _LAYER_COMPOSITION),
        archetype=_stream(archetype_seed, _LAYER_ARCHETYPE),
        perturbation=_stream(perturbation_seed, _LAYER_PERTURBATION),
        seeds=(composition_seed, archetype_seed, perturbation_seed),
    )


def derive_run_rng(root: RNGBundle, run_index: int) -> RNGBundle:
    """Streams for one run: a stateless, reproducible function of the root seeds and ``run_index``."""
    if root.seeds is None:
        raise ValueError("root bundle has no recorded seeds; create it with make_rng_bundle()")
    comp, arch, pert = root.seeds
    return RNGBundle(
        composition=_stream(comp, _LAYER_COMPOSITION, run_index),
        archetype=_stream(arch, _LAYER_ARCHETYPE, run_index),
        perturbation=_stream(pert, _LAYER_PERTURBATION, run_index),
        seeds=root.seeds,
    )


def resolve_seeds(cfg: RNGConfig) -> RNGConfig:
    """Return ``cfg`` with every unset seed replaced by a fresh random one (fixed seeds are kept)."""

    def pick(value: int | None) -> int:
        return value if value is not None else secrets.randbits(32)

    return replace(
        cfg,
        composition_seed=pick(cfg.composition_seed),
        archetype_seed=pick(cfg.archetype_seed),
        perturbation_seed=pick(cfg.perturbation_seed),
    )


def seed_triple(cfg: RNGConfig) -> tuple[int, int, int]:
    """The three seeds of a *resolved* config; raises if any is still unset."""
    triple = (cfg.composition_seed, cfg.archetype_seed, cfg.perturbation_seed)
    if any(s is None for s in triple):
        raise ValueError("seeds are unresolved; call resolve_seeds() first")
    return triple  # type: ignore[return-value]
