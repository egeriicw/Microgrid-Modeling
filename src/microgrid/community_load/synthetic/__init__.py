"""Synthetic Neighborhood Load Generation (SNLG) engine.

Monte-Carlo ensembles of neighborhoods sampled from NREL ResStock/ComStock buildings.
The package version is the single project version defined in ``microgrid.__version__``.
"""
from microgrid import __version__
from microgrid.community_load.synthetic import (
    aggregation,
    archetype,
    composition,
    config,
    ensemble,
    feasibility,
    loader,
    outputs,
    perturbation,
    rng,
    validation,
)

__all__ = [
    "config", "rng", "archetype", "loader", "composition", "aggregation",
    "perturbation", "feasibility", "ensemble", "validation", "outputs",
]
