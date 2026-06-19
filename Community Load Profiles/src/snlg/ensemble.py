"""Ensemble generation and convergence — Sections 7–8 of the methodology.

The main loop:
  for each run:
    1. sample composition  (Layer 1 RNG)
    2. select buildings    (Layer 2 RNG)
    3. load & perturb profiles (loader + perturbation + Layer 3 RNG)
    4. aggregate to neighborhood profile
    5. evaluate feasibility (hard + soft)
    6. append to ensemble

Convergence check (adaptive mode):
  |μ^K - μ^{K-M}| < ε for each tracked metric
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from snlg import aggregation, feasibility, perturbation
from snlg._types import (
    ArchetypePool,
    EnsembleResult,
    NeighborhoodResult,
    PerturbedProfile,
)
from snlg.composition import sample_buildings, sample_composition
from snlg.config import ConvergenceConfig, ScenarioConfig
from snlg.loader import load_and_scale_profile
from snlg.rng import RNGBundle, derive_run_rng, make_rng_bundle

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Single-run helper
# ---------------------------------------------------------------------------

def _run_single(
    run_index: int,
    pool: ArchetypePool,
    cfg: ScenarioConfig,
    run_rng: RNGBundle,
    data_dirs: dict[str, Path],
) -> NeighborhoodResult:
    """Execute one complete Monte Carlo run and return a NeighborhoodResult."""

    # --- Layer 1: sample composition ---
    comp = sample_composition(pool, cfg, run_rng.composition)

    # --- Layer 2: select building instances ---
    selections = sample_buildings(
        comp, pool, cfg, run_rng.archetype,
        resstock_chars=pool.resstock_chars,
    )

    # --- Load profiles + Layer 3 perturbations ---
    profiles: list[PerturbedProfile] = []
    perturbation_logs: list[dict] = []

    for sel in selections:
        # Resolve building type for MF unit scaling
        if sel.source == "resstock":
            btype_match = pool.resstock_chars.loc[
                pool.resstock_chars["bldg_id"] == sel.bldg_id,
                "in.geometry_building_type_height",
            ].values
            building_type = btype_match[0] if len(btype_match) > 0 else ""
        else:
            building_type = ""

        loaded = load_and_scale_profile(
            sel, data_dirs, cfg,
            resstock_chars=pool.resstock_chars,
            building_type=building_type,
        )

        perturbed = perturbation.perturb_building(
            loaded, cfg.perturbation, run_rng.perturbation
        )
        profiles.append(perturbed)
        perturbation_logs.append(perturbed.perturbation_log)

    # --- Aggregate (Section 5) ---
    res_profiles = [p for p in profiles if p.source == "resstock"]
    com_profiles = [p for p in profiles if p.source == "comstock"]

    residential_hourly = aggregation.aggregate_profiles(res_profiles)
    commercial_hourly = aggregation.aggregate_profiles(com_profiles)

    if residential_hourly.empty and commercial_hourly.empty:
        total_hourly = pd.Series(dtype=float)
    elif residential_hourly.empty:
        total_hourly = commercial_hourly
    elif commercial_hourly.empty:
        total_hourly = residential_hourly
    else:
        total_hourly = residential_hourly.add(commercial_hourly, fill_value=0.0)

    metrics = aggregation.compute_metrics(total_hourly)

    # --- Feasibility evaluation (Section 6) ---
    feasible, violations = feasibility.passes_hard_constraints(
        metrics["peak_kw"],
        metrics["annual_energy_kwh"],
        metrics["load_factor"],
        cfg.feasibility,
    )
    soft_score = feasibility.compute_soft_score(
        metrics["peak_kw"],
        metrics["annual_energy_kwh"],
        metrics["load_factor"],
        cfg.feasibility,
    )

    if violations:
        logger.debug("Run %d rejected: %s", run_index, "; ".join(violations))

    return NeighborhoodResult(
        run_index=run_index,
        residential_profile=residential_hourly,
        commercial_profile=commercial_hourly,
        total_profile=total_hourly,
        composition=comp,
        selections=selections,
        peak_kw=metrics["peak_kw"],
        annual_energy_kwh=metrics["annual_energy_kwh"],
        load_factor=metrics["load_factor"],
        feasible=feasible,
        soft_score=soft_score,
        perturbation_logs=perturbation_logs,
    )


# ---------------------------------------------------------------------------
# Convergence check
# ---------------------------------------------------------------------------

def _check_convergence(
    feasible_runs: list[NeighborhoodResult],
    conv_cfg: ConvergenceConfig,
) -> bool:
    """Return True when ensemble statistics have stabilized.

    Criterion: |μ^K - μ^{K-M}| < ε for all tracked metrics,
    evaluated over the last M vs. M before that.
    """
    K = len(feasible_runs)
    M = conv_cfg.lookback_window
    min_needed = M + conv_cfg.min_stable_window

    if K < min_needed:
        return False

    def get_values(attr: str) -> list[float]:
        return [getattr(r, attr) for r in feasible_runs]

    import numpy as np

    for metric in conv_cfg.metrics:
        vals = get_values(metric)
        mu_recent = float(np.mean(vals[-M:]))
        mu_prev = float(np.mean(vals[-2 * M:-M]))
        # Relative change (guard against zero)
        denom = abs(mu_prev) if abs(mu_prev) > 1e-12 else 1.0
        if abs(mu_recent - mu_prev) / denom >= conv_cfg.epsilon:
            return False

    return True


# ---------------------------------------------------------------------------
# Ensemble result construction
# ---------------------------------------------------------------------------

def _build_ensemble_result(
    all_runs: list[NeighborhoodResult],
    converged: bool,
    n_attempted: int,
    cfg: ScenarioConfig,
) -> EnsembleResult:
    """Compute derived ensemble statistics from raw runs."""
    import numpy as np

    feasible = [r for r in all_runs if r.feasible]
    n_rejected = n_attempted - len(feasible)

    # Convergence history: track metric means after each feasible run
    conv_history: dict[str, list[float]] = {m: [] for m in cfg.convergence.metrics}
    for i, _ in enumerate(feasible, start=1):
        for metric in cfg.convergence.metrics:
            vals = [getattr(r, metric) for r in feasible[:i]]
            conv_history[metric].append(float(np.mean(vals)))

    # Percentile envelopes
    percentile_envelopes: dict[str, pd.Series] = {}
    if feasible and not feasible[0].total_profile.empty:
        stacked = pd.concat(
            [r.total_profile for r in feasible], axis=1
        )
        for p in cfg.output.percentiles:
            label = f"p{int(p * 100):02d}"
            percentile_envelopes[label] = stacked.quantile(p, axis=1)

    # Diversity metrics
    diversity_metrics: dict[str, float] = {}
    if feasible:
        peaks = np.array([r.peak_kw for r in feasible])
        energies = np.array([r.annual_energy_kwh for r in feasible])
        lfs = np.array([r.load_factor for r in feasible])
        diversity_metrics = {
            "mean_peak_kw": float(peaks.mean()),
            "std_peak_kw": float(peaks.std()),
            "cv_peak": float(peaks.std() / peaks.mean()) if peaks.mean() > 0 else float("nan"),
            "mean_annual_energy_kwh": float(energies.mean()),
            "mean_load_factor": float(lfs.mean()),
        }

    # Representative cases
    representative_cases: dict[str, NeighborhoodResult] = {}
    if feasible:
        peaks_list = [r.peak_kw for r in feasible]
        median_peak = float(np.median(peaks_list))
        representative_cases["median"] = min(
            feasible, key=lambda r: abs(r.peak_kw - median_peak)
        )
        representative_cases["high_peak"] = max(feasible, key=lambda r: r.peak_kw)
        representative_cases["low_load_factor"] = min(feasible, key=lambda r: r.load_factor)

    return EnsembleResult(
        runs=feasible,
        converged=converged,
        n_attempted=n_attempted,
        n_rejected=n_rejected,
        convergence_history=conv_history,
        percentile_envelopes=percentile_envelopes,
        diversity_metrics=diversity_metrics,
        representative_cases=representative_cases,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_ensemble(
    pool: ArchetypePool,
    cfg: ScenarioConfig,
    data_dirs: dict[str, Path],
) -> EnsembleResult:
    """Run the full ensemble generation loop.

    Mode 'fixed'   : run exactly cfg.ensemble.max_runs regardless of convergence.
    Mode 'adaptive': run until convergence or cfg.ensemble.max_runs is reached.

    Returns an EnsembleResult containing all feasible runs, convergence
    history, percentile envelopes, and representative cases.
    """
    root_rng = make_rng_bundle(
        cfg.rng.composition_seed,
        cfg.rng.archetype_seed,
        cfg.rng.perturbation_seed,
    )

    ens_cfg = cfg.ensemble
    all_runs: list[NeighborhoodResult] = []
    feasible_runs: list[NeighborhoodResult] = []
    converged = False
    n_attempted = 0

    for i in range(ens_cfg.max_runs):
        run_rng = derive_run_rng(root_rng, i)
        result = _run_single(i, pool, cfg, run_rng, data_dirs)
        all_runs.append(result)
        n_attempted += 1

        if result.feasible:
            feasible_runs.append(result)

        if ens_cfg.mode == "adaptive":
            n_feasible = len(feasible_runs)
            if (
                n_feasible >= ens_cfg.min_runs
                and n_feasible % ens_cfg.check_every == 0
            ):
                if _check_convergence(feasible_runs, cfg.convergence):
                    converged = True
                    logger.info(
                        "Ensemble converged after %d attempts (%d feasible).",
                        n_attempted, n_feasible,
                    )
                    break

    if not converged and ens_cfg.mode == "fixed":
        converged = True  # fixed mode always "completes"

    return _build_ensemble_result(all_runs, converged, n_attempted, cfg)
