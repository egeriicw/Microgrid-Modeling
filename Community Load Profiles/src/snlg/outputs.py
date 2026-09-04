"""Output generation — Section 9 of the methodology.

Writes all CSVs in a format backward-compatible with the existing notebook
output structure, plus new ensemble-level files (percentile envelopes,
diversity metrics, representative cases, validation report).
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from snlg._types import EnsembleResult, NeighborhoodResult
from snlg.config import ScenarioConfig


# ---------------------------------------------------------------------------
# Per-run outputs (backward-compatible with existing notebook format)
# ---------------------------------------------------------------------------

def write_run_outputs(
    result: NeighborhoodResult,
    run_dir: Path,
    timestamp_str: str,
) -> None:
    """Write per-run CSVs matching the existing output structure exactly."""
    run_label = f"Run-{result.run_index + 1}"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Residential profile
    res = result.residential_profile.copy()
    res.rename(f"Resstock_Run {result.run_index + 1}", inplace=True)
    res.to_csv(run_dir / f"{timestamp_str}_{run_label}_residential_merged_community_load_profile.csv")

    # Commercial profile
    com = result.commercial_profile.copy()
    com.rename(f"Comstock_Run {result.run_index + 1}", inplace=True)
    com.to_csv(run_dir / f"{timestamp_str}_{run_label}_comstock_merged_community_load_profile.csv")

    # Total profile
    total = result.total_profile.copy()
    total.rename(f"Total_Run {result.run_index + 1}", inplace=True)
    total.to_csv(run_dir / f"{timestamp_str}_{run_label}_total_merged_community_load_profile.csv")

    # Building selections
    rows = []
    for sel in result.selections:
        rows.append({
            "run": run_label,
            "category": sel.category,
            "source": sel.source,
            "bldg_id": sel.bldg_id,
            "unit_count_multiplier": sel.unit_multiplier,
        })
    pd.DataFrame(rows).to_csv(
        run_dir / f"{timestamp_str}_{run_label}_building_selections.csv",
        index=False,
    )


# ---------------------------------------------------------------------------
# Ensemble-level outputs (new)
# ---------------------------------------------------------------------------

def write_compiled_runs(
    ensemble: EnsembleResult,
    out_dir: Path,
    timestamp_str: str,
) -> None:
    """Write the compiled runs CSV (all residential + commercial columns)."""
    frames = {}
    for r in ensemble.runs:
        frames[f"Resstock Run {r.run_index + 1}"] = r.residential_profile
        frames[f"Comstock Run {r.run_index + 1}"] = r.commercial_profile

    if frames:
        compiled = pd.concat(frames, axis=1)
        compiled.index.name = "timestamp"
        compiled.to_csv(
            out_dir / f"{timestamp_str}_merged_community_load_profile_total_compiled-runs.csv"
        )


def write_load_profile_total(
    ensemble: EnsembleResult,
    out_dir: Path,
    timestamp_str: str,
) -> None:
    """Write average Residential / Commercial / Total columns (legacy format)."""
    if not ensemble.runs:
        return

    res_cols = pd.concat([r.residential_profile for r in ensemble.runs], axis=1)
    com_cols = pd.concat([r.commercial_profile for r in ensemble.runs], axis=1)

    total = pd.DataFrame({
        "Residential": res_cols.mean(axis=1),
        "Commercial": com_cols.mean(axis=1),
    })
    total["Total"] = total["Residential"] + total["Commercial"]
    total.index.name = "timestamp"
    total.to_csv(out_dir / f"{timestamp_str}_load_profile_total.csv")


def write_percentile_envelopes(
    ensemble: EnsembleResult,
    out_dir: Path,
    timestamp_str: str,
) -> None:
    """Write P10/P50/P90 (or configured percentiles) hourly envelope CSV."""
    if not ensemble.percentile_envelopes:
        return
    env = pd.DataFrame(ensemble.percentile_envelopes)
    env.index.name = "timestamp"
    env.to_csv(out_dir / f"{timestamp_str}_percentile_envelopes.csv")


def write_ensemble_statistics(
    ensemble: EnsembleResult,
    out_dir: Path,
    timestamp_str: str,
) -> None:
    """Write per-run scalar statistics (peak, energy, load factor, score)."""
    rows = [
        {
            "run_index": r.run_index,
            "run_label": f"Run-{r.run_index + 1}",
            "peak_kw": r.peak_kw,
            "annual_energy_kwh": r.annual_energy_kwh,
            "load_factor": r.load_factor,
            "soft_score": r.soft_score,
        }
        for r in ensemble.runs
    ]
    pd.DataFrame(rows).to_csv(
        out_dir / f"{timestamp_str}_ensemble_statistics.csv", index=False
    )

    # Summary diversity metrics
    if ensemble.diversity_metrics:
        pd.DataFrame([ensemble.diversity_metrics]).to_csv(
            out_dir / f"{timestamp_str}_diversity_metrics.csv", index=False
        )


def write_convergence_history(
    ensemble: EnsembleResult,
    out_dir: Path,
    timestamp_str: str,
) -> None:
    """Write rolling metric means used for convergence tracking."""
    if not ensemble.convergence_history:
        return
    pd.DataFrame(ensemble.convergence_history).to_csv(
        out_dir / f"{timestamp_str}_convergence_history.csv", index=False
    )


def write_representative_cases(
    ensemble: EnsembleResult,
    out_dir: Path,
    timestamp_str: str,
) -> None:
    """Write total profiles for median, high-peak, and low-load-factor runs."""
    for case_name, result in ensemble.representative_cases.items():
        profile = result.total_profile.copy()
        profile.name = case_name
        profile.to_csv(
            out_dir / f"{timestamp_str}_representative_{case_name}.csv"
        )

    # Summary table
    if ensemble.representative_cases:
        rows = [
            {
                "case": name,
                "run_index": r.run_index,
                "peak_kw": r.peak_kw,
                "annual_energy_kwh": r.annual_energy_kwh,
                "load_factor": r.load_factor,
            }
            for name, r in ensemble.representative_cases.items()
        ]
        pd.DataFrame(rows).to_csv(
            out_dir / f"{timestamp_str}_representative_cases_summary.csv", index=False
        )


def write_overview_txt(
    cfg: ScenarioConfig,
    ensemble: EnsembleResult,
    out_dir: Path,
    timestamp_str: str,
    elapsed_minutes: float = 0.0,
) -> None:
    """Write a human-readable overview file."""
    lines = [
        f"Scenario: {cfg.name}",
        f"Time taken: {elapsed_minutes:.2f} minutes",
        f"Runs attempted: {ensemble.n_attempted}",
        f"Runs feasible: {len(ensemble.runs)}",
        f"Runs rejected: {ensemble.n_rejected}",
        f"Converged: {ensemble.converged}",
        f"Mode: {cfg.ensemble.mode}",
        "",
        f"TOTAL_BUILDINGS: {cfg.composition.total_buildings}",
        f"Composition method: {cfg.composition.method}",
        f"Perturbations enabled: {cfg.perturbation.enabled}",
        f"RANDOM_SEED (composition): {cfg.rng.composition_seed}",
        f"RANDOM_SEED (archetype): {cfg.rng.archetype_seed}",
        f"RANDOM_SEED (perturbation): {cfg.rng.perturbation_seed}",
        "",
    ]
    if ensemble.diversity_metrics:
        lines.append("Diversity metrics:")
        for k, v in ensemble.diversity_metrics.items():
            lines.append(f"  {k}: {v:.4f}")

    with open(out_dir / f"{timestamp_str}-overview.txt", "w") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------------
# Top-level writer
# ---------------------------------------------------------------------------

def save_all_outputs(
    ensemble: EnsembleResult,
    cfg: ScenarioConfig,
    timestamp_str: str,
    elapsed_minutes: float = 0.0,
) -> Path:
    """Write all outputs for an ensemble run.

    Returns the path to the timestamped output directory.
    """
    base = Path(cfg.output.base_dir)
    out_dir = base / timestamp_str
    out_dir.mkdir(parents=True, exist_ok=True)

    if cfg.output.per_run_csvs:
        for result in ensemble.runs:
            run_dir = out_dir / f"Run-{result.run_index + 1}"
            write_run_outputs(result, run_dir, timestamp_str)

    write_compiled_runs(ensemble, out_dir, timestamp_str)
    write_load_profile_total(ensemble, out_dir, timestamp_str)
    write_percentile_envelopes(ensemble, out_dir, timestamp_str)
    write_ensemble_statistics(ensemble, out_dir, timestamp_str)
    write_convergence_history(ensemble, out_dir, timestamp_str)

    if cfg.output.representative_cases:
        write_representative_cases(ensemble, out_dir, timestamp_str)

    write_overview_txt(cfg, ensemble, out_dir, timestamp_str, elapsed_minutes)

    return out_dir
