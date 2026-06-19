"""Configuration loading and validation (YAML → typed dataclasses).

All scenario parameters live in a YAML file. This module converts that
file into a ScenarioConfig dataclass that every other module receives.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml


# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------

@dataclass
class RNGConfig:
    composition_seed: int = 1001
    archetype_seed: int = 2002
    perturbation_seed: int = 3003


@dataclass
class CategoryBounds:
    min_fraction: float = 0.0
    max_fraction: float = 1.0
    fixed_count: int | None = None   # overrides fraction-based sampling if set
    source: str = "resstock"         # "resstock" or "comstock"


@dataclass
class CompositionConfig:
    total_buildings: int = 50
    method: Literal["dirichlet", "uniform_renorm"] = "dirichlet"
    dirichlet_concentration: float = 5.0
    categories: dict[str, CategoryBounds] = field(default_factory=dict)


@dataclass
class ResidentialFilterConfig:
    building_type_heights: list[str] = field(default_factory=list)
    hvac_types: list[str] | None = None
    hvac_types_exclude: list[str] | None = None
    max_sqft: float | None = None
    require_no_garage: bool = False
    require_data_complete: bool = True


@dataclass
class CommercialFilterConfig:
    building_types: list[str] = field(default_factory=list)
    include_public: bool = False


@dataclass
class ArchetypeFilterConfig:
    resstock: dict[str, ResidentialFilterConfig] = field(default_factory=dict)
    comstock: dict[str, CommercialFilterConfig] = field(default_factory=dict)


@dataclass
class HardConstraints:
    peak_kw_min: float = 0.0
    peak_kw_max: float = 1e9
    annual_energy_kwh_min: float = 0.0
    annual_energy_kwh_max: float = 1e12
    load_factor_min: float = 0.0
    load_factor_max: float = 1.0


@dataclass
class SoftConstraintBound:
    min: float | None = None
    max: float | None = None


@dataclass
class SoftConstraints:
    weights: dict[str, float] = field(default_factory=dict)
    bounds: dict[str, SoftConstraintBound] = field(default_factory=dict)


@dataclass
class FeasibilityConfig:
    hard: HardConstraints = field(default_factory=HardConstraints)
    soft: SoftConstraints = field(default_factory=SoftConstraints)


@dataclass
class EndUseFactorConfig:
    sigma: float = 0.0


@dataclass
class PerturbationConfig:
    enabled: bool = False
    max_temporal_shift_hours: int = 2
    energy_tolerance: float = 0.05
    end_use_factors: dict[str, EndUseFactorConfig] = field(default_factory=dict)


@dataclass
class ConvergenceConfig:
    lookback_window: int = 10
    min_stable_window: int = 10
    epsilon: float = 0.01
    metrics: list[str] = field(default_factory=lambda: [
        "peak_kw", "annual_energy_kwh", "load_factor"
    ])


@dataclass
class EnsembleConfig:
    mode: Literal["fixed", "adaptive"] = "fixed"
    min_runs: int = 20
    max_runs: int = 50
    check_every: int = 10


@dataclass
class ValidationConfig:
    benchmarks: dict[str, list[float]] = field(default_factory=dict)
    min_cv_peak: float = 0.01


@dataclass
class OutputConfig:
    base_dir: str = "../data/output/scenario_runs"
    per_run_csvs: bool = True
    percentiles: list[float] = field(default_factory=lambda: [0.10, 0.50, 0.90])
    representative_cases: bool = True
    validation_report: bool = True
    preserve_legacy_format: bool = True


@dataclass
class DataConfig:
    upgrade: str = "0"
    state: str = "DC"
    resstock_chars_file: str = "DC_upgrade0.xlsx"
    comstock_chars_file: str = "DC_upgrade0_agg.xlsx"
    resstock_timeseries_dir: str = "../data/input/resstock/timeseries_individual_buildings"
    comstock_timeseries_dir: str = "../data/input/comstock/timeseries_individual_buildings"
    background_dir: str = "../data/background"


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------

@dataclass
class ScenarioConfig:
    name: str = "unnamed"
    description: str = ""
    rng: RNGConfig = field(default_factory=RNGConfig)
    data: DataConfig = field(default_factory=DataConfig)
    composition: CompositionConfig = field(default_factory=CompositionConfig)
    archetype_filters: ArchetypeFilterConfig = field(default_factory=ArchetypeFilterConfig)
    perturbation: PerturbationConfig = field(default_factory=PerturbationConfig)
    feasibility: FeasibilityConfig = field(default_factory=FeasibilityConfig)
    ensemble: EnsembleConfig = field(default_factory=EnsembleConfig)
    convergence: ConvergenceConfig = field(default_factory=ConvergenceConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    output: OutputConfig = field(default_factory=OutputConfig)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base (override wins on conflicts)."""
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def _parse_rng(d: dict) -> RNGConfig:
    return RNGConfig(**d)


def _parse_data(d: dict) -> DataConfig:
    return DataConfig(**{k: str(v) for k, v in d.items()})


def _parse_category_bounds(d: dict) -> CategoryBounds:
    return CategoryBounds(
        min_fraction=float(d.get("min_fraction", 0.0)),
        max_fraction=float(d.get("max_fraction", 1.0)),
        fixed_count=d.get("fixed_count"),
        source=d.get("source", "resstock"),
    )


def _parse_composition(d: dict) -> CompositionConfig:
    cats = {
        name: _parse_category_bounds(bounds)
        for name, bounds in d.get("categories", {}).items()
    }
    return CompositionConfig(
        total_buildings=int(d.get("total_buildings", 50)),
        method=d.get("method", "dirichlet"),
        dirichlet_concentration=float(d.get("dirichlet_concentration", 5.0)),
        categories=cats,
    )


def _parse_res_filter(d: dict) -> ResidentialFilterConfig:
    return ResidentialFilterConfig(
        building_type_heights=d.get("building_type_heights", []),
        hvac_types=d.get("hvac_types"),
        hvac_types_exclude=d.get("hvac_types_exclude"),
        max_sqft=d.get("max_sqft"),
        require_no_garage=bool(d.get("require_no_garage", False)),
        require_data_complete=bool(d.get("require_data_complete", True)),
    )


def _parse_com_filter(d: dict) -> CommercialFilterConfig:
    return CommercialFilterConfig(
        building_types=d.get("building_types", []),
        include_public=bool(d.get("include_public", False)),
    )


def _parse_archetype_filters(d: dict) -> ArchetypeFilterConfig:
    res = {k: _parse_res_filter(v) for k, v in d.get("resstock", {}).items()}
    com = {k: _parse_com_filter(v) for k, v in d.get("comstock", {}).items()}
    return ArchetypeFilterConfig(resstock=res, comstock=com)


def _parse_feasibility(d: dict) -> FeasibilityConfig:
    hard_raw = d.get("hard", {})
    hard = HardConstraints(
        peak_kw_min=float(hard_raw.get("peak_kw", {}).get("min", 0.0)),
        peak_kw_max=float(hard_raw.get("peak_kw", {}).get("max", 1e9)),
        annual_energy_kwh_min=float(hard_raw.get("annual_energy_kwh", {}).get("min", 0.0)),
        annual_energy_kwh_max=float(hard_raw.get("annual_energy_kwh", {}).get("max", 1e12)),
        load_factor_min=float(hard_raw.get("load_factor", {}).get("min", 0.0)),
        load_factor_max=float(hard_raw.get("load_factor", {}).get("max", 1.0)),
    )
    soft_raw = d.get("soft", {})
    soft = SoftConstraints(
        weights=soft_raw.get("weights", {}),
        bounds={
            k: SoftConstraintBound(min=v.get("min"), max=v.get("max"))
            for k, v in soft_raw.get("bounds", {}).items()
        },
    )
    return FeasibilityConfig(hard=hard, soft=soft)


def _parse_perturbation(d: dict) -> PerturbationConfig:
    factors = {
        k: EndUseFactorConfig(sigma=float(v.get("sigma", 0.0)))
        for k, v in d.get("end_use_factors", {}).items()
    }
    return PerturbationConfig(
        enabled=bool(d.get("enabled", False)),
        max_temporal_shift_hours=int(d.get("max_temporal_shift_hours", 2)),
        energy_tolerance=float(d.get("energy_tolerance", 0.05)),
        end_use_factors=factors,
    )


def _parse_convergence(d: dict) -> ConvergenceConfig:
    return ConvergenceConfig(
        lookback_window=int(d.get("lookback_window", 10)),
        min_stable_window=int(d.get("min_stable_window", 10)),
        epsilon=float(d.get("epsilon", 0.01)),
        metrics=d.get("metrics", ["peak_kw", "annual_energy_kwh", "load_factor"]),
    )


def _parse_ensemble(d: dict) -> EnsembleConfig:
    return EnsembleConfig(
        mode=d.get("mode", "fixed"),
        min_runs=int(d.get("min_runs", 20)),
        max_runs=int(d.get("max_runs", 50)),
        check_every=int(d.get("check_every", 10)),
    )


def _parse_validation(d: dict) -> ValidationConfig:
    return ValidationConfig(
        benchmarks=d.get("benchmarks", {}),
        min_cv_peak=float(d.get("min_cv_peak", 0.01)),
    )


def _parse_output(d: dict) -> OutputConfig:
    return OutputConfig(
        base_dir=str(d.get("base_dir", "../data/output/scenario_runs")),
        per_run_csvs=bool(d.get("per_run_csvs", True)),
        percentiles=[float(p) for p in d.get("percentiles", [0.10, 0.50, 0.90])],
        representative_cases=bool(d.get("representative_cases", True)),
        validation_report=bool(d.get("validation_report", True)),
        preserve_legacy_format=bool(d.get("preserve_legacy_format", True)),
    )


def load_config(path: Path | str) -> ScenarioConfig:
    """Load a YAML scenario config file and return a typed ScenarioConfig."""
    path = Path(path)
    with path.open() as f:
        raw = yaml.safe_load(f) or {}

    cfg = ScenarioConfig(
        name=raw.get("scenario", {}).get("name", path.stem),
        description=raw.get("scenario", {}).get("description", ""),
    )
    if "rng" in raw:
        cfg.rng = _parse_rng(raw["rng"])
    if "data" in raw:
        cfg.data = _parse_data(raw["data"])
    if "composition" in raw:
        cfg.composition = _parse_composition(raw["composition"])
    if "archetype_filters" in raw:
        cfg.archetype_filters = _parse_archetype_filters(raw["archetype_filters"])
    if "perturbation" in raw:
        cfg.perturbation = _parse_perturbation(raw["perturbation"])
    if "feasibility" in raw:
        cfg.feasibility = _parse_feasibility(raw["feasibility"])
    if "ensemble" in raw:
        cfg.ensemble = _parse_ensemble(raw["ensemble"])
    if "convergence" in raw:
        cfg.convergence = _parse_convergence(raw["convergence"])
    if "validation" in raw:
        cfg.validation = _parse_validation(raw["validation"])
    if "output" in raw:
        cfg.output = _parse_output(raw["output"])

    validate_config(cfg)
    return cfg


def validate_config(cfg: ScenarioConfig) -> None:
    """Raise ValueError if any config invariants are violated."""
    errors = []

    # Composition bounds feasibility
    cats = cfg.composition.categories
    if cats:
        p_min_sum = sum(c.min_fraction for c in cats.values() if c.fixed_count is None)
        p_max_sum = sum(c.max_fraction for c in cats.values() if c.fixed_count is None)
        if p_min_sum > 1.0 + 1e-9:
            errors.append(
                f"Sum of min_fraction values ({p_min_sum:.4f}) exceeds 1.0 — "
                "no feasible composition exists."
            )
        if p_max_sum < 1.0 - 1e-9:
            errors.append(
                f"Sum of max_fraction values ({p_max_sum:.4f}) is below 1.0 — "
                "no feasible composition exists."
            )

    if errors:
        raise ValueError("ScenarioConfig validation failed:\n" + "\n".join(f"  - {e}" for e in errors))
