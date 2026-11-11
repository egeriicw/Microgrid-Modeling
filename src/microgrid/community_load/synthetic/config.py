"""Configuration loading and validation (YAML → typed dataclasses).

All scenario parameters live in a YAML file. This module converts that
file into a ScenarioConfig dataclass that every other module receives.
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Literal

import yaml

from microgrid.community_load.synthetic.attribute_filters import (
    AttributeFilter,
    check_attribute_filter_raw,
    parse_attribute_filters,
)

# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------


@dataclass
class RNGConfig:
    composition_seed: int | None = None  # None = draw a fresh random seed (recorded with the run)
    archetype_seed: int | None = None
    perturbation_seed: int | None = None


@dataclass
class CategoryBounds:
    min_fraction: float = 0.0
    max_fraction: float = 1.0
    fixed_count: int | None = None  # overrides fraction-based sampling if set
    source: str = "resstock"  # "resstock" or "comstock"


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
    require_data_complete: bool = True  # keep only buildings whose simulation completed and applies (see archetype.py)
    attributes: list[AttributeFilter] = field(default_factory=list)  # FR-ENG-11; AND-combined, empty = no narrowing


@dataclass
class CommercialFilterConfig:
    building_types: list[str] = field(default_factory=list)
    include_public: bool = False
    attributes: list[AttributeFilter] = field(default_factory=list)  # FR-ENG-11; AND-combined, empty = no narrowing


@dataclass
class ArchetypeFilterConfig:
    resstock: dict[str, ResidentialFilterConfig] = field(default_factory=dict)
    comstock: dict[str, CommercialFilterConfig] = field(default_factory=dict)
    min_pool_size: int = 10  # pool preview warns when a category matches fewer buildings than this (FR-ENG-11 c)


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
    metrics: list[str] = field(default_factory=lambda: ["peak_kw", "annual_energy_kwh", "load_factor"])


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
    # Dataset ids as plain strings, "product:release:weather:scenario:state" (FR-DATA-8). The engine only carries
    # them; community_load.resolve_dataset_paths turns them into the path fields above and records the load basis.
    dataset_resstock: str | None = None
    dataset_comstock: str | None = None
    resstock_load_basis: str | None = None  # "gross" or "net"; set by the resolver, recorded with the run
    comstock_load_basis: str | None = None


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
    return DataConfig(**{k: (None if v is None else str(v)) for k, v in d.items()})


def _parse_category_bounds(d: dict) -> CategoryBounds:
    return CategoryBounds(
        min_fraction=float(d.get("min_fraction", 0.0)),
        max_fraction=float(d.get("max_fraction", 1.0)),
        fixed_count=d.get("fixed_count"),
        source=d.get("source", "resstock"),
    )


def _parse_composition(d: dict) -> CompositionConfig:
    cats = {name: _parse_category_bounds(bounds) for name, bounds in d.get("categories", {}).items()}
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
        attributes=parse_attribute_filters(d.get("attributes"), "attributes"),
    )


def _parse_com_filter(d: dict) -> CommercialFilterConfig:
    return CommercialFilterConfig(
        building_types=d.get("building_types", []),
        include_public=bool(d.get("include_public", False)),
        attributes=parse_attribute_filters(d.get("attributes"), "attributes"),
    )


def _parse_archetype_filters(d: dict) -> ArchetypeFilterConfig:
    res = {k: _parse_res_filter(v) for k, v in d.get("resstock", {}).items()}
    com = {k: _parse_com_filter(v) for k, v in d.get("comstock", {}).items()}
    return ArchetypeFilterConfig(resstock=res, comstock=com, min_pool_size=d.get("min_pool_size", 10))


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
            k: SoftConstraintBound(min=v.get("min"), max=v.get("max")) for k, v in soft_raw.get("bounds", {}).items()
        },
    )
    return FeasibilityConfig(hard=hard, soft=soft)


def _parse_perturbation(d: dict) -> PerturbationConfig:
    factors = {k: EndUseFactorConfig(sigma=float(v.get("sigma", 0.0))) for k, v in d.get("end_use_factors", {}).items()}
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


# ---------------------------------------------------------------------------
# Strict key checking: nothing in a config file is silently ignored
# ---------------------------------------------------------------------------

_METRICS = ("peak_kw", "annual_energy_kwh", "load_factor")

#: Keys that older configs carried but that the engine never applied. They are rejected, with a pointer.
_UNSUPPORTED_KEYS = {
    "arealimit": "not supported; the supported filter is `max_sqft` (floor-area cap)",
    "garage": "not supported; the supported filter is `require_no_garage`",
    "heatpump": "not supported: adoption rates need scenario datasets (planned, FR-ENG-12)",
    "electric_vehicle": "not supported: adoption rates need scenario datasets (planned, FR-ENG-12)",
    "solar_pv": "not supported: adoption rates need scenario datasets (planned, FR-ENG-12)",
}

_TOP_KEYS = {
    "scenario",
    "rng",
    "data",
    "composition",
    "archetype_filters",
    "perturbation",
    "feasibility",
    "ensemble",
    "convergence",
    "validation",
    "output",
}
_SECTION_KEYS = {
    "scenario": {"name", "description"},
    "rng": {"composition_seed", "archetype_seed", "perturbation_seed"},
    "data": {f.name for f in fields(DataConfig)},
    "composition": {"total_buildings", "method", "dirichlet_concentration", "categories"},
    "perturbation": {"enabled", "max_temporal_shift_hours", "energy_tolerance", "end_use_factors"},
    "feasibility": {"hard", "soft"},
    "ensemble": {"mode", "min_runs", "max_runs", "check_every"},
    "convergence": {"lookback_window", "min_stable_window", "epsilon", "metrics"},
    "validation": {"benchmarks", "min_cv_peak"},
    "output": {f.name for f in fields(OutputConfig)},
    "archetype_filters": {"resstock", "comstock", "min_pool_size"},
}
_CATEGORY_KEYS = {"min_fraction", "max_fraction", "fixed_count", "source"}
_RES_FILTER_KEYS = {f.name for f in fields(ResidentialFilterConfig)}
_COM_FILTER_KEYS = {f.name for f in fields(CommercialFilterConfig)}
_MIN_MAX = {"min", "max"}


def _check_keys(path: str, value: object, allowed: set[str], errors: list[str]) -> dict | None:
    """Record an error for every key of ``value`` not in ``allowed``; return it if it is a mapping."""
    if not isinstance(value, dict):
        errors.append(f"{path or 'top level'}: expected a mapping, got {type(value).__name__}")
        return None
    for key in value:
        where = f"{path}.{key}" if path else str(key)
        if key in _UNSUPPORTED_KEYS:
            errors.append(f"{where}: {_UNSUPPORTED_KEYS[key]}")
        elif key not in allowed:
            hint = difflib.get_close_matches(str(key), sorted(allowed), n=1)
            suffix = f" (did you mean `{hint[0]}`?)" if hint else ""
            errors.append(f"{where}: unknown key{suffix}")
    return value


def _check_raw_config(raw: dict) -> list[str]:
    """Walk the raw YAML mapping and return every unknown/unsupported key as a readable error."""
    errors: list[str] = []
    _check_keys("", raw, _TOP_KEYS, errors)
    for section, allowed in _SECTION_KEYS.items():
        if section in raw:
            _check_keys(section, raw[section], allowed, errors)

    comp = raw.get("composition")
    if isinstance(comp, dict) and isinstance(comp.get("categories"), dict):
        for name, bounds in comp["categories"].items():
            _check_keys(f"composition.categories.{name}", bounds, _CATEGORY_KEYS, errors)

    filters = raw.get("archetype_filters")
    if isinstance(filters, dict):
        for kind, allowed in (("resstock", _RES_FILTER_KEYS), ("comstock", _COM_FILTER_KEYS)):
            group = filters.get(kind)
            if isinstance(group, dict):
                for name, body in group.items():
                    where = f"archetype_filters.{kind}.{name}"
                    if _check_keys(where, body, allowed, errors) is None:
                        continue
                    attrs = body.get("attributes")
                    if attrs is None:
                        continue
                    if not isinstance(attrs, list):
                        errors.append(f"{where}.attributes: expected a list of filters, got {type(attrs).__name__}")
                        continue
                    for i, item in enumerate(attrs):
                        check_attribute_filter_raw(item, f"{where}.attributes[{i}]", errors)

    feas = raw.get("feasibility")
    if isinstance(feas, dict):
        hard = feas.get("hard")
        if isinstance(hard, dict):
            for metric, body in hard.items():
                if metric not in _METRICS:
                    errors.append(f"feasibility.hard.{metric}: unknown metric (one of {', '.join(_METRICS)})")
                else:
                    _check_keys(f"feasibility.hard.{metric}", body, _MIN_MAX, errors)
        soft = feas.get("soft")
        if isinstance(soft, dict):
            _check_keys("feasibility.soft", soft, {"weights", "bounds"}, errors)
            bounds = soft.get("bounds")
            if isinstance(bounds, dict):
                for metric, body in bounds.items():
                    _check_keys(f"feasibility.soft.bounds.{metric}", body, _MIN_MAX, errors)

    pert = raw.get("perturbation")
    if isinstance(pert, dict) and isinstance(pert.get("end_use_factors"), dict):
        for group, body in pert["end_use_factors"].items():
            _check_keys(f"perturbation.end_use_factors.{group}", body, {"sigma"}, errors)
    return errors


def _text(value: object) -> str:
    """A YAML text field may be a string or a list of strings (joined with spaces)."""
    if isinstance(value, list):
        return " ".join(str(v) for v in value)
    return "" if value is None else str(value)


def load_config(path: Path | str) -> ScenarioConfig:
    """Load a YAML scenario config file and return a typed ScenarioConfig."""
    path = Path(path)
    with path.open() as f:
        raw = yaml.safe_load(f) or {}
    return config_from_dict(raw, source=str(path), default_name=path.stem)


def config_from_dict(raw: object, *, source: str = "<dict>", default_name: str = "unnamed") -> ScenarioConfig:
    """Parse and strictly validate an already-loaded config mapping (same rules as a YAML file).

    ``source`` prefixes error messages (a file path for YAML, ``<dict>`` for in-memory configs).
    """
    problems = _check_raw_config(raw) if isinstance(raw, dict) else ["top level: expected a mapping"]
    if problems:
        raise ValueError(f"{source}: invalid configuration:\n" + "\n".join(f"  - {p}" for p in problems))
    assert isinstance(raw, dict)

    scenario = raw.get("scenario", {})
    cfg = ScenarioConfig(
        name=_text(scenario.get("name")) or default_name,
        description=_text(scenario.get("description")),
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

    try:
        validate_config(cfg)
    except ValueError as exc:
        raise ValueError(f"{source}: {exc}") from None
    return cfg


def validate_config(cfg: ScenarioConfig) -> None:
    """Raise ValueError if any config invariants are violated."""
    errors: list[str] = []

    if cfg.composition.total_buildings < 1:
        errors.append(f"composition.total_buildings must be at least 1, got {cfg.composition.total_buildings}")
    if cfg.composition.method not in ("dirichlet", "uniform_renorm"):
        errors.append(f"composition.method must be 'dirichlet' or 'uniform_renorm', got {cfg.composition.method!r}")
    if cfg.ensemble.mode not in ("fixed", "adaptive"):
        errors.append(f"ensemble.mode must be 'fixed' or 'adaptive', got {cfg.ensemble.mode!r}")
    if cfg.ensemble.max_runs < 1:
        errors.append(f"ensemble.max_runs must be at least 1, got {cfg.ensemble.max_runs}")
    if cfg.convergence.epsilon <= 0 or cfg.convergence.lookback_window < 1:
        errors.append("convergence.epsilon must be positive and lookback_window at least 1")
    bad_pct = [p for p in cfg.output.percentiles if not 0.0 < p < 1.0]
    if bad_pct:
        errors.append(f"output.percentiles must be between 0 and 1 (exclusive), got {bad_pct}")
    mps = cfg.archetype_filters.min_pool_size
    if isinstance(mps, bool) or not isinstance(mps, int) or mps < 0:
        errors.append(f"archetype_filters.min_pool_size must be an integer of 0 or more, got {mps!r}")
    for fld in ("dataset_resstock", "dataset_comstock"):
        value = getattr(cfg.data, fld)
        if value is not None and (not isinstance(value, str) or len([p for p in value.split(":") if p.strip()]) != 5):
            errors.append(
                f"data.{fld} must be a dataset id 'product:release:weather:scenario:state', got {value!r}"
            )
        if value is not None and not value.startswith(fld.removeprefix("dataset_") + ":"):
            errors.append(f"data.{fld} must start with '{fld.removeprefix('dataset_')}:', got {value!r}")
    for fld in ("resstock_load_basis", "comstock_load_basis"):
        if getattr(cfg.data, fld) not in (None, "gross", "net"):
            errors.append(f"data.{fld} must be 'gross' or 'net', got {getattr(cfg.data, fld)!r}")
    for name, c in cfg.composition.categories.items():
        where = f"composition.categories.{name}"
        if c.source not in ("resstock", "comstock"):
            errors.append(f"{where}.source must be 'resstock' or 'comstock', got {c.source!r}")
        if c.fixed_count is not None and c.fixed_count < 0:
            errors.append(f"{where}.fixed_count must be 0 or more, got {c.fixed_count}")
        if c.fixed_count is None and not 0.0 <= c.min_fraction <= c.max_fraction <= 1.0:
            errors.append(
                f"{where}: need 0 <= min_fraction <= max_fraction <= 1, got {c.min_fraction}..{c.max_fraction}"
            )

    # Composition bounds feasibility
    cats = cfg.composition.categories
    # An all-fixed composition has no stochastic bounds to check.
    if any(c.fixed_count is None for c in cats.values()):
        p_min_sum = sum(c.min_fraction for c in cats.values() if c.fixed_count is None)
        p_max_sum = sum(c.max_fraction for c in cats.values() if c.fixed_count is None)
        if p_min_sum > 1.0 + 1e-9:
            errors.append(f"Sum of min_fraction values ({p_min_sum:.4f}) exceeds 1.0 — no feasible composition exists.")
        if p_max_sum < 1.0 - 1e-9:
            errors.append(
                f"Sum of max_fraction values ({p_max_sum:.4f}) is below 1.0 — no feasible composition exists."
            )

    if errors:
        raise ValueError("ScenarioConfig validation failed:\n" + "\n".join(f"  - {e}" for e in errors))
