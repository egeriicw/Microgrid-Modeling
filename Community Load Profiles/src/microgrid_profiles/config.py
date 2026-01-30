"""Configuration models and helpers for the microgrid profile pipeline.

The pipeline is driven by a YAML file that describes:

- Input data roots and file templates.
- Scenario identifiers (state, upgrade number, run id).
- Neighborhood composition (number of buildings and mix by type).
- Optional weather inputs and output toggles.

This module provides:

- Frozen :class:`dataclasses.dataclass` models used throughout the codebase.
- :func:`load_config` to load a YAML file into a :class:`ScenarioConfig`.
- Small helper functions to apply CLI overrides via :func:`dataclasses.replace`.

Docstring style follows Real Python / PEP 257 guidance.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Optional

import yaml


@dataclass(frozen=True)
class OutputOptions:
    """Controls which official outputs are written.

    Attributes:
        write_csv: Whether to write CSV outputs.
        write_txt: Whether to write text overview output(s).
        write_xlsx: Whether to write Excel workbooks.
        write_plots: Whether to write plot image files.
    """

    write_csv: bool = True
    write_txt: bool = True
    write_xlsx: bool = True
    write_plots: bool = True


@dataclass(frozen=True)
class PerformanceConfig:
    """Performance and I/O options.

    Attributes:
        fast_io: If ``True``, read parquet files in parallel using threads.
        max_workers: Maximum number of worker threads for parallel I/O.
        prune_parquet_columns: If ``True``, only read needed parquet columns.
    """

    fast_io: bool = False
    max_workers: int = 8
    prune_parquet_columns: bool = True


@dataclass(frozen=True)
class WeatherConfig:
    """Outdoor air temperature ingestion settings.

    Attributes:
        enabled: Whether weather data is joined to hourly/daily/monthly outputs.
        preferred_units: Preferred temperature units: ``"C"`` or ``"F"``.
        files: List of weather CSV file paths.
        source_labels: Optional list of labels matching ``files``.
    """

    enabled: bool = False
    preferred_units: str = "C"
    files: Optional[list[str]] = None
    source_labels: Optional[list[str]] = None


@dataclass(frozen=True)
class ProfilesConfig:
    """Typical-day-by-month profile settings.

    These settings control the additional profile-style outputs derived from the
    hourly load series.

    Attributes:
        write_typical_day_by_month: Whether to compute typical day by month.
        include_by_building_type: Whether to include building-type breakdowns.
        include_sector_comparison: Whether to include sector comparisons.
        average_across_runs: Whether to average profiles across all sampled runs.
        write_long_csv: Whether to write the long-form CSV output.
        write_pivot_csv: Whether to write the pivot-table CSV output.
        workbook_include_run0: Whether to include run-0 data in the workbook.
    """

    write_typical_day_by_month: bool = True
    include_by_building_type: bool = True
    include_sector_comparison: bool = True
    average_across_runs: bool = True
    write_long_csv: bool = True
    write_pivot_csv: bool = True
    workbook_include_run0: bool = True


@dataclass(frozen=True)
class ColumnConfig:
    """Column names expected in input data.

    The code works with multiple upstream datasets. To avoid hard-coding column
    names, they are provided through configuration.

    Attributes:
        building_id_resstock: Column name for ResStock building identifier.
        building_id_comstock: Column name for ComStock building identifier.
        resstock_building_type: ResStock building type column name.
        resstock_units_mf: ResStock multi-family units column name.
        resstock_sqft: ResStock square-footage column name.
        resstock_electricity_kwh: ResStock electricity consumption column name.
        comstock_building_type: ComStock building type column name.
        comstock_sqft: ComStock square-footage column name.
        comstock_electricity_kwh: ComStock electricity consumption column name.
        electricity_kwh: Generic electricity consumption column name (parquet).
    """

    building_id_resstock: str
    building_id_comstock: str
    resstock_building_type: str
    resstock_units_mf: str
    resstock_sqft: str
    resstock_electricity_kwh: str
    comstock_building_type: str
    comstock_sqft: str
    comstock_electricity_kwh: str
    electricity_kwh: str


@dataclass(frozen=True)
class SingleFamilyConfig:
    """Selection parameters specific to single-family housing."""

    max_footprint_area: float = 2000
    electric_hp_percent: float = 0.0


@dataclass(frozen=True)
class NeighborhoodConfig:
    """Neighborhood selection configuration.

    Attributes:
        total_buildings: Total number of buildings in the neighborhood.
        multifamily_buildings_percent_of_total: Share of buildings that are multifamily.
        single_family_buildings_percent_of_total: Share of buildings that are single-family.
        multifamily_building_names: ResStock building types that count as multifamily.
        multifamily_buildings_percentages: Mix among multifamily types. Must sum to 1.0.
        single_family: Additional single-family configuration.
    """

    total_buildings: int
    multifamily_buildings_percent_of_total: float
    single_family_buildings_percent_of_total: float
    multifamily_building_names: list[str]
    multifamily_buildings_percentages: dict[str, float]
    single_family: SingleFamilyConfig


@dataclass(frozen=True)
class ScenarioConfig:
    """Top-level configuration passed through the pipeline.

    Attributes:
        state: Two-letter state code used in templated file paths.
        upgrade_num: Upgrade number used in templated file paths.
        input_root: Root directory for input data.
        output_root: Root directory for output data.
        run_id: Optional explicit run identifier (otherwise auto-generated).
        seed: Random seed for selection.
        sample_runs: Number of Monte-Carlo runs to generate.
        include_public: Whether to include public/commercial building types.
        adjustment_multiplier_off: If ``True``, disable random per-run multipliers.
        outputs: Output writing toggles.
        performance: Performance / parallel I/O settings.
        weather: Weather ingestion settings.
        profiles: Typical-day profile settings.
        resstock_characteristics_xlsx: Path template for ResStock characteristics.
        comstock_characteristics_xlsx: Path template for ComStock characteristics.
        resstock_timeseries_dir: Path template for ResStock timeseries directory.
        comstock_timeseries_dir: Path template for ComStock timeseries directory.
        columns: Input column names.
        multifamily_filter: Building types considered multifamily for adjustments.
        neighborhood: Neighborhood selection settings.
    """

    state: str
    upgrade_num: int
    input_root: Path
    output_root: Path
    run_id: Optional[str]
    seed: int
    sample_runs: int
    include_public: bool
    adjustment_multiplier_off: bool

    outputs: OutputOptions
    performance: PerformanceConfig
    weather: WeatherConfig
    profiles: ProfilesConfig

    resstock_characteristics_xlsx: str
    comstock_characteristics_xlsx: str
    resstock_timeseries_dir: str
    comstock_timeseries_dir: str

    columns: ColumnConfig
    multifamily_filter: list[str]
    neighborhood: NeighborhoodConfig


def _require(d: dict[str, Any], key: str) -> Any:
    """Return a required key from a mapping.

    Args:
        d: Mapping of configuration keys to values.
        key: The required key to retrieve.

    Returns:
        The value stored at ``key``.

    Raises:
        ValueError: If the key is missing.
    """

    if key not in d:
        raise ValueError(f"Missing required config key: {key}")
    return d[key]


def validate_percentages(neighborhood: NeighborhoodConfig, tol: float = 1e-6) -> None:
    """Validate that neighborhood mix percentages are properly normalized.

    Args:
        neighborhood: Neighborhood configuration to validate.
        tol: Numerical tolerance for floating-point comparisons.

    Raises:
        ValueError: If any percentages are inconsistent.
    """

    mf_sum = float(sum(neighborhood.multifamily_buildings_percentages.values()))
    if abs(mf_sum - 1.0) > tol:
        raise ValueError(f"multifamily_buildings_percentages must sum to 1.0. Got {mf_sum}.")

    total_mix = float(neighborhood.multifamily_buildings_percent_of_total) + float(
        neighborhood.single_family_buildings_percent_of_total
    )
    if abs(total_mix - 1.0) > tol:
        raise ValueError(
            "multifamily_buildings_percent_of_total + single_family_buildings_percent_of_total "
            f"must sum to 1.0. Got {total_mix}."
        )


def load_config(path: str | Path) -> ScenarioConfig:
    """Load a YAML scenario config file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        A validated :class:`ScenarioConfig` instance.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        ValueError: If required configuration keys are missing or invalid.
        yaml.YAMLError: If the YAML cannot be parsed.
    """

    path = Path(path)
    raw = yaml.safe_load(path.read_text())

    outputs_raw = raw.get("outputs", {}) or {}
    outputs = OutputOptions(
        write_csv=bool(outputs_raw.get("write_csv", True)),
        write_txt=bool(outputs_raw.get("write_txt", True)),
        write_xlsx=bool(outputs_raw.get("write_xlsx", True)),
        write_plots=bool(outputs_raw.get("write_plots", True)),
    )

    perf_raw = raw.get("performance", {}) or {}
    performance = PerformanceConfig(
        fast_io=bool(perf_raw.get("fast_io", False)),
        max_workers=int(perf_raw.get("max_workers", 8)),
        prune_parquet_columns=bool(perf_raw.get("prune_parquet_columns", True)),
    )

    weather_raw = raw.get("weather", {}) or {}
    weather = WeatherConfig(
        enabled=bool(weather_raw.get("enabled", False)),
        preferred_units=str(weather_raw.get("preferred_units", "C")),
        files=list(weather_raw.get("files", []) or []),
        source_labels=list(weather_raw.get("source_labels", []) or []),
    )

    profiles_raw = raw.get("profiles", {}) or {}
    profiles = ProfilesConfig(
        write_typical_day_by_month=bool(profiles_raw.get("write_typical_day_by_month", True)),
        include_by_building_type=bool(profiles_raw.get("include_by_building_type", True)),
        include_sector_comparison=bool(profiles_raw.get("include_sector_comparison", True)),
        average_across_runs=bool(profiles_raw.get("average_across_runs", True)),
        write_long_csv=bool(profiles_raw.get("write_long_csv", True)),
        write_pivot_csv=bool(profiles_raw.get("write_pivot_csv", True)),
        workbook_include_run0=bool(profiles_raw.get("workbook_include_run0", True)),
    )

    columns_raw = _require(raw, "columns")
    columns = ColumnConfig(
        building_id_resstock=_require(columns_raw, "building_id_resstock"),
        building_id_comstock=_require(columns_raw, "building_id_comstock"),
        resstock_building_type=_require(columns_raw, "resstock_building_type"),
        resstock_units_mf=_require(columns_raw, "resstock_units_mf"),
        resstock_sqft=_require(columns_raw, "resstock_sqft"),
        resstock_electricity_kwh=_require(columns_raw, "resstock_electricity_kwh"),
        comstock_building_type=_require(columns_raw, "comstock_building_type"),
        comstock_sqft=_require(columns_raw, "comstock_sqft"),
        comstock_electricity_kwh=_require(columns_raw, "comstock_electricity_kwh"),
        electricity_kwh=_require(columns_raw, "electricity_kwh"),
    )

    sf_raw = raw.get("single_family", {}) or {}
    single_family = SingleFamilyConfig(
        max_footprint_area=float(sf_raw.get("max_footprint_area", 3000)),
        electric_hp_percent=float(sf_raw.get("electric_hp_percent", 0.4)),
    )

    neighborhood = NeighborhoodConfig(
        total_buildings=int(_require(raw, "total_buildings")),
        multifamily_buildings_percent_of_total=float(_require(raw, "multifamily_buildings_percent_of_total")),
        single_family_buildings_percent_of_total=float(_require(raw, "single_family_buildings_percent_of_total")),
        multifamily_building_names=list(_require(raw, "multifamily_building_names")),
        multifamily_buildings_percentages=dict(_require(raw, "multifamily_buildings_percentages")),
        single_family=single_family,
    )

    cfg = ScenarioConfig(
        state=str(_require(raw, "state")),
        upgrade_num=int(_require(raw, "upgrade_num")),
        input_root=Path(str(_require(raw, "input_root"))),
        output_root=Path(str(_require(raw, "output_root"))),
        run_id=raw.get("run_id", None),
        seed=int(_require(raw, "seed")),
        sample_runs=int(_require(raw, "sample_runs")),
        include_public=bool(_require(raw, "include_public")),
        adjustment_multiplier_off=bool(_require(raw, "adjustment_multiplier_off")),
        outputs=outputs,
        performance=performance,
        weather=weather,
        profiles=profiles,
        resstock_characteristics_xlsx=str(_require(raw, "resstock_characteristics_xlsx")),
        comstock_characteristics_xlsx=str(_require(raw, "comstock_characteristics_xlsx")),
        resstock_timeseries_dir=str(_require(raw, "resstock_timeseries_dir")),
        comstock_timeseries_dir=str(_require(raw, "comstock_timeseries_dir")),
        columns=columns,
        multifamily_filter=list(_require(raw, "multifamily_filter")),
        neighborhood=neighborhood,
    )

    validate_percentages(cfg.neighborhood)
    return cfg


def apply_profiles_overrides(cfg: ScenarioConfig, **kwargs: Any) -> ScenarioConfig:
    """Return a new config with :class:`ProfilesConfig` overrides applied."""

    return replace(cfg, profiles=replace(cfg.profiles, **kwargs))


def apply_weather_overrides(cfg: ScenarioConfig, **kwargs: Any) -> ScenarioConfig:
    """Return a new config with :class:`WeatherConfig` overrides applied."""

    return replace(cfg, weather=replace(cfg.weather, **kwargs))


def apply_performance_overrides(cfg: ScenarioConfig, **kwargs: Any) -> ScenarioConfig:
    """Return a new config with :class:`PerformanceConfig` overrides applied."""

    return replace(cfg, performance=replace(cfg.performance, **kwargs))
