from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Optional

import yaml


@dataclass(frozen=True)
class OutputOptions:
    """Controls which official outputs are written."""
    write_csv: bool = True
    write_txt: bool = True
    write_xlsx: bool = True
    write_plots: bool = True


@dataclass(frozen=True)
class PerformanceConfig:
    """Performance options."""
    fast_io: bool = False
    max_workers: int = 8
    prune_parquet_columns: bool = True


@dataclass(frozen=True)
class WeatherConfig:
    """Outdoor air temperature ingestion."""
    enabled: bool = False
    preferred_units: str = "C"
    files: list[str] = None
    source_labels: list[str] = None


@dataclass(frozen=True)
class ProfilesConfig:
    """Typical-day-by-month profile settings."""
    write_typical_day_by_month: bool = True
    include_by_building_type: bool = True
    include_sector_comparison: bool = True
    average_across_runs: bool = True
    write_long_csv: bool = True
    write_pivot_csv: bool = True
    workbook_include_run0: bool = True


@dataclass(frozen=True)
class ColumnConfig:
    building_id_resstock: str
    building_id_comstock: str
    resstock_building_type: str
    resstock_units_mf: str
    resstock_sqft: str
    comstock_building_type: str
    comstock_sqft: str
    electricity_kwh: str


@dataclass(frozen=True)
class SingleFamilyConfig:
    max_footprint_area: float = 3000
    electric_hp_percent: float = 0.4


@dataclass(frozen=True)
class NeighborhoodConfig:
    total_buildings: int
    multifamily_buildings_percent_of_total: float
    single_family_buildings_percent_of_total: float
    multifamily_building_names: list[str]
    multifamily_buildings_percentages: dict[str, float]
    single_family: SingleFamilyConfig


@dataclass(frozen=True)
class ScenarioConfig:
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
    if key not in d:
        raise ValueError(f"Missing required config key: {key}")
    return d[key]


def validate_percentages(neighborhood: NeighborhoodConfig, tol: float = 1e-6) -> None:
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
        comstock_building_type=_require(columns_raw, "comstock_building_type"),
        comstock_sqft=_require(columns_raw, "comstock_sqft"),
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


def apply_profiles_overrides(cfg: ScenarioConfig, **kwargs) -> ScenarioConfig:
    return replace(cfg, profiles=replace(cfg.profiles, **kwargs))


def apply_weather_overrides(cfg: ScenarioConfig, **kwargs) -> ScenarioConfig:
    return replace(cfg, weather=replace(cfg.weather, **kwargs))


def apply_performance_overrides(cfg: ScenarioConfig, **kwargs) -> ScenarioConfig:
    return replace(cfg, performance=replace(cfg.performance, **kwargs))
