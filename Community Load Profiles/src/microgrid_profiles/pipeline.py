from __future__ import annotations
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

from .config import ScenarioConfig
from .io import read_characteristics_xlsx, read_many_building_parquets, read_many_building_parquets_cached, write_csv, write_txt, write_xlsx
from .paths import ensure_dir, resolve_paths
from .selection import building_parquet_path, construct_neighborhood_residential, construct_neighborhood_commercial
from .transforms import merge_timeseries, apply_multifamily_adjustments, resample_hourly_sum, agg_daily_sum, agg_monthly_sum, agg_daily_mean, agg_monthly_mean, build_total, build_compiled, add_run_cols, avg_from_compiled, typical_day_monthly
from .viz import plot_hourly
from .weather import read_weather_csv_auto

def _run_id():
    return datetime.now().strftime("%Y-%m-%dT%H%M%S")

def run_pipeline(cfg: ScenarioConfig) -> Path:
    run_id = cfg.run_id or _run_id()
    paths = resolve_paths(cfg, run_id)
    ensure_dir(paths.scenario_output_dir)

    res_chars = read_characteristics_xlsx(paths.resstock_characteristics_xlsx)
    com_chars = read_characteristics_xlsx(paths.comstock_characteristics_xlsx)

    rng = np.random.default_rng(cfg.seed)
    mult = np.ones(cfg.sample_runs) if cfg.adjustment_multiplier_off else rng.normal(1.0, 0.2, size=cfg.sample_runs)

    weather_df = None
    if cfg.weather.enabled and cfg.weather.files:
        parts = []
        for i, fp in enumerate(cfg.weather.files):
            label = (cfg.weather.source_labels[i] if cfg.weather.source_labels and i < len(cfg.weather.source_labels) else f"weather_{i}")
            parts.append(read_weather_csv_auto(Path(fp), preferred_units=cfg.weather.preferred_units, source_label=label).df)
        weather_df = None
        for p in parts:
            weather_df = p if weather_df is None else weather_df.combine_first(p)

    compiled = None
    sheets: dict[str, pd.DataFrame] = {}

    c = cfg.columns

    for i in range(cfg.sample_runs):
        run_dir = paths.scenario_output_dir / f"Run-{i}"
        ensure_dir(run_dir)

        res_sel = construct_neighborhood_residential(cfg, res_chars, rng)
        com_sel = construct_neighborhood_commercial(cfg, com_chars, rng)

        res_paths = [building_parquet_path(paths.resstock_timeseries_dir, bid) for bid in res_sel.building_ids]
        com_paths = [building_parquet_path(paths.comstock_timeseries_dir, bid) for bid in com_sel.building_ids]

        if cfg.performance.fast_io:
            res_ts = read_many_building_parquets_cached(res_paths, max_workers=cfg.performance.max_workers, columns=([c.building_id_resstock, c.electricity_kwh] if cfg.performance.prune_parquet_columns else None))
            com_ts = read_many_building_parquets_cached(com_paths, max_workers=cfg.performance.max_workers, columns=([c.building_id_comstock, c.electricity_kwh] if cfg.performance.prune_parquet_columns else None))
        else:
            res_ts = read_many_building_parquets(res_paths)
            com_ts = read_many_building_parquets(com_paths)

        res_subset = res_chars[res_chars[c.building_id_resstock].isin(res_sel.building_ids)].copy()
        com_subset = com_chars[com_chars[c.building_id_comstock].isin(com_sel.building_ids)].copy()

        res_m = merge_timeseries(cfg, res_ts, res_subset, c.building_id_resstock)
        com_m = merge_timeseries(cfg, com_ts, com_subset, c.building_id_comstock)

        res_m = apply_multifamily_adjustments(cfg, res_m)
        if c.electricity_kwh in res_m.columns:
            res_m[c.electricity_kwh] = res_m[c.electricity_kwh] * float(mult[i])

        res_h_full = resample_hourly_sum(res_m)
        com_h_full = resample_hourly_sum(com_m)

        res_h = res_h_full[[c.electricity_kwh]] if c.electricity_kwh in res_h_full.columns else res_h_full
        com_h = com_h_full[[c.electricity_kwh]] if c.electricity_kwh in com_h_full.columns else com_h_full
        tot_h = build_total(res_h, com_h, c.electricity_kwh)

        if weather_df is not None:
            res_h = res_h.join(weather_df, how="left")
            com_h = com_h.join(weather_df, how="left")
            tot_h = tot_h.join(weather_df, how="left")

        res_d = agg_daily_sum(res_h[[c.electricity_kwh]]) if c.electricity_kwh in res_h.columns else agg_daily_sum(res_h)
        com_d = agg_daily_sum(com_h[[c.electricity_kwh]]) if c.electricity_kwh in com_h.columns else agg_daily_sum(com_h)
        tot_d = agg_daily_sum(tot_h[[c.electricity_kwh]]) if c.electricity_kwh in tot_h.columns else agg_daily_sum(tot_h)

        res_mo = agg_monthly_sum(res_h[[c.electricity_kwh]]) if c.electricity_kwh in res_h.columns else agg_monthly_sum(res_h)
        com_mo = agg_monthly_sum(com_h[[c.electricity_kwh]]) if c.electricity_kwh in com_h.columns else agg_monthly_sum(com_h)
        tot_mo = agg_monthly_sum(tot_h[[c.electricity_kwh]]) if c.electricity_kwh in tot_h.columns else agg_monthly_sum(tot_h)

        if "outdoor_air_temperature" in tot_h.columns:
            t_d = agg_daily_mean(tot_h, "outdoor_air_temperature")
            t_m = agg_monthly_mean(tot_h, "outdoor_air_temperature")
            res_d = res_d.join(t_d, how="left"); com_d = com_d.join(t_d, how="left"); tot_d = tot_d.join(t_d, how="left")
            res_mo = res_mo.join(t_m, how="left"); com_mo = com_mo.join(t_m, how="left"); tot_mo = tot_mo.join(t_m, how="left")

        if compiled is None:
            compiled = build_compiled(res_h.index)
        compiled = add_run_cols(compiled, i, res_h, com_h, c.electricity_kwh)

        if cfg.outputs.write_csv:
            write_csv(res_h, run_dir / "residential_community_load_profile_hourly.csv")
            write_csv(com_h, run_dir / "commercial_community_load_profile_hourly.csv")
            write_csv(tot_h, run_dir / "total_community_load_profile_hourly.csv")
            write_csv(res_d, run_dir / "residential_community_load_profile_daily.csv")
            write_csv(com_d, run_dir / "commercial_community_load_profile_daily.csv")
            write_csv(tot_d, run_dir / "total_community_load_profile_daily.csv")
            write_csv(res_mo, run_dir / "residential_community_load_profile_monthly.csv")
            write_csv(com_mo, run_dir / "commercial_community_load_profile_monthly.csv")
            write_csv(tot_mo, run_dir / "total_community_load_profile_monthly.csv")

        if cfg.profiles.write_typical_day_by_month and c.electricity_kwh in res_h.columns and c.electricity_kwh in com_h.columns:
            res_long, res_pivot = typical_day_monthly(res_h[c.electricity_kwh], "residential")
            com_long, com_pivot = typical_day_monthly(com_h[c.electricity_kwh], "commercial")
            if cfg.outputs.write_csv and cfg.profiles.write_long_csv:
                write_csv(res_long, run_dir / "typical_day_monthly_residential_long.csv")
                write_csv(com_long, run_dir / "typical_day_monthly_commercial_long.csv")
            if cfg.outputs.write_csv and cfg.profiles.write_pivot_csv:
                write_csv(res_pivot, run_dir / "typical_day_monthly_residential_pivot.csv")
                write_csv(com_pivot, run_dir / "typical_day_monthly_commercial_pivot.csv")

        if cfg.outputs.write_plots and c.electricity_kwh in tot_h.columns:
            plot_hourly(tot_h[[c.electricity_kwh]], f"Run {i} Total Community Hourly Profile", paths.scenario_output_dir / "plots" / f"run_{i:02d}_hourly.png")

        if i == 0 and cfg.outputs.write_xlsx and cfg.profiles.workbook_include_run0:
            sheets["run0_total_hourly"] = tot_h.copy()
            sheets["run0_total_daily"] = tot_d.copy()
            sheets["run0_total_monthly"] = tot_mo.copy()

    if compiled is None:
        return paths.scenario_output_dir

    if cfg.outputs.write_csv:
        write_csv(compiled, paths.scenario_output_dir / "compiled_runs.csv")

    res_avg, com_avg, tot_avg = avg_from_compiled(compiled)
    avg_h = pd.concat([res_avg, com_avg, tot_avg], axis=1)
    avg_d = agg_daily_sum(avg_h)
    avg_mo = agg_monthly_sum(avg_h)

    if cfg.outputs.write_csv:
        write_csv(res_avg, paths.scenario_output_dir / "residential_load_profile_total.csv")
        write_csv(com_avg, paths.scenario_output_dir / "commercial_load_profile_total.csv")
        write_csv(tot_avg, paths.scenario_output_dir / "total_load_profile_total.csv")
        write_csv(avg_d, paths.scenario_output_dir / "merged_community_load_profile_daily_average.csv")
        write_csv(avg_mo, paths.scenario_output_dir / "merged_community_load_profile_monthly_average.csv")

    if cfg.outputs.write_txt:
        write_txt(f"Run ID: {run_id}\nState: {cfg.state}\nUpgrade: {cfg.upgrade_num}\nSeed: {cfg.seed}\n", paths.scenario_output_dir / f"{run_id}-overview.txt")

    if cfg.outputs.write_xlsx:
        sheets["compiled_runs"] = compiled.copy()
        sheets["avg_hourly"] = avg_h.copy()
        sheets["avg_daily"] = avg_d.copy()
        sheets["avg_monthly"] = avg_mo.copy()
        write_xlsx(paths.scenario_output_dir / "excel" / "summary.xlsx", sheets)

    if cfg.outputs.write_plots:
        plot_hourly(tot_avg.rename(columns={"Total":"Total"}), "Average Total Community Hourly Profile", paths.scenario_output_dir / "plots" / "average_total_hourly.png")

    return paths.scenario_output_dir
