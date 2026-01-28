from __future__ import annotations
import numpy as np
import pandas as pd
from .config import ScenarioConfig

def merge_timeseries(cfg: ScenarioConfig, ts: pd.DataFrame, chars: pd.DataFrame, id_col: str) -> pd.DataFrame:
    merged = ts.merge(chars, on=id_col, how="left")
    merged["timestamp"] = pd.to_datetime(merged["timestamp"])
    return merged.set_index("timestamp")

def apply_multifamily_adjustments(cfg: ScenarioConfig, df: pd.DataFrame) -> pd.DataFrame:
    c = cfg.columns
    out = df.copy()
    mf_mask = out[c.resstock_building_type].isin(set(cfg.multifamily_filter)) if c.resstock_building_type in out.columns else None
    if mf_mask is None:
        return out
    if c.electricity_kwh in out.columns and c.resstock_units_mf in out.columns:
        out.loc[mf_mask, c.electricity_kwh] = out.loc[mf_mask, c.electricity_kwh] * out.loc[mf_mask, c.resstock_units_mf]
    if c.resstock_sqft in out.columns and c.resstock_units_mf in out.columns:
        out["in.sqft_adjust"] = out[c.resstock_sqft]
        out.loc[mf_mask, "in.sqft_adjust"] = out.loc[mf_mask, c.resstock_sqft] * out.loc[mf_mask, c.resstock_units_mf]
    return out

def resample_hourly_sum(df: pd.DataFrame) -> pd.DataFrame:
    return df.resample("h").sum(numeric_only=True) if not df.empty else df

def agg_daily_sum(df: pd.DataFrame) -> pd.DataFrame:
    return df.resample("D").sum(numeric_only=True) if not df.empty else df

def agg_monthly_sum(df: pd.DataFrame) -> pd.DataFrame:
    return df.resample("ME").sum(numeric_only=True) if not df.empty else df

def agg_daily_mean(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df[[col]].resample("D").mean(numeric_only=True) if (not df.empty and col in df.columns) else pd.DataFrame(index=df.index)

def agg_monthly_mean(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df[[col]].resample("ME").mean(numeric_only=True) if (not df.empty and col in df.columns) else pd.DataFrame(index=df.index)

def build_total(res_h: pd.DataFrame, com_h: pd.DataFrame, res_kwh_col: str, com_kwh_col: str, tot_kwh_col: str = "Total") -> pd.DataFrame:
    print(f'Build Total.')
    parts = []
    if res_kwh_col in res_h.columns: parts.append(res_h[res_kwh_col])
    if com_kwh_col in com_h.columns: parts.append(com_h[com_kwh_col])
    if not parts: return pd.DataFrame()
    total = pd.concat(parts, axis=1).fillna(0.0).sum(axis=1)
    return pd.DataFrame({tot_kwh_col: total})

def build_compiled(idx: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(index=idx)

def add_run_cols(compiled: pd.DataFrame, i: int, res_h: pd.DataFrame, com_h: pd.DataFrame, res_kwh_col: str, com_kwh_col: str) -> pd.DataFrame:
    out = compiled.copy()
    out[f"Residential{i}"] = res_h[res_kwh_col].values
    out[f"Commercial{i}"] = com_h[com_kwh_col].values
    return out

def avg_from_compiled(compiled: pd.DataFrame):
    res_cols = [c for c in compiled.columns if c.startswith("Residential")]
    com_cols = [c for c in compiled.columns if c.startswith("Commercial")]
    res = compiled[res_cols].mean(axis=1) if res_cols else pd.Series(index=compiled.index, dtype=float)
    com = compiled[com_cols].mean(axis=1) if com_cols else pd.Series(index=compiled.index, dtype=float)
    tot = res.add(com, fill_value=0.0)
    return pd.DataFrame({"Residential": res}), pd.DataFrame({"Commercial": com}), pd.DataFrame({"Total": tot})

def typical_day_monthly(hourly_series: pd.Series, sector: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = hourly_series.to_frame("mean_kwh")
    df["sector"] = sector
    df["month"] = df.index.month
    df["hour"] = df.index.hour
    long_df = df.groupby(["sector","month","hour"], as_index=False)["mean_kwh"].mean()
    pivot = long_df.pivot_table(index="hour", columns="month", values="mean_kwh", aggfunc="mean").sort_index()
    return long_df.sort_values(["sector","month","hour"]), pivot
