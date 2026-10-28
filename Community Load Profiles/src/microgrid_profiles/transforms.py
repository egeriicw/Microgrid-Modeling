"""Data transformation and aggregation helpers.

These functions operate primarily on pandas objects:

- Merging building timeseries with characteristics.
- Applying adjustments (e.g., multifamily scaling).
- Resampling to hourly, daily, and monthly frequency.
- Building combined totals and cross-run compiled outputs.

The functions are intentionally small and composable to support unit testing.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .config import ScenarioConfig


def merge_timeseries(cfg: ScenarioConfig, ts: pd.DataFrame, chars: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """Merge timeseries rows with a characteristics table.

    Args:
        cfg: Scenario configuration (currently unused, reserved for future options).
        ts: Timeseries DataFrame containing a ``timestamp`` column and an ID column.
        chars: Characteristics DataFrame keyed by ``id_col``.
        id_col: Column name to join on.

    Returns:
        A DataFrame indexed by timestamp.

    Raises:
        KeyError: If ``id_col`` is missing from the input frames.
    """

    merged = ts.merge(chars, on=id_col, how="left")
    merged["timestamp"] = pd.to_datetime(merged["timestamp"])
    return merged.set_index("timestamp")


def apply_multifamily_adjustments(cfg: ScenarioConfig, df: pd.DataFrame) -> pd.DataFrame:
    """Apply multifamily scaling adjustments.

    For selected multifamily building types, this function scales electricity
    consumption by the number of units and creates an adjusted square-footage
    column.

    Args:
        cfg: Scenario configuration.
        df: Merged timeseries/characteristics DataFrame.

    Returns:
        A copy of ``df`` with adjustments applied.

    Notes:
        If required columns are missing, the function returns ``df`` unchanged.
    """

    c = cfg.columns
    out = df.copy()

    mf_mask = (
        out[c.resstock_building_type].isin(set(cfg.multifamily_filter))
        if c.resstock_building_type in out.columns
        else None
    )
    if mf_mask is None:
        return out

    if c.electricity_kwh in out.columns and c.resstock_units_mf in out.columns:
        out.loc[mf_mask, c.electricity_kwh] = out.loc[mf_mask, c.electricity_kwh] * out.loc[mf_mask, c.resstock_units_mf]

    if c.resstock_sqft in out.columns and c.resstock_units_mf in out.columns:
        out["in.sqft_adjust"] = out[c.resstock_sqft]
        out.loc[mf_mask, "in.sqft_adjust"] = out.loc[mf_mask, c.resstock_sqft] * out.loc[mf_mask, c.resstock_units_mf]

    return out


def resample_hourly_sum(df: pd.DataFrame) -> pd.DataFrame:
    """Resample a DataFrame to hourly frequency using sum."""

    return df.resample("h").sum(numeric_only=True) if not df.empty else df


def agg_daily_sum(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate a DataFrame to daily totals (sum)."""

    return df.resample("D").sum(numeric_only=True) if not df.empty else df


def agg_monthly_sum(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate a DataFrame to monthly totals (sum).

    Uses month-end frequency (``"ME"``).
    """

    return df.resample("ME").sum(numeric_only=True) if not df.empty else df


def agg_daily_mean(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Aggregate a single column to daily mean values."""

    return df[[col]].resample("D").mean(numeric_only=True) if (not df.empty and col in df.columns) else pd.DataFrame(index=df.index)


def agg_monthly_mean(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Aggregate a single column to monthly mean values."""

    return df[[col]].resample("ME").mean(numeric_only=True) if (not df.empty and col in df.columns) else pd.DataFrame(index=df.index)


def build_total(
    res_h: pd.DataFrame,
    com_h: pd.DataFrame,
    res_kwh_col: str,
    com_kwh_col: str,
    tot_kwh_col: str = "Total",
) -> pd.DataFrame:
    """Build an hourly total community load series.

    Args:
        res_h: Residential hourly DataFrame.
        com_h: Commercial hourly DataFrame.
        res_kwh_col: Residential kWh column name.
        com_kwh_col: Commercial kWh column name.
        tot_kwh_col: Output column name for the total.

    Returns:
        A DataFrame with a single total column.
    """

    # Retain this print to match existing pipeline behavior.
    print("Build Total.")

    parts: list[pd.Series] = []
    if res_kwh_col in res_h.columns:
        parts.append(res_h[res_kwh_col])
    if com_kwh_col in com_h.columns:
        parts.append(com_h[com_kwh_col])

    if not parts:
        return pd.DataFrame()

    total = pd.concat(parts, axis=1).fillna(0.0).sum(axis=1)
    return pd.DataFrame({tot_kwh_col: total})


def build_compiled(idx: pd.DatetimeIndex) -> pd.DataFrame:
    """Create an empty compiled-runs table indexed by timestamp."""

    return pd.DataFrame(index=idx)


def add_run_cols(
    compiled: pd.DataFrame,
    i: int,
    res_h: pd.DataFrame,
    com_h: pd.DataFrame,
    res_kwh_col: str,
    com_kwh_col: str,
) -> pd.DataFrame:
    """Add residential and commercial columns for a specific run.

    Args:
        compiled: Compiled DataFrame to extend.
        i: Run index.
        res_h: Residential hourly data.
        com_h: Commercial hourly data.
        res_kwh_col: Residential kWh column name.
        com_kwh_col: Commercial kWh column name.

    Returns:
        A copy of ``compiled`` with additional columns.

    Raises:
        KeyError: If required columns are missing.
    """

    out = compiled.copy()
    out[f"Residential{i}"] = res_h[res_kwh_col].values
    out[f"Commercial{i}"] = com_h[com_kwh_col].values
    return out


def avg_from_compiled(compiled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute average residential, commercial, and total series from compiled runs.

    Args:
        compiled: Compiled DataFrame produced by :func:`add_run_cols`.

    Returns:
        Tuple of ``(residential_df, commercial_df, total_df)``.
    """

    res_cols = [c for c in compiled.columns if c.startswith("Residential")]
    com_cols = [c for c in compiled.columns if c.startswith("Commercial")]

    res = compiled[res_cols].mean(axis=1) if res_cols else pd.Series(index=compiled.index, dtype=float)
    com = compiled[com_cols].mean(axis=1) if com_cols else pd.Series(index=compiled.index, dtype=float)
    tot = res.add(com, fill_value=0.0)

    return pd.DataFrame({"Residential": res}), pd.DataFrame({"Commercial": com}), pd.DataFrame({"Total": tot})


def typical_day_monthly(hourly_series: pd.Series, sector: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute a typical (mean) day profile by month.

    Args:
        hourly_series: Hourly energy series indexed by datetime.
        sector: Label for the sector (e.g., ``"residential"``).

    Returns:
        A tuple of:

        - ``long_df``: Long-form DataFrame with columns ``sector, month, hour, mean_kwh``.
        - ``pivot``: Pivoted DataFrame with hour as index and month as columns.

    Raises:
        AttributeError: If ``hourly_series`` does not have a datetime index.
    """

    df = hourly_series.to_frame("mean_kwh")
    df["sector"] = sector
    df["month"] = df.index.month
    df["hour"] = df.index.hour

    long_df = df.groupby(["sector", "month", "hour"], as_index=False)["mean_kwh"].mean()
    pivot = long_df.pivot_table(index="hour", columns="month", values="mean_kwh", aggfunc="mean").sort_index()

    return long_df.sort_values(["sector", "month", "hour"]), pivot
