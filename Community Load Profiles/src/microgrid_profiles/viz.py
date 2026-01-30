"""Visualization utilities.

Plotting is kept in a small module so the core pipeline logic can remain
headless/testable while still supporting optional plot outputs.

This module includes:

- Simple line plots for time-series outputs.
- Load duration curve (LDC) plots for hourly/daily/monthly loads.

For an LDC, values are sorted from highest to lowest and plotted against an
"exceedance" axis (the fraction or percent of time the load is equaled or
exceeded).
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def plot_hourly(df: pd.DataFrame, title: str, out_path: Path) -> None:
    """Plot a DataFrame and save it to disk.

    Args:
        df: DataFrame to plot.
        title: Plot title.
        out_path: Output image path. Parent directories are created.
    """

    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    df.plot(ax=ax, title=title, legend=True)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def compute_ldc_df(residential: pd.Series, commercial: pd.Series) -> pd.DataFrame:
    """Compute a load duration curve table for stacked residential/commercial load.

    The curve is computed by sorting intervals by *total* load
    (``residential + commercial``) in descending order. The returned table then
    contains the corresponding residential and commercial values in that order.

    Args:
        residential: Residential load series indexed by datetime.
        commercial: Commercial load series indexed by datetime.

    Returns:
        A DataFrame with columns:

        - ``rank``: 1..N where 1 is the highest-load interval.
        - ``exceedance_fraction``: rank/(N+1) in (0, 1).
        - ``exceedance_percent``: exceedance_fraction * 100.
        - ``residential``
        - ``commercial``
        - ``total``

    Notes:
        Missing values are treated as zero.
    """

    df = pd.concat(
        [
            residential.rename("residential"),
            commercial.rename("commercial"),
        ],
        axis=1,
    ).fillna(0.0)

    df["total"] = df["residential"] + df["commercial"]
    df = df.sort_values("total", ascending=False)

    n = len(df)
    rank = pd.Series(range(1, n + 1), index=df.index, dtype=float)
    exceedance_fraction = rank / float(n + 1)

    out = pd.DataFrame(
        {
            "rank": rank.values,
            "exceedance_fraction": exceedance_fraction.values,
            "exceedance_percent": (exceedance_fraction * 100.0).values,
            "residential": df["residential"].values,
            "commercial": df["commercial"].values,
            "total": df["total"].values,
        }
    )
    return out


def plot_ldc_stacked(ldc: pd.DataFrame, title: str, out_path: Path) -> None:
    """Plot a stacked load duration curve and save it.

    Args:
        ldc: DataFrame returned by :func:`compute_ldc_df`.
        title: Plot title.
        out_path: Output image path.
    """

    out_path.parent.mkdir(parents=True, exist_ok=True)

    x = ldc["exceedance_percent"]
    y1 = ldc["residential"]
    y2 = ldc["commercial"]

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.stackplot(x, y1, y2, labels=["Residential", "Commercial"], alpha=0.8)
    ax.set_title(title)
    ax.set_xlabel("Exceedance (%)")
    ax.set_ylabel("Load (kWh)")
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
