"""Unit tests for aggregation helpers."""

import pandas as pd

from microgrid_profiles.transforms import agg_daily_sum, agg_monthly_sum


def test_daily_monthly_sum() -> None:
    """Daily and monthly aggregations should sum correctly."""

    idx = pd.date_range("2020-01-01", periods=48, freq="H")
    df = pd.DataFrame({"kwh": 1.0}, index=idx)

    d = agg_daily_sum(df)
    assert d.iloc[0]["kwh"] == 24.0 and d.iloc[1]["kwh"] == 24.0

    m = agg_monthly_sum(df)
    assert m.iloc[0]["kwh"] == 48.0
