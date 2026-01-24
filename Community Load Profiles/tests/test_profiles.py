import pandas as pd
from microgrid_profiles.transforms import typical_day_monthly

def test_typical_day_monthly():
    idx = pd.date_range("2020-01-01", periods=24*10, freq="H")
    s = pd.Series(1.0, index=idx)
    long_df, pivot = typical_day_monthly(s, "total")
    assert {"sector","month","hour","mean_kwh"} <= set(long_df.columns)
    assert pivot.index.min() == 0 and pivot.index.max() == 23
