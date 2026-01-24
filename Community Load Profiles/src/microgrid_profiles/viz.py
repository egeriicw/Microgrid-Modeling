from __future__ import annotations
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

def plot_hourly(df: pd.DataFrame, title: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    df.plot(ax=ax, title=title, legend=True)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
