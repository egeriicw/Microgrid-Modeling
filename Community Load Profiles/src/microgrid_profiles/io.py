from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable
import pandas as pd

def read_characteristics_xlsx(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Characteristics file not found: {path}")
    return pd.read_excel(path)

def read_building_parquet(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Timeseries parquet not found: {path}")
    return pd.read_parquet(path, columns=columns)

def read_many_building_parquets(paths: Iterable[Path]) -> pd.DataFrame:
    dfs = [read_building_parquet(p) for p in paths]
    return pd.concat(dfs, axis=0) if dfs else pd.DataFrame()

def read_many_building_parquets_cached(paths: Iterable[Path], max_workers: int = 8, columns: list[str] | None = None) -> pd.DataFrame:
    path_list = list(paths)
    if not path_list:
        return pd.DataFrame()
    unique_paths = list(dict.fromkeys(path_list))
    cache: dict[Path, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(read_building_parquet, p, columns): p for p in unique_paths}
        for fut in as_completed(futs):
            cache[futs[fut]] = fut.result()
    dfs = [cache[p] for p in path_list]
    return pd.concat(dfs, axis=0) if dfs else pd.DataFrame()

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=True)

def write_txt(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def write_xlsx(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            df.to_excel(w, sheet_name=name, index=True)
