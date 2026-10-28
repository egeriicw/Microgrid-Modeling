"""I/O helpers for the microgrid profile pipeline.

This module centralizes small, testable functions for reading and writing the
pipeline's primary file formats:

- Excel spreadsheets for building characteristics.
- Parquet files for per-building hourly timeseries.
- CSV/TXT/XLSX outputs.

Keeping file access behind these functions makes it easier to add caching,
parallelism, or alternate backends later.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable

import pandas as pd


def read_characteristics_xlsx(path: Path) -> pd.DataFrame:
    """Read a building-characteristics spreadsheet.

    Args:
        path: Path to the Excel file.

    Returns:
        A :class:`pandas.DataFrame` containing the characteristics data.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
    """

    if not path.exists():
        raise FileNotFoundError(f"Characteristics file not found: {path}")
    return pd.read_excel(path)


def read_building_parquet(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    """Read a single building parquet file.

    Args:
        path: Path to the parquet file.
        columns: Optional list of columns to load.

    Returns:
        A :class:`pandas.DataFrame` containing the building timeseries.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
    """

    if not path.exists():
        raise FileNotFoundError(f"Timeseries parquet not found: {path}")
    return pd.read_parquet(path, columns=columns)


def read_many_building_parquets(paths: Iterable[Path]) -> pd.DataFrame:
    """Read and concatenate multiple building parquet files.

    Args:
        paths: Iterable of parquet file paths.

    Returns:
        A concatenated :class:`pandas.DataFrame`, or an empty DataFrame if
        ``paths`` is empty.

    Raises:
        FileNotFoundError: If any parquet file path does not exist.
    """

    dfs = [read_building_parquet(p) for p in paths]
    return pd.concat(dfs, axis=0) if dfs else pd.DataFrame()


def read_many_building_parquets_cached(
    paths: Iterable[Path],
    max_workers: int = 8,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Read many building parquet files with de-duplication and parallelism.

    The input list can contain duplicate paths (e.g., when sampling with
    replacement). This function reads each unique file only once and then
    reconstructs the original order.

    Args:
        paths: Iterable of parquet file paths.
        max_workers: Maximum number of worker threads.
        columns: Optional list of columns to load.

    Returns:
        Concatenated DataFrame in the same building order as ``paths``.

    Raises:
        FileNotFoundError: If any parquet file path does not exist.
    """

    path_list = list(paths)
    if not path_list:
        return pd.DataFrame()

    # Preserve original order while dropping duplicates.
    unique_paths = list(dict.fromkeys(path_list))

    cache: dict[Path, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(read_building_parquet, p, columns): p for p in unique_paths}
        for fut in as_completed(futs):
            cache[futs[fut]] = fut.result()

    dfs = [cache[p] for p in path_list]
    return pd.concat(dfs, axis=0) if dfs else pd.DataFrame()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV, creating parent directories if needed."""

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=True)


def write_txt(text: str, path: Path) -> None:
    """Write a UTF-8 text file, creating parent directories if needed."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_xlsx(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    """Write multiple DataFrames to a single Excel workbook.

    Args:
        path: Output path for the workbook.
        sheets: Mapping of ``sheet_name`` to DataFrame.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            df.to_excel(w, sheet_name=name, index=True)
