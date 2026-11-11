"""Dataset identity, catalog, manifest and characteristics dictionary (FR-DATA-1, DATA-7, DATA-8).

Imports nothing else from microgrid (see tests/test_architecture.py). Raw Parquet stays on disk.
"""

from microgrid.datasets.catalog import (
    DATA_ROOT_ENV,
    Catalog,
    CatalogEntry,
    CatalogError,
    load_catalog,
    parse_catalog,
    resolve_data_root,
)
from microgrid.datasets.identity import DatasetId

__all__ = [
    "DATA_ROOT_ENV",
    "Catalog",
    "CatalogEntry",
    "CatalogError",
    "DatasetId",
    "load_catalog",
    "parse_catalog",
    "resolve_data_root",
]
