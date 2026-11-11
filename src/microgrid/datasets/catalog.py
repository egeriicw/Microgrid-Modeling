"""Dataset catalog loaded from YAML (FR-DATA-8). Strict: unknown or missing keys, duplicates and bad values fail loudly.

Adding a scenario (an extra upgrade) means adding a catalog row; no code changes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from microgrid.datasets.identity import PRODUCTS, DatasetId

DATA_ROOT_ENV = "MICROGRID_DATA_DIR"
CATALOG_VERSION = 1
LOAD_BASES = ("gross", "net")

_REQUIRED = (
    "product",
    "release",
    "weather",
    "state",
    "scenario_key",
    "partition_id",
    "name",
    "scenario_family",
    "load_basis",
    "license",
    "relative_path",
)
_OPTIONAL = ("verified", "notes", "manifest_sha256", "size_bytes")
_TOP_KEYS = ("version", "datasets")


class CatalogError(ValueError):
    """Raised for any invalid catalog content or lookup."""


@dataclass(frozen=True)
class CatalogEntry:
    product: str
    release: str
    weather: str
    state: str
    scenario_key: str
    partition_id: str  # the on-disk upgrade=<partition_id> value; numeric for ResStock, string for ComStock
    name: str
    scenario_family: str
    load_basis: str  # "gross" demand or "net" of on-site generation; never mix silently
    license: str
    relative_path: str  # partition directory, relative to the data root
    verified: bool = False  # False until release/layout confirmed against the real files and documentation
    notes: str = ""
    manifest_sha256: str | None = None  # FR-DATA-8: sha256 of this dataset's manifest, when recorded
    size_bytes: int | None = None  # FR-DATA-8: total size of the dataset on disk, when recorded

    @property
    def dataset_id(self) -> DatasetId:
        return DatasetId(self.product, self.release, self.weather, self.scenario_key, self.state)

    def resolve(self, data_root: Path) -> Path:
        """Absolute location of this dataset's partition directory under ``data_root``."""
        return Path(data_root) / self.relative_path


@dataclass(frozen=True)
class Catalog:
    entries: tuple[CatalogEntry, ...]

    def get(self, dataset_id: DatasetId) -> CatalogEntry:
        for e in self.entries:
            if e.dataset_id == dataset_id:
                return e
        raise CatalogError(
            f"dataset {dataset_id} is not in catalog; known: {[str(e.dataset_id) for e in self.entries]}"
        )

    def for_state(self, product: str, release: str, weather: str, state: str) -> list[CatalogEntry]:
        return [
            e for e in self.entries if (e.product, e.release, e.weather, e.state) == (product, release, weather, state)
        ]


def resolve_data_root(cli_value: str | None) -> Path:
    """Data root from ``--data-root`` first, else ``MICROGRID_DATA_DIR``. No default: never guess a location."""
    value = cli_value or os.environ.get(DATA_ROOT_ENV)
    if not value:
        raise CatalogError(f"no data root: pass --data-root or set {DATA_ROOT_ENV}")
    return Path(value)


def _text(raw: dict[str, Any], key: str, where: str) -> str:
    v = raw[key]
    if v is None or isinstance(v, bool) or not isinstance(v, str | int) or not str(v).strip():
        raise CatalogError(f"{where}: {key} must be a non-empty string, got {v!r}")
    return str(v).strip()


def _parse_entry(raw: Any, i: int) -> CatalogEntry:
    where = f"datasets[{i}]"
    if not isinstance(raw, dict):
        raise CatalogError(f"{where}: must be a mapping")
    unknown = sorted(set(raw) - set(_REQUIRED) - set(_OPTIONAL))
    if unknown:
        raise CatalogError(f"{where}: unknown key(s) {unknown}")
    missing = [k for k in _REQUIRED if k not in raw]
    if missing:
        raise CatalogError(f"{where}: missing required key(s) {missing}")
    vals = {k: _text(raw, k, where) for k in _REQUIRED}
    if vals["product"] not in PRODUCTS:
        raise CatalogError(f"{where}: product must be one of {PRODUCTS}, got {vals['product']!r}")
    if vals["load_basis"] not in LOAD_BASES:
        raise CatalogError(f"{where}: load_basis must be one of {LOAD_BASES}, got {vals['load_basis']!r}")
    rel = PurePosixPath(vals["relative_path"])
    if rel.is_absolute() or ".." in rel.parts:
        raise CatalogError(f"{where}: relative_path must be relative to the data root without '..', got {rel}")
    verified = raw.get("verified", False)
    if not isinstance(verified, bool):
        raise CatalogError(f"{where}: verified must be true or false")
    digest = raw.get("manifest_sha256")
    if digest is not None and not (
        isinstance(digest, str) and len(digest) == 64 and all(c in "0123456789abcdef" for c in digest.lower())
    ):
        raise CatalogError(f"{where}: manifest_sha256 must be 64 hex characters or null, got {digest!r}")
    size = raw.get("size_bytes")
    if size is not None and (isinstance(size, bool) or not isinstance(size, int) or size < 0):
        raise CatalogError(f"{where}: size_bytes must be a non-negative integer or null, got {size!r}")
    return CatalogEntry(
        **vals,
        verified=verified,
        notes=str(raw.get("notes", "")),
        manifest_sha256=None if digest is None else digest.lower(),
        size_bytes=size,
    )


def parse_catalog(doc: Any) -> Catalog:
    if not isinstance(doc, dict):
        raise CatalogError("catalog must be a mapping with 'version' and 'datasets'")
    unknown = sorted(set(doc) - set(_TOP_KEYS))
    if unknown:
        raise CatalogError(f"catalog: unknown key(s) {unknown}")
    if doc.get("version") != CATALOG_VERSION:
        raise CatalogError(f"catalog version must be {CATALOG_VERSION}, got {doc.get('version')!r}")
    raws = doc.get("datasets")
    if not isinstance(raws, list):
        raise CatalogError("catalog: 'datasets' must be a list")
    entries = [_parse_entry(r, i) for i, r in enumerate(raws)]
    seen_ids: set[DatasetId] = set()
    seen_parts: set[tuple[str, str, str, str, str]] = set()
    for e in entries:
        if e.dataset_id in seen_ids:
            raise CatalogError(f"duplicate dataset id {e.dataset_id}")
        seen_ids.add(e.dataset_id)
        part = (e.product, e.release, e.weather, e.state, e.partition_id)
        if part in seen_parts:
            raise CatalogError(
                f"duplicate partition {e.partition_id!r} for {e.product}/{e.release}/{e.weather}/{e.state}"
            )
        seen_parts.add(part)
    return Catalog(tuple(entries))


def load_catalog(path: Path) -> Catalog:
    """Load and strictly validate a catalog YAML file."""
    try:
        doc = yaml.safe_load(Path(path).read_text())
    except OSError as exc:
        raise CatalogError(f"cannot read catalog {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise CatalogError(f"catalog {path} is not valid YAML: {exc}") from exc
    return parse_catalog(doc)
