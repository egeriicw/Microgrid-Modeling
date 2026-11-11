"""Fill ``manifest_sha256`` and ``size_bytes`` for one catalog entry, writing a NEW catalog file (FR-DATA-8).

The input catalog is never modified. The output is a text copy of the input (comments preserved) with the two fields
set on the chosen entry; the result is re-validated with the strict catalog parser before it is left on disk.

``manifest_sha256`` is the sha256 of the manifest FILE you pass (a manifest covers a whole data root, so it is the
fingerprint of the data root snapshot that contained this dataset). ``size_bytes`` is the sum of the sizes of the
manifest files under the entry's ``relative_path``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from microgrid.datasets.catalog import CatalogError, load_catalog, parse_catalog
from microgrid.datasets.manifest import read_manifest, sha256_file

_FIELDS = ("manifest_sha256", "size_bytes")


def _blocks(lines: list[str]) -> list[tuple[int, int]]:
    """(start, end) line ranges of each ``  - `` list item under ``datasets:``."""
    starts = [i for i, ln in enumerate(lines) if ln.startswith("  - ")]
    return [(s, starts[k + 1] if k + 1 < len(starts) else len(lines)) for k, s in enumerate(starts)]


def _set_fields(block: list[str], values: dict[str, str]) -> list[str]:
    kept = [ln for ln in block if not any(ln.strip().startswith(f + ":") for f in _FIELDS)]
    had_trailing_blank = bool(block) and not block[-1].strip()
    while kept and not kept[-1].strip():
        kept.pop()
    return kept + [f"    {k}: {v}" for k, v in values.items()] + ([""] if had_trailing_blank else [])


def fill_catalog(catalog_path: Path, manifest_path: Path, key: str, out_path: Path) -> Path:
    catalog_path, manifest_path, out_path = Path(catalog_path), Path(manifest_path), Path(out_path)
    if out_path.resolve() == catalog_path.resolve():
        raise CatalogError(f"refusing to write {out_path} in place; choose a new output file")
    if out_path.exists() or out_path.is_symlink():
        raise CatalogError(f"output {out_path} already exists; choose a new file name (nothing is overwritten)")
    cat = load_catalog(catalog_path)
    try:
        entry = next(e for e in cat.entries if str(e.dataset_id) == key)
    except StopIteration:
        raise CatalogError(
            f"dataset key {key!r} is not in catalog {catalog_path}; known: {[str(e.dataset_id) for e in cat.entries]}"
        ) from None
    manifest = read_manifest(manifest_path)
    prefix = entry.relative_path.rstrip("/") + "/"
    size = sum(f.size for rel, f in manifest.files.items() if rel.startswith(prefix))
    if size == 0:
        raise CatalogError(f"manifest {manifest_path} lists no files under {entry.relative_path}; not filling {key}")
    values = {"manifest_sha256": sha256_file(manifest_path), "size_bytes": str(size)}

    lines = catalog_path.read_text().splitlines()
    blocks = _blocks(lines)
    index = cat.entries.index(entry)
    if len(blocks) != len(cat.entries):  # parser and text disagree: fail loudly rather than guess
        raise CatalogError(f"could not locate the text of entry {key} in {catalog_path}")
    start, end = blocks[index]
    new_lines = lines[:start] + _set_fields(lines[start:end], values) + lines[end:]
    text = "\n".join(new_lines).rstrip("\n") + "\n"
    parse_catalog(yaml.safe_load(text))  # re-validate before writing anything
    with open(out_path, "x") as fh:  # exclusive create: never overwrites
        fh.write(text)
    return out_path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Write a NEW catalog file with manifest_sha256 and size_bytes filled in.")
    ap.add_argument("--catalog", required=True, type=Path)
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--key", required=True, help="dataset key: product/release/weather/scenario_key/state")
    ap.add_argument("--out", required=True, type=Path, help="NEW catalog file to write (must not exist)")
    args = ap.parse_args(argv)
    try:
        out = fill_catalog(args.catalog, args.manifest, args.key, args.out)
    except (CatalogError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"wrote {out}; review it, then copy it over configs/datasets/catalog.yaml yourself")
    return 0
