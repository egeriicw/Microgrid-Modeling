"""OEDI S3 download helper.

This script reads a small JSON manifest describing which S3 prefixes to sync.
It is designed to be:

- Idempotent: safe to re-run.
- Resumable: uses `aws s3 sync`.
- Explicit: only syncs what you list in the manifest.

The manifest is intentionally kept simple so it can be parsed without extra
Python dependencies.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def _aws_sync(
    src: str,
    dst: Path,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    dry_run: bool = False,
) -> None:
    cmd = ["aws", "s3", "sync", src, str(dst)]

    # Apply excludes first; includes override later.
    if exclude:
        for pat in exclude:
            cmd += ["--exclude", pat]
    if include:
        for pat in include:
            cmd += ["--include", pat]

    if dry_run:
        cmd.append("--dryrun")

    _run(cmd)


def load_manifest(path: Path) -> dict[str, Any]:
    """Load the OEDI manifest JSON."""

    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    ap = argparse.ArgumentParser(description="Download inputs from NREL OEDI (S3).")
    ap.add_argument(
        "--manifest",
        default="../data/sources/oedi_manifest.json",
        help="Path to OEDI manifest JSON.",
    )
    ap.add_argument(
        "--data-dir",
        default=os.environ.get("DATA_DIR", "../data"),
        help="Local data directory root.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print planned sync operations.")

    args = ap.parse_args()

    manifest_path = Path(args.manifest).resolve()
    data_dir = Path(args.data_dir).resolve()

    m = load_manifest(manifest_path)
    bucket = (m.get("bucket") or "").strip()

    if not bucket:
        raise SystemExit(
            f"Manifest bucket is empty. Fill in `bucket` in {manifest_path} (e.g., oedi-data-lake)."
        )

    datasets = m.get("datasets") or {}

    for name, cfg in datasets.items():
        prefix = (cfg.get("prefix") or "").strip()
        if not prefix:
            print(f"Skipping {name}: prefix is empty")
            continue

        include = cfg.get("include") or []
        exclude = cfg.get("exclude") or []

        src = f"s3://{bucket}/{prefix.lstrip('/')}"
        dst = data_dir / "input" / "oedi" / name
        dst.mkdir(parents=True, exist_ok=True)

        print(f"Sync: {src} -> {dst}")
        _aws_sync(src, dst, include=include, exclude=exclude, dry_run=bool(args.dry_run))


if __name__ == "__main__":
    main()
