"""Write a NEW catalog file with manifest_sha256 and size_bytes filled in for one entry.

    uv run python scripts/catalog_fill.py --catalog configs/datasets/catalog.yaml --manifest manifest.json \
        --key resstock/2025_release_1/amy2018/baseline/DC --out catalog.filled.yaml

Never edits the catalog in place and never overwrites an existing file.
"""

from microgrid.datasets.catalog_fill import main

if __name__ == "__main__":
    raise SystemExit(main())
