"""Build/verify a manifest of the data root and report disk use (FR-DATA-1).

    uv run python scripts/data_manifest.py build  --data-root /path/to/data
    uv run python scripts/data_manifest.py verify --data-root /path/to/data
    uv run python scripts/data_manifest.py report            # uses env MICROGRID_DATA_DIR

Does not download anything and never deletes files; only writes <data-root>/manifest.json.
"""

from microgrid.datasets.manifest import main

if __name__ == "__main__":
    raise SystemExit(main())
