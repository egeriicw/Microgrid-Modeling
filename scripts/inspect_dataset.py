"""Inspect a real ResStock/ComStock data root (read only) and write a report to paste back.

    uv run --extra dev python scripts/inspect_dataset.py --data-root /path/to/data --out ~/inspect-report.md \
        --json ~/inspect-report.json [--sample-buildings 5] [--hash (needs --out)]

See docs/runbooks/inspect-my-data.md. Never modifies the data root.
"""

from __future__ import annotations

import sys

from microgrid.datasets.inspect import main


def _registry_names() -> dict[str, tuple[str, ...]] | None:
    """Building-type names the registry expects, so the report can flag discrepancies. None if unavailable."""
    try:
        from microgrid.community_load.registry import default_registry

        types = default_registry().active_types()
    except Exception as exc:  # noqa: BLE001 - the inspector still works without the registry check
        print(f"[inspect] registry names unavailable, skipping the registry check: {exc}", file=sys.stderr)
        return None
    return {
        "resstock": tuple(sorted({n for t in types for n in t.resstock_building_type_heights})),
        "comstock": tuple(sorted({n for t in types for n in t.comstock_building_types})),
    }


if __name__ == "__main__":
    raise SystemExit(main(expected_types=_registry_names()))
