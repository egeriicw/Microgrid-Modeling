"""Script entry point for running the microgrid profile pipeline.

This module is intentionally lightweight so it can be used as a runnable script
(e.g., from a shell or batch job) without requiring the package to be installed.

It adds the project ``src/`` directory to ``sys.path`` so imports resolve when
executed in-place.

Notes:
    Prefer using :mod:`microgrid_profiles.cli` for the primary command-line
    interface when the package is installed.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from microgrid_profiles.config import load_config
from microgrid_profiles.pipeline import run_pipeline


def main(argv: list[str] | None = None) -> None:
    """Run the pipeline from a config file.

    Args:
        argv: Optional argument list to parse instead of :data:`sys.argv`.

    Raises:
        FileNotFoundError: If the config path does not exist.
        ValueError: If the config file is missing required keys.
    """

    ap = argparse.ArgumentParser(description="Run the microgrid profile pipeline.")
    ap.add_argument("--config", required=True, help="Path to a YAML scenario config file.")
    args = ap.parse_args(argv)

    cfg = load_config(Path(args.config))
    print(run_pipeline(cfg))


if __name__ == "__main__":
    main()
