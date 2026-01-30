"""Command-line interface for generating community microgrid load profiles.

The CLI loads a YAML configuration file (see :func:`microgrid_profiles.config.load_config`)
then applies optional command-line overrides before running the pipeline.

This module is intended to be used as a console-script entry point.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from .config import (
    apply_performance_overrides,
    apply_profiles_overrides,
    apply_weather_overrides,
    load_config,
)
from .pipeline import run_pipeline


def main(argv: list[str] | None = None) -> None:
    """Generate community load profiles for a scenario.

    Args:
        argv: Optional argument list to parse instead of :data:`sys.argv`.

    Raises:
        FileNotFoundError: If the config file path (or referenced inputs) do not exist.
        ValueError: If the configuration is invalid.
    """

    p = argparse.ArgumentParser(description="Generate community microgrid load profiles.")
    p.add_argument("--config", required=True, help="Path to a YAML scenario config file.")

    # Performance / I/O.
    p.add_argument("--fast-io", action="store_true", help="Read building parquet files in parallel.")
    p.add_argument("--max-workers", type=int, default=None, help="Max worker threads for parallel I/O.")
    p.add_argument(
        "--no-prune-parquet-columns",
        action="store_true",
        help="Read all parquet columns (slower / higher memory).",
    )

    # Weather.
    p.add_argument("--enable-weather", action="store_true", help="Join weather data to hourly outputs.")
    p.add_argument(
        "--weather-file",
        action="append",
        default=None,
        help="Path to a weather CSV. Repeat to provide multiple sources.",
    )
    p.add_argument("--weather-units", default=None, help="Preferred weather units: 'C' or 'F'.")

    # Profiles.
    p.add_argument("--profiles", dest="profiles_enabled", action="store_true", help="Enable profile outputs.")
    p.add_argument(
        "--no-profiles",
        dest="profiles_enabled",
        action="store_false",
        help="Disable profile outputs.",
    )
    p.set_defaults(profiles_enabled=None)

    args = p.parse_args(argv)
    cfg = load_config(Path(args.config))

    if args.fast_io:
        cfg = apply_performance_overrides(cfg, fast_io=True)
    if args.max_workers is not None:
        cfg = apply_performance_overrides(cfg, max_workers=int(args.max_workers))
    if args.no_prune_parquet_columns:
        cfg = apply_performance_overrides(cfg, prune_parquet_columns=False)

    if args.enable_weather:
        files = args.weather_file or cfg.weather.files
        units = args.weather_units or cfg.weather.preferred_units
        cfg = apply_weather_overrides(cfg, enabled=True, files=files, preferred_units=units)

    if args.profiles_enabled is not None:
        cfg = apply_profiles_overrides(cfg, write_typical_day_by_month=bool(args.profiles_enabled))

    out_dir = run_pipeline(cfg)
    print(str(out_dir))
