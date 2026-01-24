from __future__ import annotations
import argparse
from pathlib import Path
from .config import load_config, apply_performance_overrides, apply_weather_overrides, apply_profiles_overrides
from .pipeline import run_pipeline

def main() -> None:
    p = argparse.ArgumentParser(description="Generate community microgrid load profiles.")
    p.add_argument("--config", required=True)
    p.add_argument("--fast-io", action="store_true")
    p.add_argument("--max-workers", type=int, default=None)
    p.add_argument("--no-prune-parquet-columns", action="store_true")

    p.add_argument("--enable-weather", action="store_true")
    p.add_argument("--weather-file", action="append", default=None)
    p.add_argument("--weather-units", default=None)

    p.add_argument("--profiles", dest="profiles_enabled", action="store_true")
    p.add_argument("--no-profiles", dest="profiles_enabled", action="store_false")
    p.set_defaults(profiles_enabled=None)

    args = p.parse_args()
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
