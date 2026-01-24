from __future__ import annotations
import argparse
from pathlib import Path
from src.microgrid_profiles.config import load_config
from src.microgrid_profiles.pipeline import run_pipeline

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = load_config(Path(args.config))
    print(run_pipeline(cfg))

if __name__ == "__main__":
    main()
