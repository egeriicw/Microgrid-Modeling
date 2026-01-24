from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from .config import ScenarioConfig

@dataclass(frozen=True)
class ResolvedPaths:
    resstock_characteristics_xlsx: Path
    comstock_characteristics_xlsx: Path
    resstock_timeseries_dir: Path
    comstock_timeseries_dir: Path
    scenario_output_dir: Path

def resolve_paths(cfg: ScenarioConfig, run_id: str) -> ResolvedPaths:
    def fmt(s: str) -> str:
        return s.format(state=cfg.state, upgrade_num=cfg.upgrade_num)
    input_root = cfg.input_root
    output_root = cfg.output_root
    return ResolvedPaths(
        resstock_characteristics_xlsx=input_root / fmt(cfg.resstock_characteristics_xlsx),
        comstock_characteristics_xlsx=input_root / fmt(cfg.comstock_characteristics_xlsx),
        resstock_timeseries_dir=input_root / fmt(cfg.resstock_timeseries_dir),
        comstock_timeseries_dir=input_root / fmt(cfg.comstock_timeseries_dir),
        scenario_output_dir=output_root / "scenario_runs" / run_id,
    )

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)
