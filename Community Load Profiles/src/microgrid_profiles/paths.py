"""Path resolution utilities.

The pipeline uses templated file names for inputs (state / upgrade number) and
writes all outputs into a run-specific directory.

This module converts a :class:`~microgrid_profiles.config.ScenarioConfig` into
concrete :class:`pathlib.Path` instances.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import ScenarioConfig


@dataclass(frozen=True)
class ResolvedPaths:
    """Concrete filesystem paths used by a pipeline run."""

    resstock_characteristics_xlsx: Path
    comstock_characteristics_xlsx: Path
    resstock_timeseries_dir: Path
    comstock_timeseries_dir: Path
    scenario_output_dir: Path


def resolve_paths(cfg: ScenarioConfig, run_id: str) -> ResolvedPaths:
    """Resolve input and output paths for a run.

    Args:
        cfg: Scenario configuration.
        run_id: Run identifier used to create a unique output directory.

    Returns:
        A :class:`ResolvedPaths` object.
    """

    def fmt(s: str) -> str:
        """Format a path template using scenario fields."""

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


def ensure_dir(path: Path) -> None:
    """Create a directory (and parents) if it doesn't exist."""

    path.mkdir(parents=True, exist_ok=True)
