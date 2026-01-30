"""Microgrid load-profile generation package.

The package contains utilities to:

- Load and validate scenario configuration (:mod:`microgrid_profiles.config`).
- Select residential/commercial buildings for a synthetic neighborhood
  (:mod:`microgrid_profiles.selection`).
- Transform timeseries data into hourly/daily/monthly aggregates
  (:mod:`microgrid_profiles.transforms`).
- Run the full end-to-end pipeline and write outputs
  (:func:`microgrid_profiles.pipeline.run_pipeline`).
"""

from __future__ import annotations

__all__ = ["config", "pipeline"]
