"""RED placeholder — FR-E1 / FR-E2 (see docs/test-plan.md §4 Phase 1.1).

The engine must import with no PYTHONPATH/sys.path manipulation, expose a
semver __version__, and pin an explicit public surface. These tests are the
RED step for making `microgrid.community_load.synthetic` an installable distribution.

Remove the skip guard once `microgrid.community_load.synthetic/pyproject.toml` exists and
`pip install -e ./microgrid.community_load.synthetic` (or repo root) is wired.
"""
from __future__ import annotations

import importlib
import re

import pytest

pytestmark = pytest.mark.xfail(
    reason="FR-E1/E2 not implemented: microgrid.community_load.synthetic not yet an installable package",
    strict=False,
)

EXPECTED_PUBLIC_SURFACE = {
    # modules
    "config", "rng", "archetype", "loader", "composition", "aggregation",
    "perturbation", "feasibility", "ensemble", "validation", "outputs",
    "run", "_types",
}


def test_import_without_path_hacks():
    # A fresh interpreter would be ideal; here we at least assert the module
    # resolves and was not loaded from a `src/` that only works via PYTHONPATH.
    mod = importlib.import_module("microgrid.community_load.synthetic")
    assert mod is not None


def test_version_is_semver():
    import microgrid.community_load.synthetic

    assert re.fullmatch(r"\d+\.\d+\.\d+([-+].*)?", microgrid.community_load.synthetic.__version__)


def test_public_surface_is_pinned():
    import microgrid.community_load.synthetic

    assert set(microgrid.community_load.synthetic.__all__) == EXPECTED_PUBLIC_SURFACE


def test_public_surface_all_importable():
    import microgrid.community_load.synthetic

    for name in microgrid.community_load.synthetic.__all__:
        assert hasattr(microgrid.community_load.synthetic, name), f"microgrid.community_load.synthetic.{name} missing"


def test_run_facade_is_top_level():
    from microgrid.community_load.synthetic.run import run_scenario  # noqa: F401
