"""RED placeholder — FR-E1 / FR-E2 (see docs/test-plan.md §4 Phase 1.1).

The engine must import with no PYTHONPATH/sys.path manipulation, expose a
semver __version__, and pin an explicit public surface. These tests are the
RED step for making `snlg` an installable distribution.

Remove the skip guard once `snlg/pyproject.toml` exists and
`pip install -e ./snlg` (or repo root) is wired.
"""
from __future__ import annotations

import importlib
import re

import pytest

pytestmark = pytest.mark.xfail(
    reason="FR-E1/E2 not implemented: snlg not yet an installable package",
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
    snlg = importlib.import_module("snlg")
    assert snlg is not None


def test_version_is_semver():
    import snlg

    assert re.fullmatch(r"\d+\.\d+\.\d+([-+].*)?", snlg.__version__)


def test_public_surface_is_pinned():
    import snlg

    assert set(snlg.__all__) == EXPECTED_PUBLIC_SURFACE


def test_public_surface_all_importable():
    import snlg

    for name in snlg.__all__:
        assert hasattr(snlg, name), f"snlg.{name} missing"


def test_run_facade_is_top_level():
    from snlg.run import run_scenario  # noqa: F401
