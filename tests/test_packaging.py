"""Packaging contract (FR-PLAT-3, Sprint 1 story 1.1).

The library must install cleanly, import without path hacks, expose one semver
version, declare only the dependencies it actually uses, and pin its public surface.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text())

# Names that must NOT be runtime dependencies of the library.
NOT_RUNTIME = {
    "pytest",
    "pytest-cov",
    "ipykernel",
    "dataclasses",  # dev/notebook tooling or stdlib backport
    "boto3",
    "pvlib",
    "xlsxwriter",
    "matplotlib",
    "scipy",  # not imported anywhere in src/ (add back when used)
}

EXPECTED_PUBLIC_SURFACE = {
    "config",
    "rng",
    "archetype",
    "loader",
    "composition",
    "aggregation",
    "perturbation",
    "feasibility",
    "ensemble",
    "validation",
    "outputs",
    "run",
}


def _names(requirements: list[str]) -> set[str]:
    return {re.split(r"[<>=!~\[; ]", r, maxsplit=1)[0].lower().replace("_", "-") for r in requirements}


def test_runtime_dependencies_are_only_what_src_uses():
    runtime = _names(PYPROJECT["project"]["dependencies"])
    assert not (runtime & NOT_RUNTIME), f"remove from runtime deps: {sorted(runtime & NOT_RUNTIME)}"
    # what the engine really imports or needs at runtime
    assert {"numpy", "pandas", "pyarrow", "pyyaml", "openpyxl"} <= runtime


def test_dev_and_notebook_extras_exist():
    extras = PYPROJECT["project"]["optional-dependencies"]
    assert {"pytest", "pytest-cov", "ruff", "mypy", "pandas-stubs", "types-pyyaml"} <= _names(extras["dev"])
    assert {"ipykernel", "matplotlib"} <= _names(extras["notebooks"])


def test_python_version_pin_satisfies_requires_python():
    pinned = (ROOT / ".python-version").read_text().strip()
    major, minor = (int(x) for x in pinned.split(".")[:2])
    m = re.fullmatch(r">=\s*(\d+)\.(\d+)", PYPROJECT["project"]["requires-python"].strip())
    assert m, "requires-python must be a simple lower bound like '>=3.12'"
    assert (major, minor) >= (int(m.group(1)), int(m.group(2)))
    # tested floor is documented in the PRD: 3.12+
    assert (int(m.group(1)), int(m.group(2))) >= (3, 12)


def test_version_has_a_single_source_and_is_semver():
    import microgrid
    import microgrid.community_load.synthetic as synthetic

    assert re.fullmatch(r"\d+\.\d+\.\d+([-+].*)?", microgrid.__version__)
    assert PYPROJECT["project"]["version"] == microgrid.__version__
    assert synthetic.__version__ == microgrid.__version__


def test_import_works_in_a_clean_interpreter_without_pythonpath(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    result = subprocess.run(
        [sys.executable, "-I", "-c", "import microgrid.community_load.synthetic as s; print(s.__version__)"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert re.fullmatch(r"\d+\.\d+\.\d+.*", result.stdout.strip())


def test_public_surface_is_pinned_and_importable():
    import microgrid.community_load.synthetic as synthetic

    assert set(synthetic.__all__) == EXPECTED_PUBLIC_SURFACE
    for name in synthetic.__all__:
        assert hasattr(synthetic, name), f"synthetic.{name} missing"


def test_run_facade_is_top_level():
    from microgrid.community_load.synthetic.run import run_scenario  # noqa: F401
