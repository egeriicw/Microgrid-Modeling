"""CI must actually gate merges (FR-PLAT-5, Sprint 1 story 1.2)."""

from __future__ import annotations

import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
PYPROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text())


def _run_lines(job: str) -> list[str]:
    return [s["run"] for s in WORKFLOW["jobs"][job]["steps"] if "run" in s]


def test_lint_job_blocks_on_failure():
    lines = _run_lines("lint")
    assert any("ruff check" in line for line in lines)
    assert any("ruff format --check" in line for line in lines)
    assert not any("|| true" in line for line in lines), "lint failures must fail the build"


def test_type_check_runs_and_blocks():
    lines = _run_lines("lint")
    assert any("mypy" in line for line in lines)
    assert not any("|| true" in line for line in lines)


def test_engine_job_installs_the_package_and_measures_coverage():
    lines = " ".join(_run_lines("engine"))
    assert "pip install -e" in lines or "uv sync" in lines
    assert "PYTHONPATH" not in lines, "tests must pass on the installed package, not a path hack"
    assert "--cov" in lines


def test_tool_configuration_lives_in_pyproject():
    tools = PYPROJECT["tool"]
    assert "ruff" in tools and "mypy" in tools
