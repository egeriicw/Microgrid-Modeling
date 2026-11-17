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


# --- W0-CI: the API and web suites must run and block --------------------------------------------------------------


def _enabled_jobs() -> dict[str, dict]:
    return {name: job for name, job in WORKFLOW["jobs"].items() if job.get("if") is not False}


def test_api_and_web_jobs_are_enabled():
    for job in ("api", "web"):
        assert job in WORKFLOW["jobs"], f"{job} job missing"
        assert WORKFLOW["jobs"][job].get("if") is not False, f"{job} job is switched off with `if: false`"


def test_no_enabled_step_swallows_failures():
    for name, job in _enabled_jobs().items():
        assert "continue-on-error" not in job, f"job {name} must not continue on error"
        for step in job["steps"]:
            assert "continue-on-error" not in step, f"a step in {name} has continue-on-error: {step}"
            assert "|| true" not in step.get("run", ""), f"a step in {name} swallows failures: {step['run']}"


def test_web_job_runs_typecheck_lint_test_and_build_on_node_22():
    job = WORKFLOW["jobs"]["web"]
    lines = " ".join(_run_lines("web"))
    for script in ("npm ci", "typecheck", "lint", "test", "build"):
        assert script in lines, f"web job does not run {script}"
    assert "--ci" not in lines, "--ci is a jest flag; the app uses vitest"
    node = [s for s in job["steps"] if str(s.get("uses", "")).startswith("actions/setup-node")]
    assert node and str(node[0]["with"]["node-version"]) == "22"
    assert node[0]["with"].get("cache-dependency-path") == "web/package-lock.json"


def test_api_job_runs_ruff_mypy_and_pytest_with_coverage_on_python_312():
    job = WORKFLOW["jobs"]["api"]
    lines = " ".join(_run_lines("api"))
    for tool in ("ruff check", "ruff format --check", "mypy", "pytest", "--cov=microgrid_api"):
        assert tool in lines, f"api job does not run {tool}"
    assert "alembic upgrade" not in lines, (
        "migrations run via python -m microgrid_api.db.migrate, not alembic from api/"
    )
    assert "microgrid_api.db.migrate" in lines
    py = [s for s in job["steps"] if str(s.get("uses", "")).startswith("actions/setup-python")]
    assert py and str(py[0]["with"]["python-version"]) == "3.12"


def test_api_job_wires_postgres_for_the_opt_in_tests():
    job = WORKFLOW["jobs"]["api"]
    assert "postgres" in job.get("services", {}), "Postgres-only tests would silently skip"
    env = job.get("env", {}) | {k: v for s in job["steps"] for k, v in s.get("env", {}).items()}
    assert "MICROGRID_TEST_DATABASE_URL" in env, "api tests select Postgres via MICROGRID_TEST_DATABASE_URL"


def test_compose_smoke_stays_disabled_until_wave_1():
    assert WORKFLOW["jobs"]["compose-smoke"].get("if") is False
