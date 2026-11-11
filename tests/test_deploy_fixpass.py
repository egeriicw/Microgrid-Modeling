"""Post-QA fix pass for the local stack (story 1.7): env names, uvicorn target, migrate step, make targets, CI job.

Static only; no Docker daemon needed.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"
COMPOSE = yaml.safe_load((DEPLOY / "docker-compose.yml").read_text())
SERVICES = COMPOSE["services"]
DOCKERFILE = (DEPLOY / "api.Dockerfile").read_text()
RUNBOOK = (ROOT / "docs" / "runbooks" / "local-stack.md").read_text()
CI = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text())

AGREED = {
    "MICROGRID_DATABASE_URL",
    "MICROGRID_REDIS_URL",
    "MICROGRID_JWT_SECRET",
    "MICROGRID_DATA_DIR",
    "MICROGRID_OUTPUT_DIR",
    "MICROGRID_ENVIRONMENT",
}
needs_make = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")


def _make_n(target: str) -> str:
    r = subprocess.run(
        ["make", "-C", str(ROOT), "-n", target, "COMPOSE=docker compose"], capture_output=True, text=True, check=False
    )
    assert r.returncode == 0, r.stderr
    return r.stdout


@pytest.mark.parametrize("name", ["api", "worker", "migrate"])
def test_app_services_use_agreed_prefixed_env_names(name):
    env = SERVICES[name]["environment"]
    assert AGREED <= set(env), f"{name} missing {AGREED - set(env)}"
    for bare in ("DATABASE_URL", "REDIS_URL", "JWT_SECRET", "DATA_DIR", "OUTPUT_DIR"):
        assert bare not in env, f"{name} still sets unprefixed {bare}"
    assert env["MICROGRID_ENVIRONMENT"] == "production"
    assert env["MICROGRID_DATA_DIR"] == "/data" and env["MICROGRID_OUTPUT_DIR"] == "/outputs"


def test_dockerfile_env_lines_are_prefixed():
    joined = " ".join(re.findall(r"^ENV\s+(.+)$", DOCKERFILE, re.M))
    assert "MICROGRID_DATA_DIR=/data" in joined and "MICROGRID_OUTPUT_DIR=/outputs" in joined
    assert not re.search(r"(?<![A-Z_])(DATA_DIR|OUTPUT_DIR)=", joined)


def test_env_example_and_smoke_and_runbook_have_no_bare_names():
    text = (DEPLOY / ".env.example").read_text()
    smoke = (DEPLOY / "tests" / "test_compose_smoke.sh").read_text()
    for bare in ("DATABASE_URL", "REDIS_URL", "OUTPUT_DIR"):
        for body in (text, smoke, RUNBOOK):
            assert not re.search(rf"(?<![A-Z_]){bare}", body), f"unprefixed {bare}"


def test_api_cmd_uses_factory_target():
    section = DOCKERFILE.split("AS api")[1].split("AS worker")[0]
    cmd = re.search(r"^CMD (\[.*\])$", section, re.M).group(1)
    assert yaml.safe_load(cmd) == [
        "uvicorn",
        "--factory",
        "microgrid_api.main:create_app",
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
    ]


def test_migrate_service_is_one_shot_and_gates_api():
    m = SERVICES["migrate"]
    assert m["image"] == SERVICES["api"]["image"] and m["build"]["target"] == "api"
    assert m["command"] == ["python", "-m", "microgrid_api.db.migrate"]
    assert m["restart"] == "no"
    assert set(m["depends_on"]) == {"db"} and m["depends_on"]["db"]["condition"] == "service_healthy"
    assert SERVICES["api"]["depends_on"]["migrate"]["condition"] == "service_completed_successfully"
    assert not any(":/data" in str(v) for v in m.get("volumes", [])), "migrate needs no data mount"
    assert m["cap_drop"] == ["ALL"] and "no-new-privileges:true" in m["security_opt"]


@needs_make
def test_make_up_starts_everything_except_the_placeholder_worker():
    out = _make_n("up")
    line = next(ln for ln in out.splitlines() if " up " in ln)
    assert line.split()[-5:] == ["db", "redis", "web", "migrate", "api"]
    assert "worker" not in line
    make = (ROOT / "Makefile").read_text()
    assert "1.10" in make and "worker" in make


def test_smoke_script_fails_unless_ready_and_runs_migrate():
    smoke = (DEPLOY / "tests" / "test_compose_smoke.sh").read_text()
    assert re.search(r'"ready"\s*:\s*true|ready.*true', smoke), "smoke must check ready is true"
    assert "migrate" in smoke


def test_dockerfile_installs_engine_before_api_with_explanation():
    engine = DOCKERFILE.index("RUN pip install .\n")
    api = DOCKERFILE.index("RUN pip install ./api")
    assert engine < api
    between = DOCKERFILE[engine:api]
    assert "microgrid-planner" in DOCKERFILE and "#" in between and "order" in DOCKERFILE.lower()


@needs_make
def test_image_check_prints_only():
    out = _make_n("image-check")
    executed = [ln for ln in out.splitlines() if not ln.lstrip().startswith(("echo", "make:"))]
    assert executed == [], f"image-check must only echo, found {executed}"
    assert "docker buildx imagetools inspect timescale/timescaledb-ha:pg17" in out


def test_runbook_covers_new_behaviour():
    low = RUNBOOK.lower()
    for phrase in ("make image-check", "migrate", "make smoke", "timescaledb-ha:pg17", "only you can verify", "worker"):
        assert phrase in low, phrase
    assert "ready" in low and "story 1.10" in low


def test_ci_has_compose_static_job_and_keeps_others_disabled():
    job = CI["jobs"]["compose-config"]
    assert "if" not in job
    runs = " ".join(s.get("run", "") for s in job["steps"])
    base = "docker compose --project-directory . --env-file deploy/.env.example -f deploy/docker-compose.yml"
    assert f"{base} config -q" in runs
    assert f"{base} -f deploy/docker-compose.ci.yml config -q" in runs
    builds = [s for s in job["steps"] if "build" in s.get("run", "")]
    assert builds and all(s.get("continue-on-error") is True for s in builds)
    for off in ("api", "web", "compose-smoke"):
        assert CI["jobs"][off]["if"] is False
    for on in ("lint", "engine"):
        assert "if" not in CI["jobs"][on]
