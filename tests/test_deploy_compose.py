"""Static checks of the local Docker Compose stack (FR-PLAT-4, FR-DATA-4; Sprint 1 story 1.7).

These parse the YAML only; they need no Docker daemon. Compose is run with ``--project-directory .`` from the repo
root, so relative paths in the compose file are relative to the repo root.
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
COMPOSE_PATH = DEPLOY / "docker-compose.yml"
COMPOSE = yaml.safe_load(COMPOSE_PATH.read_text())
SERVICES = COMPOSE["services"]
REQUIRED = {"db", "redis", "api", "worker", "web", "migrate"}
ONE_SHOT = {"migrate"}  # runs and exits; no healthcheck by design
SECRET_WORDS = ("PASSWORD", "SECRET", "TOKEN", "KEY")


def _ports(svc: dict) -> list[str]:
    return [str(p) for p in svc.get("ports", [])]


def test_required_services_present_and_no_dropped_ones():
    assert set(SERVICES) == REQUIRED
    text = COMPOSE_PATH.read_text().lower()
    for dropped in ("minio", "rq", "nextjs", "next_public"):
        assert not re.search(rf"\b{dropped}\b", text), f"{dropped} was dropped from the MVP stack"


def test_every_service_has_healthcheck():
    for name, svc in SERVICES.items():
        if name not in ONE_SHOT:
            assert svc.get("healthcheck", {}).get("test"), f"{name} lacks a healthcheck"


def test_depends_on_uses_health_conditions():
    for name, svc in SERVICES.items():
        deps = svc.get("depends_on", {})
        assert isinstance(deps, dict) or not deps, f"{name}: use the mapping form with conditions"
        for dep, cond in deps.items():
            expected = "service_completed_successfully" if dep in ONE_SHOT else "service_healthy"
            assert cond.get("condition") == expected, f"{name}->{dep} must wait for {expected}"
    assert set(SERVICES["api"]["depends_on"]) == {"db", "redis", "migrate"}
    assert set(SERVICES["worker"]["depends_on"]) >= {"db", "redis"}


def test_ports_bound_to_loopback_only():
    published = 0
    for name, svc in SERVICES.items():
        for p in _ports(svc):
            published += 1
            assert p.startswith("127.0.0.1:"), f"{name} port {p} must bind 127.0.0.1"
    assert published >= 4


def test_no_inline_secrets():
    for name, svc in SERVICES.items():
        for key, val in (svc.get("environment") or {}).items():
            if any(w in key.upper() for w in SECRET_WORDS):
                assert str(val).startswith("${"), f"{name}.{key} must come from the environment, not be inline"
        # passwords embedded in URLs must also be interpolated
        for key, val in (svc.get("environment") or {}).items():
            collapsed = re.sub(r"\$\{[^}]*\}", "VAR", str(val))  # hide interpolations
            m = re.search(r"://[^:/@]+:([^@]+)@", collapsed)
            if m:
                assert m.group(1) == "VAR", f"{name}.{key} embeds a literal password"


def test_env_example_labels_dev_defaults():
    text = (DEPLOY / ".env.example").read_text()
    assert "DEVELOPMENT ONLY" in text
    for line in text.splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, val = line.split("=", 1)
            if any(w in key.upper() for w in SECRET_WORDS):
                assert "dev" in val.lower(), f"{key} must be an obviously development-only value"
    assert "MICROGRID_DATA_DIR" in text
    assert "minio" not in text.lower() and "S3_" not in text


def test_env_files_are_ignored_by_git_and_docker():
    assert re.search(r"^\.env$", (ROOT / ".gitignore").read_text(), re.M)
    ignore = (ROOT / ".dockerignore").read_text().splitlines()
    assert ".env" in ignore or "**/.env" in ignore


def test_data_mount_is_read_only_and_from_env_var():
    mounts = [
        (n, v)
        for n, s in SERVICES.items()
        if n != "redis"
        for v in s.get("volumes", [])
        if re.search(r":/data(:|$)", str(v))
    ]
    assert {n for n, _ in mounts} >= {"api", "worker"}
    for name, vol in mounts:
        assert vol.startswith("${MICROGRID_DATA_DIR:-./data}:/data:ro"), f"{name} data mount must be env-driven, ro"


def test_named_volumes_for_db_redis_and_outputs():
    vols = COMPOSE["volumes"]
    assert {"pgdata", "redisdata", "outputs"} <= set(vols)
    assert any(str(v).startswith("pgdata:") for v in SERVICES["db"]["volumes"])
    assert any(str(v).startswith("redisdata:") for v in SERVICES["redis"]["volumes"])
    for name in ("api", "worker"):
        assert any(
            str(v).startswith("outputs:/outputs") and not str(v).endswith(":ro") for v in SERVICES[name]["volumes"]
        )


def test_no_bind_mount_of_whole_repo_or_writable_data():
    for name, svc in SERVICES.items():
        for v in svc.get("volumes", []):
            assert not str(v).startswith(("..", ".:", "./:")), f"{name} mounts the repo root: {v}"


def test_images_pinned_not_latest():
    for name, svc in SERVICES.items():
        image = svc.get("image")
        if image:
            assert ":" in image.split("/")[-1], f"{name} image {image} has no tag"
            assert not image.endswith(":latest"), f"{name} image uses latest"
        else:
            assert "build" in svc, f"{name} needs image or build"


def test_db_image_bundles_postgis_and_timescale_on_arm64():
    image = SERVICES["db"]["image"]
    assert image.startswith("timescale/timescaledb-ha:")  # bundles PostGIS; multi-arch [verify]
    assert (DEPLOY / "db" / "init" / "01-extensions.sql").read_text().lower().count("create extension") >= 2


def test_dockerfiles_non_root_slim_no_secrets():
    text = (DEPLOY / "api.Dockerfile").read_text()
    assert re.search(r"^FROM python:3\.12-slim", text, re.M)
    assert re.search(r"^USER (?!root)\S+", text, re.M)
    assert not re.search(r"^(ENV|ARG)\s+\S*(PASSWORD|SECRET|TOKEN|KEY)", text, re.M | re.I)
    assert "COPY data" not in text and "COPY . " not in text
    assert re.search(r"^FROM .* AS worker", text, re.M)
    assert re.search(r"^FROM .* AS api", text, re.M)
    for name in ("api", "worker"):
        assert SERVICES[name]["build"]["target"] == name


def test_dockerignore_excludes_data_and_junk():
    lines = {line.strip().rstrip("/") for line in (ROOT / ".dockerignore").read_text().splitlines()}
    for must in (
        "data",
        ".git",
        ".venv",
        "__pycache__",
        ".env",
        "**/.env",
        ".mypy_cache",
        ".ruff_cache",
        ".pytest_cache",
    ):
        assert must in lines, f".dockerignore must exclude {must}"
    assert any(x in lines for x in ("**/*.parquet", "*.parquet"))


def test_worker_placeholder_fails_loudly(tmp_path):
    script = DEPLOY / "worker-entrypoint.sh"
    assert script.exists()
    env = {"PATH": "/usr/bin:/bin", "PYTHON": "python3", "PYTHONPATH": str(tmp_path)}
    r = subprocess.run(["bash", str(script)], capture_output=True, text=True, env=env, check=False)
    assert r.returncode != 0
    assert "microgrid_api.worker" in r.stderr and "story 1.10" in r.stderr


def test_ci_override_only_tightens_healthchecks_and_uses_fixtures():
    ci = yaml.safe_load((DEPLOY / "docker-compose.ci.yml").read_text())
    assert set(ci["services"]) <= REQUIRED
    text = (DEPLOY / "docker-compose.ci.yml").read_text().lower()
    assert "minio" not in text and "mock" not in text


def test_makefile_targets_and_safe_down():
    make = (ROOT / "Makefile").read_text()
    for target in ("up", "down", "logs", "ps", "config-check", "disk", "prune-help", "nuke"):
        assert re.search(rf"^{target}:", make, re.M), f"missing make target {target}"
    down = re.search(r"^down:.*\n((?:\t.*\n)+)", make, re.M).group(1)
    assert "-v" not in down and "--volumes" not in down
    nuke = re.search(r"^nuke:.*\n((?:\t.*\n)+)", make, re.M).group(1)
    assert "read" in nuke  # asks for confirmation
    prune = re.search(r"^prune-help:.*\n((?:\t.*\n)+)", make, re.M).group(1)
    assert all(line.lstrip("\t").lstrip("@").startswith("echo") for line in prune.splitlines() if line.strip())


def test_smoke_script_and_runbook_exist():
    smoke = (DEPLOY / "tests" / "test_compose_smoke.sh").read_text()
    assert "/api/v1/health" in smoke and "/api/v1/ready" in smoke
    assert "down -v" not in smoke and "--volumes" not in smoke
    runbook = (ROOT / "docs" / "runbooks" / "local-stack.md").read_text()
    for phrase in ("Apple silicon", "docker system df", "Never run", "volume"):
        assert phrase in runbook


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI not installed")
def test_docker_compose_config_validates(tmp_path):
    env = tmp_path / "env"
    env.write_text((DEPLOY / ".env.example").read_text())
    base = ["docker", "compose", "--project-directory", str(ROOT), "--env-file", str(env), "-f", str(COMPOSE_PATH)]
    r = subprocess.run([*base, "config", "-q"], capture_output=True, text=True, check=False)
    if "docker" in r.stderr.lower() and "unknown" in r.stderr.lower():
        pytest.skip("docker compose plugin missing")
    assert r.returncode == 0, r.stderr
    r = subprocess.run(
        [*base, "-f", str(DEPLOY / "docker-compose.ci.yml"), "config", "-q"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, r.stderr
