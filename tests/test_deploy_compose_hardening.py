"""Independent QA hardening checks for the local stack (story 1.7). Static only; no Docker daemon needed.

These close gaps found by mutating the compose file (see the QA review): disabled healthchecks, extra writable
bind mounts of data, secret defaults, privilege escalation, Dockerfile ADD of data, destructive make targets, and
drift between the compose environment and the API package's settings.
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
COMPOSE_TEXT = (DEPLOY / "docker-compose.yml").read_text()
COMPOSE = yaml.safe_load(COMPOSE_TEXT)
CI = yaml.safe_load((DEPLOY / "docker-compose.ci.yml").read_text())
SERVICES = COMPOSE["services"]


def _binds(services: dict):
    """Yield (service, volume string) for every host bind mount (source is a path or an env-driven path)."""
    for name, svc in services.items():
        for vol in svc.get("volumes", []):
            source = str(vol).split(":", 1)[0]
            if source.startswith((".", "/", "~", "$")):
                yield name, str(vol)


def test_healthchecks_are_real_not_disabled():
    for name, svc in SERVICES.items():
        hc = svc["healthcheck"]
        assert hc.get("disable") is not True, f"{name} healthcheck disabled"
        assert hc["test"][0] in ("CMD", "CMD-SHELL"), f"{name} healthcheck test must be CMD or CMD-SHELL, not NONE"
        assert len(hc["test"]) > 1, f"{name} healthcheck has no command"


def test_every_host_bind_mount_is_read_only():
    """The owner's data (and any other host path) may only ever be mounted read-only, in every service and overlay."""
    for services in (SERVICES, CI["services"]):
        for name, vol in _binds(services):
            assert vol.endswith(":ro"), f"{name} has a writable host bind mount: {vol}"


def test_nothing_mounts_data_outside_the_two_engine_services():
    for name, vol in _binds(SERVICES):
        if "data" in vol.lower():
            assert name in ("api", "worker"), f"{name} must not mount data: {vol}"
            assert vol.endswith(":/data:ro")


def test_required_secrets_have_no_default():
    """`${VAR:?msg}` fails loudly when unset; `${VAR:-x}` would silently run with a guessable secret."""
    for var in ("POSTGRES_PASSWORD", "JWT_SECRET"):
        uses = re.findall(r"\$\{" + var + r"([^}]*)\}", COMPOSE_TEXT)
        assert uses, f"{var} not used"
        for rest in uses:
            assert rest.startswith(":?"), f"{var} must use ${{{var}:?...}}, found '{rest}'"
    assert not re.search(r"\$\{\w*(PASSWORD|SECRET|TOKEN|KEY)\w*:-", COMPOSE_TEXT), "secret with a default value"


def test_no_privilege_escalation_in_compose():
    for name, svc in SERVICES.items():
        assert not svc.get("privileged"), f"{name} privileged"
        assert "cap_add" not in svc, f"{name} adds capabilities"
        assert svc.get("network_mode") != "host", f"{name} host network"
        assert not svc.get("pid") and not svc.get("ipc"), f"{name} shares host namespaces"
    for name in ("api", "worker"):
        assert SERVICES[name]["cap_drop"] == ["ALL"]
        assert "no-new-privileges:true" in SERVICES[name]["security_opt"]
    assert "docker.sock" not in COMPOSE_TEXT


def test_dockerfile_copies_only_an_allowlist_and_never_add():
    text = (DEPLOY / "api.Dockerfile").read_text()
    assert not re.search(r"^ADD\b", text, re.M), "ADD can fetch URLs and unpack archives; use COPY"
    sources = set()
    for m in re.finditer(r"^COPY\s+(?:--\S+\s+)*(.+?)\s+\S+\s*$", text, re.M):
        sources.update(m.group(1).split())
    assert sources <= {"pyproject.toml", "README.md", "src", "api", "deploy/worker-entrypoint.sh"}, sources
    assert "data" not in sources


def _ignored_top_level() -> set[str]:
    return {
        line.strip().rstrip("/")
        for line in (ROOT / ".dockerignore").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }


def test_build_context_keeps_what_pip_install_needs():
    """Root pyproject declares readme=README.md and ships src/**/*.yaml; the context must not drop them."""
    ignored = _ignored_top_level()
    for needed in ("pyproject.toml", "README.md", "src", "api", "deploy"):
        assert needed not in ignored, f".dockerignore drops {needed}, pip install would fail"
    assert not any(p.startswith("*.") and p.endswith(("yaml", "yml", "toml", "md")) for p in ignored)
    assert (ROOT / "README.md").exists()
    assert 'readme = "README.md"' in (ROOT / "pyproject.toml").read_text()


def test_no_directory_named_data_under_packages():
    """`**/data` in .dockerignore silently strips any nested data/ dir (e.g. package data) from the image."""
    assert "**/data" in _ignored_top_level()
    for pkg in ("src", "api/src"):
        base = ROOT / pkg
        if base.exists():
            hits = [p for p in base.rglob("data") if p.is_dir()]
            assert not hits, f"{hits} would be excluded from the image by **/data"


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
@pytest.mark.parametrize("answer", ["", "no", "delete", "DELET"])
def test_make_nuke_aborts_without_exact_confirmation(answer):
    r = subprocess.run(
        ["make", "-C", str(ROOT), "nuke", "COMPOSE=echo WOULD-RUN"],
        input=answer + "\n",
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode != 0
    assert "WOULD-RUN" not in r.stdout, "nuke ran the compose command without confirmation"


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_make_nuke_aborts_with_closed_stdin():
    r = subprocess.run(
        ["make", "-C", str(ROOT), "nuke", "COMPOSE=echo WOULD-RUN"],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode != 0 and "WOULD-RUN" not in r.stdout


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_make_nuke_proceeds_only_on_exact_delete():
    r = subprocess.run(
        ["make", "-C", str(ROOT), "nuke", "COMPOSE=echo WOULD-RUN"],
        input="DELETE\n",
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0 and "WOULD-RUN down --volumes" in r.stdout


DESTRUCTIVE = re.compile(r"down\s+(-v\b|--volumes)|volume\s+(prune|rm)|system\s+prune|rm\s+-r|rmi\b|\brm\s+-f")


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
@pytest.mark.parametrize("target", ["up", "down", "logs", "ps", "config-check", "disk", "prune-help", "smoke"])
def test_only_nuke_can_delete_volumes(target):
    r = subprocess.run(
        ["make", "-C", str(ROOT), "-n", target, "COMPOSE=docker compose"], capture_output=True, text=True, check=False
    )
    executed = [ln for ln in r.stdout.splitlines() if not ln.lstrip().startswith("echo")]
    assert not any(DESTRUCTIVE.search(ln) for ln in executed), executed


def test_smoke_script_and_runbook_destructive_commands_only_as_warnings():
    smoke = (DEPLOY / "tests" / "test_compose_smoke.sh").read_text()
    assert not DESTRUCTIVE.search(smoke)
    assert not re.search(r"\bdown\b", re.sub(r"#.*", "", smoke)), "smoke teardown must be `stop`, not `down`"
    runbook = (ROOT / "docs" / "runbooks" / "local-stack.md").read_text()
    in_code = False
    for line in runbook.splitlines():
        if line.startswith("```"):
            in_code = not in_code
        if in_code:
            assert not DESTRUCTIVE.search(line), f"runbook code block suggests a destructive command: {line}"


def test_compose_env_matches_api_settings_when_api_package_present():
    """Drift guard: once api/src exists, compose must set the env names its Settings actually reads, and the
    uvicorn target must be importable (factory flag when the module only offers create_app)."""
    config = ROOT / "api" / "src" / "microgrid_api" / "config.py"
    if not config.exists():
        pytest.skip("api package not present on this branch")
    prefix = re.search(r'env_prefix="(\w*)"', config.read_text())
    prefix = prefix.group(1) if prefix else ""
    env = SERVICES["api"]["environment"]
    assert f"{prefix}DATABASE_URL" in env, f"api reads {prefix}DATABASE_URL; compose sets {sorted(env)}"
    dockerfile = (DEPLOY / "api.Dockerfile").read_text()
    main = (ROOT / "api" / "src" / "microgrid_api" / "main.py").read_text()
    if "def create_app" in main and "microgrid_api.main:app" in dockerfile:
        assert "def __getattr__" in main, "uvicorn microgrid_api.main:app needs a module-level app"
