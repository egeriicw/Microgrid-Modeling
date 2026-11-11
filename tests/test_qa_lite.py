"""QA review tests for the free-disk guard and the lite stack: gaps found by mutation checks and hostile inputs.

Fixtures only: a fake ``df`` and a stub ``docker`` on PATH, both in pytest temp dirs. Nothing touches the real disk or Docker.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy" / "preflight.sh"
DEPLOY = ROOT / "deploy"
needs_make = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
HEADER = "Filesystem 1024-blocks Used Available Capacity Mounted on"
GB_KB = 1024 * 1024


def _exe(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def _fake_df(tmp: Path, avail_kb: str, rc: int = 0) -> Path:
    bindir = tmp / "bin"
    bindir.mkdir(exist_ok=True)
    _exe(bindir / "df", f"#!/bin/sh\ncat <<'EOF'\n{HEADER}\n/dev/d 100 1 {avail_kb} 9% /\nEOF\nexit {rc}\n")
    return bindir


def _preflight(tmp: Path, mode: str, avail_kb: str, env: dict[str, str] | None = None):
    bindir = _fake_df(tmp, avail_kb)
    full_env = {"PATH": f"{bindir}:{os.environ['PATH']}", "HOME": str(tmp)}
    full_env.update(env or {})
    return subprocess.run(
        ["bash", str(SCRIPT), mode], capture_output=True, text=True, env=full_env, cwd=tmp, check=False
    )


@pytest.mark.parametrize(
    ("mode", "need"),
    [("full", 12), ("lite", 5)],
)
def test_threshold_boundary_is_exact_for_each_mode(tmp_path, mode, need):
    assert _preflight(tmp_path, mode, str(need * GB_KB - 1)).returncode != 0
    assert _preflight(tmp_path, mode, str(need * GB_KB)).returncode == 0


def test_preflight_creates_no_files_and_changes_nothing(tmp_path):
    _preflight(tmp_path, "lite", str(100 * GB_KB))
    _preflight(tmp_path, "lite", "1")  # refusal path too
    assert sorted(p.name for p in tmp_path.iterdir()) == ["bin"]  # only the fake df we put there


def test_real_df_on_a_nonexistent_path_refuses():
    r = subprocess.run(
        ["bash", str(SCRIPT), "lite"],
        capture_output=True,
        text=True,
        env={**os.environ, "PREFLIGHT_PATH": "/nonexistent/qa-path"},
        check=False,
    )
    assert r.returncode != 0 and "Refusing" in r.stderr


@needs_make
@pytest.mark.parametrize("target", ["up", "lite-up"])
def test_refused_preflight_aborts_make_before_any_docker_call(tmp_path, target):
    """Real `make` with a stub docker that records calls: a refusal must stop the whole recipe."""
    bindir = _fake_df(tmp_path, "1024")  # 1 MB free
    log = tmp_path / "docker_calls"
    _exe(bindir / "docker", f'#!/bin/sh\necho "$@" >> {log}\n')
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "HOME": str(tmp_path)}
    # -o deploy/.env: pretend the env file is up to date so make does not create it in the repo during the test
    r = subprocess.run(["make", "-C", str(ROOT), "-o", "deploy/.env", target], capture_output=True, text=True, env=env)
    assert r.returncode != 0, r.stdout + r.stderr
    assert not log.exists(), f"docker was invoked after a refused preflight: {log.read_text()}"


@needs_make
def test_passing_preflight_reaches_docker_with_stub(tmp_path):
    bindir = _fake_df(tmp_path, str(100 * GB_KB))
    log = tmp_path / "docker_calls"
    _exe(bindir / "docker", f'#!/bin/sh\necho "$@" >> {log}\n')
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "HOME": str(tmp_path)}
    r = subprocess.run(["make", "-C", str(ROOT), "lite-up"], capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "docker-compose.lite.yml" in log.read_text()


def test_development_mode_is_set_only_in_the_lite_file():
    for path in DEPLOY.glob("*.y*ml"):
        text = path.read_text()
        has_dev = "MICROGRID_ENVIRONMENT: development" in text or "MICROGRID_ENVIRONMENT=development" in text
        assert has_dev == (path.name == "docker-compose.lite.yml"), path.name
    for path in (ROOT / "Makefile", *DEPLOY.glob(".env*")):
        assert "MICROGRID_ENVIRONMENT" not in path.read_text(), path.name


def test_lite_cannot_reach_other_networks_volumes_or_host_namespaces():
    lite = yaml.safe_load((DEPLOY / "docker-compose.lite.yml").read_text())
    for name, svc in lite["services"].items():
        for banned in ("network_mode", "pid", "ipc", "cap_add", "devices", "extra_hosts", "volumes_from"):
            assert banned not in svc, (name, banned)
        for vol in svc.get("volumes", []):
            assert "docker.sock" not in str(vol), name
    for vname, vdef in lite["volumes"].items():
        assert not (vdef and (vdef.get("external") or vdef.get("name"))), (
            f"{vname} must not point at another project's volume"
        )


@needs_make
def test_make_does_not_ignore_a_preflight_failure():
    for target in ("up", "lite-up"):
        out = subprocess.run(["make", "-C", str(ROOT), "-n", target], capture_output=True, text=True, check=True).stdout
        raw = (ROOT / "Makefile").read_text()
        assert out
        assert "\t-./deploy/preflight.sh" not in raw and "\t@-./deploy/preflight.sh" not in raw


# --- fail-OPEN defects found by hostile inputs (QA-1..3), fixed in the fail-closed pass. ---


@pytest.mark.parametrize("val", ["08", "09"])
def test_leading_zero_override_must_not_pass_with_no_free_space(tmp_path, val):
    r = _preflight(tmp_path, "lite", "1000", env={"PREFLIGHT_MIN_FREE_GB": val})
    assert r.returncode != 0, r.stdout + r.stderr


@pytest.mark.parametrize("val", ["9999999999999", "99999999999999999999"])
def test_absurdly_large_override_must_refuse(tmp_path, val):
    r = _preflight(tmp_path, "lite", str(100 * GB_KB), env={"PREFLIGHT_MIN_FREE_GB": val})
    assert r.returncode != 0, r.stdout + r.stderr


def test_absurdly_large_available_value_must_not_pass_on_an_arithmetic_error(tmp_path):
    r = _preflight(tmp_path, "lite", "99999999999999999999999")
    assert "integer expression expected" not in r.stderr and "value too great" not in r.stderr
