"""Tests for deploy/preflight.sh (free-disk guard) using a fake ``df`` on PATH. Never touches the real disk."""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy" / "preflight.sh"
needs_make = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
GB_KB = 1024 * 1024


def _run(tmp_path: Path, mode: str, df_stdout: str, df_rc: int = 0, env: dict[str, str] | None = None):
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    fake = bindir / "df"
    fake.write_text(f"#!/bin/sh\ncat <<'EOF'\n{df_stdout}\nEOF\nexit {df_rc}\n")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    full_env = {"PATH": f"{bindir}:{os.environ['PATH']}", "HOME": str(tmp_path)}
    full_env.update(env or {})
    return subprocess.run(["bash", str(SCRIPT), mode], capture_output=True, text=True, env=full_env, check=False)


def _df(free_gb: float) -> str:
    avail = int(free_gb * GB_KB)
    return (
        "Filesystem 1024-blocks Used Available Capacity Mounted on\n"
        f"/dev/disk3s5 488245288 400000000 {avail} 90% /System/Volumes/Data"
    )


def _out(r: subprocess.CompletedProcess[str]) -> str:
    return r.stdout + r.stderr


def test_script_exists_and_is_executable():
    assert SCRIPT.exists() and os.access(SCRIPT, os.X_OK)


def test_full_refuses_below_12_gb(tmp_path):
    r = _run(tmp_path, "full", _df(6.7))
    assert r.returncode != 0
    out = _out(r)
    assert "6.7" in out and "12" in out and "make lite-up" in out and "df -h /System/Volumes/Data" in out


def test_full_passes_above_12_gb(tmp_path):
    r = _run(tmp_path, "full", _df(40))
    assert r.returncode == 0, _out(r)


def test_lite_threshold_is_5_gb(tmp_path):
    assert _run(tmp_path, "lite", _df(6.7)).returncode == 0
    r = _run(tmp_path, "lite", _df(4.2))
    assert r.returncode != 0 and "5" in _out(r)


def test_exactly_at_threshold_passes(tmp_path):
    assert _run(tmp_path, "full", _df(12)).returncode == 0


def test_override_is_honoured_and_announced(tmp_path):
    r = _run(tmp_path, "full", _df(6.7), env={"PREFLIGHT_MIN_FREE_GB": "5"})
    assert r.returncode == 0, _out(r)
    assert "PREFLIGHT_MIN_FREE_GB" in _out(r)  # never silent


def test_override_can_make_it_stricter(tmp_path):
    assert _run(tmp_path, "lite", _df(6.7), env={"PREFLIGHT_MIN_FREE_GB": "10"}).returncode != 0


@pytest.mark.parametrize("bad", ["abc", "-3", "1.5", ""])
def test_bad_override_refuses(tmp_path, bad):
    r = _run(tmp_path, "full", _df(40), env={"PREFLIGHT_MIN_FREE_GB": bad})
    assert r.returncode != 0 and "PREFLIGHT_MIN_FREE_GB" in _out(r)


@pytest.mark.parametrize(
    "stdout",
    ["", "garbage", "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/x 1 2 notanumber 3% /"],
)
def test_unparseable_df_refuses(tmp_path, stdout):
    r = _run(tmp_path, "full", stdout)
    assert r.returncode != 0 and "df" in _out(r).lower()


def test_df_failure_refuses(tmp_path):
    assert _run(tmp_path, "lite", _df(100), df_rc=1).returncode != 0


def test_unknown_mode_refuses(tmp_path):
    assert _run(tmp_path, "huge", _df(100)).returncode != 0
    assert _run(tmp_path, "", _df(100)).returncode != 0


def test_script_never_deletes_anything():
    code = "\n".join(ln for ln in SCRIPT.read_text().splitlines() if not ln.lstrip().startswith("#"))
    assert not re.search(r"\b(rm|rmdir|unlink|mkfs|prune|docker|find|truncate|dd)\b|--volumes", code)


@needs_make
@pytest.mark.parametrize(("target", "mode"), [("up", "full"), ("lite-up", "lite")])
def test_make_up_targets_run_preflight_before_docker(target, mode):
    out = subprocess.run(["make", "-C", str(ROOT), "-n", target], capture_output=True, text=True, check=True).stdout
    lines = [ln for ln in out.splitlines() if ln.strip() and not ln.lstrip().startswith("make:")]
    pre = next(i for i, ln in enumerate(lines) if f"preflight.sh {mode}" in ln)
    dock = next(i for i, ln in enumerate(lines) if "docker compose" in ln)
    assert pre < dock
