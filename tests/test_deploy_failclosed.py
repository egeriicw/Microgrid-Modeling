"""Fail-closed tests for deploy/preflight.sh, `make smoke`, `make lite-nuke` and the .env ordering.

Fixtures only: a fake ``df`` and a stub ``docker`` on PATH, both in pytest temp dirs. Nothing touches real disk or Docker.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy" / "preflight.sh"
GB_KB = 1024 * 1024
HEADER = "Filesystem 1024-blocks Used Available Capacity Mounted on"
needs_make = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")


def _exe(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def _df_lines(tmp: Path, lines: str) -> Path:
    bindir = tmp / "bin"
    bindir.mkdir(exist_ok=True)
    _exe(bindir / "df", f"#!/bin/sh\ncat <<'EOF'\n{HEADER}\n{lines}\nEOF\n")
    return bindir


def _run(tmp: Path, mode: str, lines: str, env: dict[str, str] | None = None):
    bindir = _df_lines(tmp, lines)
    full_env = {"PATH": f"{bindir}:{os.environ['PATH']}", "HOME": str(tmp)}
    full_env.update(env or {})
    return subprocess.run(
        ["bash", str(SCRIPT), mode], capture_output=True, text=True, env=full_env, cwd=tmp, check=False
    )


def _plenty(kb: int = 100 * GB_KB) -> str:
    return f"/dev/d 100 1 {kb} 9% /"


@pytest.mark.parametrize(
    "val",
    ["+5", "5GB", "0x10", "5\n", "5\t", "\t5", "5\n6", " 5", "5 ", "00", "007", "08", "100000", "-0", "1e3", "５"],
)
def test_override_must_match_strict_pattern_or_refuse(tmp_path, val):
    r = _run(tmp_path, "lite", _plenty(), env={"PREFLIGHT_MIN_FREE_GB": val})
    assert r.returncode == 2, (val, r.stdout, r.stderr)
    assert "PREFLIGHT_MIN_FREE_GB" in r.stderr
    assert "syntax error" not in r.stderr and "value too great" not in r.stderr and "OK" not in r.stdout


def test_override_exactly_99999_refuses_on_space_not_on_error(tmp_path):
    r = _run(tmp_path, "lite", _plenty(), env={"PREFLIGHT_MIN_FREE_GB": "99999"})
    assert r.returncode == 1 and "REFUSING" in r.stderr and "99999 GB needed" in r.stderr


def test_override_zero_is_an_announced_explicit_disable(tmp_path):
    r = _run(tmp_path, "full", _plenty(1), env={"PREFLIGHT_MIN_FREE_GB": "0"})
    assert r.returncode == 0
    assert "DISABLED" in r.stderr and "PREFLIGHT_MIN_FREE_GB=0" in r.stderr


def test_override_valid_number_still_compares(tmp_path):
    assert _run(tmp_path, "lite", _plenty(6 * GB_KB), env={"PREFLIGHT_MIN_FREE_GB": "6"}).returncode == 0
    assert _run(tmp_path, "lite", _plenty(6 * GB_KB - 1), env={"PREFLIGHT_MIN_FREE_GB": "6"}).returncode != 0


@pytest.mark.parametrize("avail", ["1" * 16, "9" * 30, "+5", "5e6", "-1", "0x10", "1.5", "08x"])
def test_available_value_not_plain_bounded_digits_refuses(tmp_path, avail):
    r = _run(tmp_path, "lite", f"/dev/d 100 1 {avail} 9% /")
    assert r.returncode != 0 and "OK" not in r.stdout, r.stderr
    assert "integer expression" not in r.stderr and "syntax error" not in r.stderr


def test_available_with_leading_zero_is_read_as_decimal_not_octal(tmp_path):
    # 0000010485760 kb is 10 GB read as decimal; as octal it would be invalid or a different number.
    assert _run(tmp_path, "lite", _plenty(0).replace(" 0 ", " 0010485760 ")).returncode == 0


def test_available_fifteen_digits_is_accepted_and_passes(tmp_path):
    assert _run(tmp_path, "full", _plenty(int("9" * 15))).returncode == 0


def test_df_line_with_extra_columns_and_spaces_in_mount_is_parsed(tmp_path):
    r = _run(tmp_path, "lite", f"/dev/d 100 1 {10 * GB_KB} 9% /Volumes/My Disk extra cols here")
    assert r.returncode == 0, r.stderr


def test_df_with_two_data_lines_is_ambiguous_and_refuses(tmp_path):
    r = _run(tmp_path, "lite", f"{_plenty()}\n{_plenty()}")
    assert r.returncode != 0 and "OK" not in r.stdout


def test_df_with_short_line_refuses(tmp_path):
    assert _run(tmp_path, "lite", "/dev/d 100 1").returncode != 0


# --- make smoke / make lite-nuke / .env ordering, with a stub docker on PATH ---


def _stub_env(tmp: Path, avail_kb: int):
    bindir = _df_lines(tmp, _plenty(avail_kb))
    log = tmp / "docker_calls"
    _exe(bindir / "docker", f'#!/bin/sh\necho "$@" >> {log}\nexit 1\n')  # exit 1: smoke must not carry on regardless
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "HOME": str(tmp)}
    return env, log


def _scratch_repo(tmp: Path) -> Path:
    """A copy of just the files `make` needs, so a missing deploy/.env can be observed without touching the real repo."""
    work = tmp / "repo"
    (work / "deploy" / "tests").mkdir(parents=True)
    for rel in ("Makefile", "deploy/preflight.sh", "deploy/.env.example", "deploy/tests/test_compose_smoke.sh"):
        shutil.copy(ROOT / rel, work / rel)
    return work


@needs_make
def test_make_smoke_runs_preflight_first_and_aborts_before_docker(tmp_path):
    work = _scratch_repo(tmp_path)
    env, log = _stub_env(tmp_path, 1024)
    r = subprocess.run(["make", "-C", str(work), "smoke"], capture_output=True, text=True, env=env)
    assert r.returncode != 0
    assert "REFUSING" in r.stderr
    assert not log.exists(), log.read_text()
    assert not (work / "deploy" / ".env").exists()


@needs_make
def test_smoke_script_directly_runs_preflight_first_and_aborts_before_docker(tmp_path):
    work = _scratch_repo(tmp_path)
    env, log = _stub_env(tmp_path, 1024)
    r = subprocess.run(
        [str(work / "deploy" / "tests" / "test_compose_smoke.sh")], capture_output=True, text=True, env=env
    )
    assert r.returncode != 0 and "REFUSING" in r.stderr
    assert not log.exists(), log.read_text()
    assert not (work / "deploy" / ".env").exists()


@needs_make
def test_smoke_script_with_passing_preflight_reaches_docker(tmp_path):
    work = _scratch_repo(tmp_path)
    env, log = _stub_env(tmp_path, 100 * GB_KB)
    r = subprocess.run(
        [str(work / "deploy" / "tests" / "test_compose_smoke.sh")], capture_output=True, text=True, env=env
    )
    assert "preflight: OK" in r.stdout
    assert log.exists()  # the stub docker fails, so the script fails, but only after the preflight passed
    assert r.returncode != 0


@needs_make
def test_refused_up_leaves_no_env_file(tmp_path):
    work = _scratch_repo(tmp_path)
    env, log = _stub_env(tmp_path, 1024)
    r = subprocess.run(["make", "-C", str(work), "up"], capture_output=True, text=True, env=env)
    assert r.returncode != 0
    assert not (work / "deploy" / ".env").exists(), "a refused run created deploy/.env"
    assert not log.exists()


@needs_make
def test_passing_up_creates_env_file_after_preflight(tmp_path):
    work = _scratch_repo(tmp_path)
    env, log = _stub_env(tmp_path, 100 * GB_KB)
    subprocess.run(["make", "-C", str(work), "up"], capture_output=True, text=True, env=env)
    assert (work / "deploy" / ".env").exists()
    assert log.exists()


@needs_make
@pytest.mark.parametrize("answer", ["", "no", "delete", "DELET"])
def test_make_lite_nuke_aborts_without_exact_confirmation(answer):
    r = subprocess.run(
        ["make", "-C", str(ROOT), "lite-nuke", "LITE=echo WOULD-RUN"],
        input=answer + "\n",
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode != 0
    assert "WOULD-RUN" not in r.stdout


@needs_make
def test_make_lite_nuke_aborts_with_closed_stdin():
    r = subprocess.run(
        ["make", "-C", str(ROOT), "lite-nuke", "LITE=echo WOULD-RUN"],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode != 0 and "WOULD-RUN" not in r.stdout


@needs_make
def test_make_lite_nuke_proceeds_only_on_exact_delete():
    r = subprocess.run(
        ["make", "-C", str(ROOT), "lite-nuke", "LITE=echo WOULD-RUN"],
        input="DELETE\n",
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0 and "WOULD-RUN down --volumes" in r.stdout


@needs_make
def test_lite_nuke_targets_only_the_lite_project_never_the_full_one(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    log = tmp_path / "docker_calls"
    _exe(bindir / "docker", f'#!/bin/sh\necho "$@" >> {log}\n')
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}"}
    r = subprocess.run(
        ["make", "-C", str(ROOT), "lite-nuke"], input="DELETE\n", capture_output=True, text=True, env=env, check=False
    )
    assert r.returncode == 0, r.stderr
    calls = log.read_text().splitlines()
    assert calls and all("docker-compose.lite.yml" in c for c in calls), calls
    assert not any("deploy/docker-compose.yml" in c for c in calls)
    assert "volume" not in " ".join(calls).replace("--volumes", "")  # no `docker volume rm` by name
    assert "name: microgrid-lite" in (ROOT / "deploy" / "docker-compose.lite.yml").read_text()


def test_stale_text_is_fixed():
    sql = (ROOT / "deploy" / "db" / "init" / "01-extensions.sql").read_text()
    assert "Alembic migrations also" not in sql and "only place" in sql
    mk = (ROOT / "Makefile").read_text()
    assert "microgrid-lite_*" in mk and "microgrid_*" in mk
    rb = (ROOT / "docs" / "runbooks" / "local-stack.md").read_text()
    assert "lite-nuke" in rb and "smoke" in rb and "preflight" in rb
    assert "df -h /System/Volumes/Data" in rb and "make -n lite-up" in rb
    assert "runs the free-disk preflight" in rb
