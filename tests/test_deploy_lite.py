"""Static checks of the `lite` Compose file (small-disk option): api + migrate on SQLite, one shared image.

Parses YAML and runs ``make -n`` only; needs no Docker daemon. Every safety property of the full stack must hold here too.
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
LITE_PATH = DEPLOY / "docker-compose.lite.yml"
LITE_TEXT = LITE_PATH.read_text()
LITE = yaml.safe_load(LITE_TEXT)
FULL_TEXT = (DEPLOY / "docker-compose.yml").read_text()
FULL = yaml.safe_load(FULL_TEXT)
SERVICES = LITE["services"]
needs_make = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")


def _make_n(*args: str) -> str:
    r = subprocess.run(["make", "-C", str(ROOT), "-n", *args], capture_output=True, text=True, check=True)
    return r.stdout


def test_lite_runs_only_migrate_and_api_with_its_own_project_name():
    assert set(SERVICES) == {"migrate", "api"}
    assert LITE["name"] == "microgrid-lite" and LITE["name"] != FULL["name"]  # volumes never mix with the full stack


def test_lite_shares_one_image_with_the_full_stack_api():
    assert SERVICES["api"]["image"] == SERVICES["migrate"]["image"] == FULL["services"]["api"]["image"]
    assert SERVICES["api"]["build"]["target"] == "api"


def test_lite_is_the_only_place_that_uses_development_and_sqlite():
    env = SERVICES["api"]["environment"]
    assert env["MICROGRID_ENVIRONMENT"] == "development"
    assert env["MICROGRID_DATABASE_URL"].startswith("sqlite:////db/")
    assert SERVICES["migrate"]["environment"] == env
    assert FULL["services"]["api"]["environment"]["MICROGRID_ENVIRONMENT"] == "production"
    assert "sqlite" not in FULL_TEXT.lower()
    header = LITE_TEXT.split("services:")[0]
    assert "development" in header.lower() and "NOT" in header, "the file header must explain the dev-mode trade-off"


def test_lite_has_no_secrets_and_needs_no_env_file():
    for name, svc in SERVICES.items():
        for key, val in svc["environment"].items():
            assert not any(w in key.upper() for w in ("PASSWORD", "SECRET", "TOKEN", "KEY")), (name, key)
            assert "@" not in str(val), f"{name}.{key} looks like a credentialed URL"
    assert not re.search(r"\$\{[A-Z_]+:\?", LITE_TEXT), "lite must not require any variable"


def test_lite_ports_loopback_only():
    ports = [str(p) for s in SERVICES.values() for p in s.get("ports", [])]
    assert ports and all(p.startswith("127.0.0.1:") for p in ports)


def test_lite_data_mount_is_read_only_and_only_on_api():
    mounted = 0
    for name, svc in SERVICES.items():
        for vol in svc.get("volumes", []):
            if ":/data" in str(vol):
                mounted += 1
                assert name == "api" and str(vol).endswith(":/data:ro"), vol
            source = str(vol).split(":", 1)[0]
            if source.startswith((".", "/", "~", "$")):
                assert str(vol).endswith(":ro"), f"{name}: host bind {vol} must be read-only"
    assert mounted == 1


def test_lite_database_lives_in_a_named_volume_shared_by_migrate_and_api():
    assert "litedb" in LITE["volumes"]
    for name in ("migrate", "api"):
        assert any(str(v).startswith("litedb:/db") for v in SERVICES[name]["volumes"]), name


def test_lite_hardening_and_healthchecks():
    for name, svc in SERVICES.items():
        assert svc["cap_drop"] == ["ALL"], name
        assert "no-new-privileges:true" in svc["security_opt"], name
        assert svc.get("mem_limit"), f"{name} needs a memory ceiling"
        assert "privileged" not in svc and "user" not in svc, name
    health = SERVICES["api"]["healthcheck"]
    assert health["test"] and health["test"][0] == "CMD"
    assert SERVICES["migrate"]["restart"] == "no"
    assert SERVICES["api"]["depends_on"]["migrate"]["condition"] == "service_completed_successfully"


def test_lite_images_are_built_locally_not_floating_pulls():
    for svc in SERVICES.values():
        assert svc["image"].endswith(":dev") and "build" in svc  # no registry image whose tag could float


def test_dockerfile_prepares_a_writable_db_dir_for_the_non_root_user():
    text = (DEPLOY / "api.Dockerfile").read_text()
    assert re.search(r"mkdir -p [^\n]*/db\b", text) and re.search(r"chown app:app[^\n]*/db\b", text)
    assert "USER app" in text


@needs_make
def test_lite_make_targets_exist_and_use_the_lite_file():
    up = _make_n("lite-up")
    assert "preflight.sh lite" in up
    assert "docker-compose.lite.yml" in up and " up " in up and "--wait" in up
    down = _make_n("lite-down")
    assert "docker-compose.lite.yml" in down and " down" in down
    assert " -v" not in down and "--volumes" not in down


@needs_make
def test_lite_up_does_not_create_the_full_stack_secrets_file():
    assert "cp deploy/.env.example" not in _make_n("lite-up")


@needs_make
def test_disk_plan_prints_only_and_points_to_df():
    out = _make_n("disk-plan")
    executed = [ln for ln in out.splitlines() if not ln.lstrip().startswith(("echo", "make:"))]
    assert executed == [], executed
    assert "df -h /System/Volumes/Data" in out
    assert "full" in out.lower() and "lite" in out.lower()


def test_runbook_has_small_disk_section_with_safe_commands_and_dated_sizes():
    text = (ROOT / "docs" / "runbooks" / "local-stack.md").read_text()
    assert "## Small disk" in text
    low = text.lower()
    for phrase in (
        "make lite-up",
        "make lite-down",
        "make disk-plan",
        "df -h /system/volumes/data",
        "docker system df",
        "docker builder prune",
        "external drive",
        "2026-11-11",
        "estimate",
        "postgis",
        "timescaledb",
    ):
        assert phrase in low, phrase
