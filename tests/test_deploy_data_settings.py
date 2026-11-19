"""W1-DEPLOY2: the owner can set the data-file, temperature and request settings through deploy/.env (FR-PLAT-4,
FR-ENG-3 "never silently ignore configuration").

The API refuses a temperature variable the chosen source does not use, and treats an EMPTY value as set. So Compose
must pass each of these settings through ONLY when the owner set it: an unset or commented-out line in deploy/.env
must leave the variable ABSENT in the container. Compose's pass-through form (a key with no value, ``NAME:`` in a
mapping) does that: the value comes from the shell or the --env-file, and an unresolved one renders as ``null`` and is
removed from the container's environment (Compose specification, "environment") [verify on a running daemon; the CI
smoke checks it inside the real containers].

A typo in deploy/.env (``MICROGRID_TEMPERATURE_UNIT``) would never reach the container, so the API could not catch it;
``deploy/check-env.sh`` refuses unknown ``MICROGRID_*`` names before anything starts.

``docker compose config`` needs only the docker CLI, no daemon.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"
FULL = yaml.safe_load((DEPLOY / "docker-compose.yml").read_text())
LITE = yaml.safe_load((DEPLOY / "docker-compose.lite.yml").read_text())
ENV_EXAMPLE = (DEPLOY / ".env.example").read_text()
SMOKE = DEPLOY / "tests" / "test_compose_smoke.sh"
SMOKE_TEXT = SMOKE.read_text()
CHECK_ENV = DEPLOY / "check-env.sh"
COMPOSITION = DEPLOY / "tests" / "smoke-composition.json"
RUNBOOK = (ROOT / "docs" / "runbooks" / "local-stack.md").read_text()
API_CONFIG = (ROOT / "api" / "src" / "microgrid_api" / "config.py").read_text()

PASS_THROUGH = (
    "MICROGRID_RESSTOCK_CHARS_FILE",
    "MICROGRID_COMSTOCK_CHARS_FILE",
    "MICROGRID_TEMPERATURE_SOURCE",
    "MICROGRID_TEMPERATURE_COLUMN",
    "MICROGRID_TEMPERATURE_UNITS",
    "MICROGRID_TEMPERATURE_WEATHER_FILE",
    "MICROGRID_TEMPERATURE_TIME_COLUMN",
    "MICROGRID_MAX_REQUEST_BYTES",
    "MICROGRID_PERMISSION_OVERRIDES",
)
FULL_APP = ("migrate", "api", "worker")
LITE_APP = ("migrate", "api")


# --- the compose files ------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("services", "names"), [(FULL["services"], FULL_APP), (LITE["services"], LITE_APP)])
def test_settings_are_pass_through_only_in_every_app_service(services, names):
    for name in names:
        env = services[name]["environment"]
        for var in PASS_THROUGH:
            assert var in env, f"{name} does not pass {var} through"
            assert env[var] is None, f"{name}.{var} must be the pass-through form `{var}:` (no value, no default)"
    for name, svc in services.items():
        if name not in names:
            assert not set(PASS_THROUGH) & set(svc.get("environment") or {}), name


def test_pass_through_settings_are_real_api_settings():
    body = API_CONFIG.split("class Settings(BaseSettings):", 1)[1].split("\n    @", 1)[0]
    fields = {m.group(1) for m in re.finditer(r"^    (\w+):", body, re.M)}
    for var in PASS_THROUGH:
        assert var.removeprefix("MICROGRID_").lower() in fields, var


def test_no_other_pass_through_variable_slipped_in():
    for compose, names in ((FULL, FULL_APP), (LITE, LITE_APP)):
        for name in names:
            nulls = {k for k, v in compose["services"][name]["environment"].items() if v is None}
            assert nulls == set(PASS_THROUGH), (name, sorted(nulls))


# --- the template -----------------------------------------------------------------------------------------------


def _commented_assignments() -> dict[str, str]:
    out: dict[str, str] = {}
    for line in ENV_EXAMPLE.splitlines():
        m = re.match(r"^#\s?(MICROGRID_[A-Z_]+)=(.*)$", line)
        if m:
            out[m.group(1)] = m.group(2)
    return out


def test_template_documents_every_setting_commented_out_with_its_default():
    commented = _commented_assignments()
    assert set(PASS_THROUGH) <= set(commented), sorted(set(PASS_THROUGH) - set(commented))
    assert commented["MICROGRID_RESSTOCK_CHARS_FILE"] == "DC_upgrade0.xlsx"
    assert commented["MICROGRID_COMSTOCK_CHARS_FILE"] == "DC_upgrade0_agg.xlsx"
    assert commented["MICROGRID_TEMPERATURE_SOURCE"] == "none"
    assert commented["MICROGRID_MAX_REQUEST_BYTES"] == "1048576"
    assert commented["MICROGRID_PERMISSION_OVERRIDES"] == """'{"client":{"filters.use":true}}'"""
    # the defaults written in the template are the API's real defaults
    assert 'resstock_chars_file: str = "DC_upgrade0.xlsx"' in API_CONFIG
    assert 'comstock_chars_file: str = "DC_upgrade0_agg.xlsx"' in API_CONFIG
    assert 'temperature_source: str = "none"' in API_CONFIG
    assert "max_request_bytes: int = Field(default=1_048_576" in API_CONFIG


def test_template_never_sets_a_pass_through_setting():
    for line in ENV_EXAMPLE.splitlines():
        assert not re.match(rf"^\s*(export\s+)?({'|'.join(PASS_THROUGH)})=", line), line


def test_template_explains_in_plain_language():
    low = ENV_EXAMPLE.lower()
    for phrase in ("background", "[verify]", "ls data/background", "comment", "empty", "timeseries_column",
                   "weather_file", "413", "filters.use"):  # fmt: skip
        assert phrase.lower() in low, phrase


# --- what `docker compose config` renders (no daemon needed) ----------------------------------------------------


def _config(env_file: Path, *files: str, extra_env: dict[str, str] | None = None) -> dict:
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not installed (only `docker compose config` is needed)")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("MICROGRID_", "API_KEYS", "JWT_", "POSTGRES_"))}
    env |= extra_env or {}
    cmd = ["docker", "compose", "--project-directory", str(ROOT), "--env-file", str(env_file)]
    for f in files:
        cmd += ["-f", str(DEPLOY / f)]
    r = subprocess.run([*cmd, "config", "--format", "json"], capture_output=True, text=True, env=env, check=False)
    if r.returncode != 0 and "compose" in r.stderr and "unknown" in r.stderr:
        pytest.skip("docker compose plugin not installed")
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


STACKS = [
    (("docker-compose.yml",), FULL_APP),
    (("docker-compose.yml", "docker-compose.ci.yml"), FULL_APP),
    (("docker-compose.lite.yml",), LITE_APP),
]


@pytest.mark.parametrize(("files", "services"), STACKS)
def test_template_leaves_every_setting_absent_in_the_containers(files, services):
    cfg = _config(DEPLOY / ".env.example", *files)
    for name in services:
        env = cfg["services"][name]["environment"]
        for var in PASS_THROUGH:
            assert env.get(var) is None, f"{name}.{var} renders as {env.get(var)!r}; it must be absent"


@pytest.mark.parametrize(("files", "services"), STACKS)
def test_only_what_the_owner_set_reaches_the_containers(tmp_path, files, services):
    env_file = tmp_path / ".env"
    env_file.write_text(
        ENV_EXAMPLE
        + "\nMICROGRID_RESSTOCK_CHARS_FILE=DC_baseline_res.xlsx\n"
        + "MICROGRID_TEMPERATURE_SOURCE=timeseries_column\n"
        + "MICROGRID_TEMPERATURE_COLUMN=out.site_outdoor_air_temperature\n"
        + "MICROGRID_TEMPERATURE_UNITS=F\n"
        + """MICROGRID_PERMISSION_OVERRIDES='{"client":{"filters.use":true}}'\n"""
        + "#MICROGRID_COMSTOCK_CHARS_FILE=commented_out.xlsx\n"
    )
    cfg = _config(env_file, *files)
    for name in services:
        env = cfg["services"][name]["environment"]
        assert env["MICROGRID_RESSTOCK_CHARS_FILE"] == "DC_baseline_res.xlsx"
        assert env["MICROGRID_TEMPERATURE_SOURCE"] == "timeseries_column"
        assert env["MICROGRID_TEMPERATURE_COLUMN"] == "out.site_outdoor_air_temperature"
        assert env["MICROGRID_TEMPERATURE_UNITS"] == "F"
        assert json.loads(env["MICROGRID_PERMISSION_OVERRIDES"]) == {"client": {"filters.use": True}}
        for var in ("MICROGRID_COMSTOCK_CHARS_FILE", "MICROGRID_TEMPERATURE_WEATHER_FILE",
                    "MICROGRID_TEMPERATURE_TIME_COLUMN", "MICROGRID_MAX_REQUEST_BYTES"):  # fmt: skip
            assert env.get(var) is None, f"{name}.{var} was not set but renders as {env.get(var)!r}"


def test_an_empty_line_is_passed_as_empty_so_the_api_refuses_it_loudly(tmp_path):
    """`NAME=` (empty) is "set" for Compose and for the API: it reaches the container as "" and the API refuses to
    start (a data-file name must not be empty; a temperature field the source does not use must be unset). The
    template therefore tells the owner to comment a line out, never to empty it."""
    env_file = tmp_path / ".env"
    env_file.write_text(ENV_EXAMPLE + "\nMICROGRID_TEMPERATURE_COLUMN=\n")
    env = _config(env_file, "docker-compose.yml")["services"]["api"]["environment"]
    assert env["MICROGRID_TEMPERATURE_COLUMN"] == ""


def test_the_shell_wins_over_the_env_file_as_documented(tmp_path):
    env = _config(DEPLOY / ".env.example", "docker-compose.yml", extra_env={"MICROGRID_MAX_REQUEST_BYTES": "2048"})[
        "services"
    ]["api"]["environment"]
    assert env["MICROGRID_MAX_REQUEST_BYTES"] == "2048"
    assert "shell" in ENV_EXAMPLE.lower() and "MICROGRID_" in ENV_EXAMPLE


# --- typo guard: deploy/check-env.sh ----------------------------------------------------------------------------


def _check_env(tmp_path: Path, text: str) -> subprocess.CompletedProcess[str]:
    f = tmp_path / "env"
    f.write_text(text)
    return subprocess.run([str(CHECK_ENV), str(f)], capture_output=True, text=True, check=False)


def test_check_env_accepts_the_template_and_every_known_setting(tmp_path):
    assert _check_env(tmp_path, ENV_EXAMPLE).returncode == 0
    known = "".join(f"{v}=x\n" for v in PASS_THROUGH)
    r = _check_env(tmp_path, ENV_EXAMPLE + known + "export MICROGRID_DATA_DIR=/Volumes/x\n")
    assert r.returncode == 0, r.stderr


@pytest.mark.parametrize(
    "line",
    [
        "MICROGRID_TEMPERATURE_UNIT=F",
        "MICROGRID_RESSTOCK_CHAR_FILE=a.xlsx",
        "  MICROGRID_TEMPRATURE_SOURCE=none",
        "export MICROGRID_TEMPERATURE_COLUM=x",
        "microgrid_temperature_source=none",
        "MICROGRID_ENVIRONMENT=development",  # compose sets it; an .env line would be silently ignored
        "MICROGRID_OUTPUT_DIR=/tmp/x",
    ],
)
def test_check_env_refuses_a_name_compose_would_silently_drop(tmp_path, line):
    r = _check_env(tmp_path, ENV_EXAMPLE + line + "\n")
    assert r.returncode != 0
    name = line.strip().removeprefix("export ").split("=", 1)[0]
    assert name in r.stderr and "deploy/.env" in r.stderr
    assert "x.xlsx" not in r.stderr and "/tmp/x" not in r.stderr, "values are never printed"


def test_check_env_ignores_comments_and_fails_on_a_missing_file(tmp_path):
    assert _check_env(tmp_path, "# MICROGRID_TEMPERATURE_UNIT=F\n#MICROGRID_X=1\n").returncode == 0
    r = subprocess.run([str(CHECK_ENV), str(tmp_path / "nope")], capture_output=True, text=True, check=False)
    assert r.returncode != 0 and "nope" in r.stderr


def test_check_env_list_matches_compose():
    text = CHECK_ENV.read_text()
    m = re.search(r"^KNOWN=\"([^\"]*)\"", text, re.M)
    assert m, "KNOWN=... list not found"
    assert set(m.group(1).split()) == {*PASS_THROUGH, "MICROGRID_DATA_DIR"}


def test_check_env_runs_before_compose_in_make_and_smoke():
    make = (ROOT / "Makefile").read_text()
    for target in ("up", "lite-up", "config-check"):
        m = re.search(rf"^{re.escape(target)}:.*?(?=^\S|\Z)", make, re.M | re.S)
        assert m and "./deploy/check-env.sh" in m.group(0), target
        body = m.group(0)
        compose = body.index("$(LITE)") if target == "lite-up" else body.index("$(COMPOSE)")
        assert body.index("check-env.sh") < compose, target
    code = re.sub(r"#.*", "", SMOKE_TEXT)
    assert code.index("./deploy/preflight.sh") < code.index("./deploy/check-env.sh") < code.index('"${COMPOSE[@]}"')


# --- the smoke script -------------------------------------------------------------------------------------------


def test_smoke_requires_data_files_ok_and_explains_a_failure():
    assert 'for key in ("db", "schema", "redis", "data_files")' in SMOKE_TEXT
    assert "data-files-problem" in SMOKE_TEXT
    for name in ("MICROGRID_RESSTOCK_CHARS_FILE", "MICROGRID_COMSTOCK_CHARS_FILE", "MICROGRID_TEMPERATURE_",
                 "MICROGRID_DATA_DIR", "background", "ls "):  # fmt: skip
        assert name in SMOKE_TEXT, name


def test_smoke_proves_pass_through_absence_inside_the_containers_in_ci():
    m = re.search(r"^PASS_THROUGH=\(([^)]*)\)", SMOKE_TEXT, re.M)
    assert m and tuple(m.group(1).split()) == PASS_THROUGH
    code = re.sub(r"#.*", "", SMOKE_TEXT)
    assert re.search(r'exec -T "\$svc" python', code)


def test_smoke_submits_a_custom_composition_run():
    code = re.sub(r"#.*", "", SMOKE_TEXT)
    assert "post_run deploy/tests/smoke-composition.json" in code and '--data-binary "@$1"' in code
    after = code.split("post_run deploy/tests/smoke-composition.json", 1)[1]
    assert 'wait_succeeded "$comp_id"' in after and 'check_results "$comp_id"' in after


def test_composition_request_is_small_and_uses_types_the_fixture_has():
    body = json.loads(COMPOSITION.read_text())
    assert set(body) == {"composition", "seeds"}
    comp = body["composition"]
    assert set(comp) <= {"name", "total_buildings", "building_types", "max_runs"}
    assert comp["max_runs"] <= 10 and comp["total_buildings"] <= 20
    # building types the synthetic fixture has buildings for (see tests/fixtures/synthetic_data.py defaults)
    have = {"sf_detached", "mf_small", "mf_mid", "commercial_private", "institutional_school"}
    types = [e["building_type"] for e in comp["building_types"]]
    assert set(types) <= have and len(types) == len(set(types)) >= 3
    assert {"resstock", "comstock"} <= {
        "comstock" if t in ("commercial_private", "institutional_school") else "resstock" for t in types
    }, "the custom run should exercise both datasets"
    assert any("fixed_count" in e for e in comp["building_types"])
    assert any("min_fraction" in e for e in comp["building_types"])


# --- fixture script and runbook ---------------------------------------------------------------------------------


def test_fixture_script_says_the_names_are_settings():
    text = (DEPLOY / "ci" / "make_fixture_data.py").read_text()
    assert "MICROGRID_RESSTOCK_CHARS_FILE" in text and "MICROGRID_COMSTOCK_CHARS_FILE" in text
    assert "never sets the\ncharacteristics file names" not in text


def test_runbook_explains_choosing_the_data_files():
    assert "## Telling the app which data files to use" in RUNBOOK
    section = RUNBOOK.split("## Telling the app which data files to use", 1)[1].split("\n## ", 1)[0]
    for phrase in ("MICROGRID_RESSTOCK_CHARS_FILE", "MICROGRID_COMSTOCK_CHARS_FILE", "ls data/background",
                   "data_files", "/api/v1/ready", "MICROGRID_TEMPERATURE_SOURCE", "data inspector",
                   "comment", "make down", "check-env.sh"):  # fmt: skip
        assert phrase in section, phrase
