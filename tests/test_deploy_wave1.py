"""Wave 1 W1-DEPLOY: real worker and real web image in the Compose stack, and the fixture-backed CI smoke run
(FR-PLAT-2, FR-PLAT-4, carry-over A3).

Static checks plus one local end-to-end engine run on generated fixture data laid out exactly as the API's server-side
data section expects. No Docker daemon is needed; the container run itself is proven by the `compose-smoke` CI job.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"
COMPOSE_TEXT = (DEPLOY / "docker-compose.yml").read_text()
COMPOSE = yaml.safe_load(COMPOSE_TEXT)
SERVICES = COMPOSE["services"]
CI_OVERLAY = yaml.safe_load((DEPLOY / "docker-compose.ci.yml").read_text())
ENV_EXAMPLE = (DEPLOY / ".env.example").read_text()
WEB_DOCKERFILE = DEPLOY / "web.Dockerfile"
NGINX = DEPLOY / "web" / "nginx.conf"
SMOKE = DEPLOY / "tests" / "test_compose_smoke.sh"
SMOKE_RUN = DEPLOY / "tests" / "smoke-run.json"
FIXTURE_SCRIPT = DEPLOY / "ci" / "make_fixture_data.py"
WORKFLOW = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text())
API_CONFIG = ROOT / "api" / "src" / "microgrid_api" / "config.py"
API_RUNS_SERVICE = ROOT / "api" / "src" / "microgrid_api" / "services" / "runs.py"
needs_make = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")


def _env_file_values() -> dict[str, str]:
    """Parse deploy/.env.example the way Compose and `set -a; . file` both read it (single quotes are literal)."""
    out: dict[str, str] = {}
    for line in ENV_EXAMPLE.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        if len(val) >= 2 and val[0] == val[-1] == "'":
            val = val[1:-1]
        out[key] = val
    return out


# --- web image -------------------------------------------------------------------------------------------------


def test_web_service_builds_the_real_app_image():
    web = SERVICES["web"]
    assert web["build"] == {"context": ".", "dockerfile": "deploy/web.Dockerfile", "target": "web"}
    assert web["image"] == "microgrid-web:dev"
    assert "volumes" not in web, "the web image carries its own build and config; no bind mounts"
    assert web["cap_drop"] == ["ALL"] and "no-new-privileges:true" in web["security_opt"]
    assert web["ports"] == ["127.0.0.1:${WEB_PORT:-3000}:8080"]
    assert web["mem_limit"] and web["healthcheck"]["test"][0] in ("CMD", "CMD-SHELL")
    assert not (DEPLOY / "web" / "index.html").exists(), "the placeholder page is gone"


def test_web_dockerfile_is_multi_stage_node22_build_then_unprivileged_nginx():
    text = WEB_DOCKERFILE.read_text()
    froms = re.findall(r"^FROM\s+(\S+)(?:\s+AS\s+(\S+))?", text, re.M)
    assert froms[0][0].startswith("node:22") and froms[0][1] == "build"
    assert froms[-1][0].startswith("nginxinc/nginx-unprivileged:") and froms[-1][1] == "web"
    assert all(":" in image and not image.endswith(":latest") for image, _ in froms)
    assert re.search(r"^RUN npm ci\b", text, re.M) and re.search(r"^RUN npm run build\b", text, re.M)
    assert re.search(r"^COPY --from=build \S*/dist /usr/share/nginx/html", text, re.M)
    assert re.search(r"^COPY deploy/web/nginx\.conf /etc/nginx/conf\.d/default\.conf", text, re.M)
    assert not re.search(r"^ADD\b", text, re.M)
    # The contract with the web story: the real API client, same-origin /api/v1, so no mock mode and no base URL.
    code = re.sub(r"#.*", "", text)
    assert "VITE_API_MODE" not in code and "VITE_API_BASE_URL" not in code
    assert not re.search(r"^(ENV|ARG)\s+\S*(PASSWORD|SECRET|TOKEN|KEY)", text, re.M | re.I)
    runtime = text.split("AS web", 1)[1]
    assert re.search(r"^USER (?!root|0\b)\S+", runtime, re.M), "runtime stage must run as a non-root user"
    assert re.search(r"^EXPOSE 8080$", runtime, re.M)


def test_web_dockerfile_copies_only_web_sources_and_the_nginx_config():
    text = WEB_DOCKERFILE.read_text()
    sources = set()
    for m in re.finditer(r"^COPY\s+(?:--\S+\s+)*(.+?)\s+\S+\s*$", text, re.M):
        sources.update(m.group(1).split())
    assert sources <= {"web/package.json", "web/package-lock.json", "web/", "deploy/web/nginx.conf", "/web/dist"}


def _nginx_locations(conf: str) -> dict[str, str]:
    conf = re.sub(r"#.*", "", conf)
    return {m.group(1).strip(): m.group(2) for m in re.finditer(r"^\s*location\s+([^{]+)\{([^}]*)\}", conf, re.M)}


def test_nginx_proxies_api_with_sse_settings_and_keeps_authorization():
    conf = NGINX.read_text()
    locs = _nginx_locations(conf)
    api = locs["/api/"]
    assert re.search(r"proxy_pass\s+\$\w+;", api) or "proxy_pass http://api:8000;" in api
    assert "http://api:8000" in api, "the proxy target is the api service on the compose network"
    assert re.search(r"proxy_buffering\s+off;", api), "SSE needs proxy buffering off"
    assert re.search(r"proxy_cache\s+off;", api)
    assert re.search(r"proxy_http_version\s+1\.1;", api)
    timeout = re.search(r"proxy_read_timeout\s+(\d+)([smh]?);", api)
    assert timeout, "SSE streams need a long read timeout"
    seconds = int(timeout.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600}[timeout.group(2)]
    assert seconds >= 600
    code = re.sub(r"#.*", "", conf)
    assert "Authorization" not in code, "the Authorization header must pass through untouched"
    assert "proxy_pass_request_headers off" not in code and "proxy_set_header Connection" in code


def test_nginx_spa_fallback_and_security_headers():
    conf = NGINX.read_text()
    locs = _nginx_locations(conf)
    assert re.search(r"try_files\s+\$uri\s+\$uri/\s+/index\.html;", locs["/"])
    assert re.search(r"^\s*listen\s+8080;", conf, re.M)
    assert re.search(r"server_tokens\s+off;", conf)
    for header in (
        "X-Content-Type-Options",
        "X-Frame-Options",
        "Referrer-Policy",
        "Content-Security-Policy",
        "Permissions-Policy",
    ):
        assert re.search(rf"add_header\s+{header}\s+.+\s+always;", conf), f"missing security header {header}"
    csp = re.search(r'add_header\s+Content-Security-Policy\s+"([^"]+)"', conf).group(1)
    assert "default-src 'self'" in csp and "frame-ancestors 'none'" in csp and "object-src 'none'" in csp
    assert "script-src 'self'" in csp and "unsafe-eval" not in csp
    # nginx drops inherited server-level add_header lines in any location that has its own add_header.
    for where, body in locs.items():
        assert "add_header" not in body, f"location {where} would silently drop the security headers"


def test_dockerignore_keeps_web_build_output_and_modules_out_of_the_context():
    lines = {ln.strip().rstrip("/") for ln in (ROOT / ".dockerignore").read_text().splitlines()}
    assert "web/dist" in lines and ("**/node_modules" in lines or "web/node_modules" in lines)
    assert "web" not in lines and "web/src" not in lines


# --- worker ----------------------------------------------------------------------------------------------------


def _run_entrypoint(tmp_path: Path, extra_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    stub = bindir / "celery"
    stub.write_text('#!/bin/sh\nfor a in "$@"; do echo "ARG:$a"; done\n')
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    env = {"PATH": f"{bindir}:/usr/bin:/bin", **extra_env}
    return subprocess.run(
        ["sh", str(DEPLOY / "worker-entrypoint.sh")], capture_output=True, text=True, env=env, check=False
    )


def test_worker_entrypoint_execs_celery_worker_with_beat_and_settings(tmp_path):
    text = (DEPLOY / "worker-entrypoint.sh").read_text()
    assert "placeholder" not in text.lower() and "story 1.10" not in text
    assert re.search(r"^exec celery\b", text, re.M)
    r = _run_entrypoint(tmp_path, {"WORKER_CONCURRENCY": "3", "LOG_LEVEL": "WARNING"})
    assert r.returncode == 0, r.stderr
    args = [ln[4:] for ln in r.stdout.splitlines() if ln.startswith("ARG:")]
    assert args[:3] == ["-A", "microgrid_api.worker", "worker"]
    assert "--concurrency=3" in args and "--loglevel=WARNING" in args
    assert "--beat" in args, "the stuck-run reaper is scheduled by Celery beat inside the single worker"
    schedule = [a for a in args if a.startswith("--schedule=")]
    assert schedule and schedule[0].startswith("--schedule=/tmp/"), "/app is not writable by the app user"


def test_worker_entrypoint_rejects_a_bad_concurrency(tmp_path):
    r = _run_entrypoint(tmp_path, {"WORKER_CONCURRENCY": "two"})
    assert r.returncode != 0 and "WORKER_CONCURRENCY" in r.stderr
    assert "ARG:" not in r.stdout


def test_worker_service_is_real_and_wired_like_the_api():
    w = SERVICES["worker"]
    assert w["build"]["target"] == "worker" and w["image"] == "microgrid-worker:dev"
    env = w["environment"]
    assert env["WORKER_CONCURRENCY"] == "${WORKER_CONCURRENCY:-2}", "the template value must reach the container"
    for key in ("MICROGRID_REDIS_URL", "MICROGRID_DATABASE_URL", "MICROGRID_API_KEYS", "MICROGRID_JWT_SECRET"):
        assert env[key] == SERVICES["api"]["environment"][key], f"worker and api disagree on {key}"
    assert env["MICROGRID_DATA_DIR"] == "/data" and env["MICROGRID_OUTPUT_DIR"] == "/outputs"
    assert "${MICROGRID_DATA_DIR:-./data}:/data:ro" in w["volumes"] and "outputs:/outputs" in w["volumes"]
    assert w["depends_on"]["migrate"]["condition"] == "service_completed_successfully"
    hc = " ".join(w["healthcheck"]["test"])
    assert "celery -A microgrid_api.worker inspect ping" in hc


# --- settings: compose, env template and the API's Settings agree ----------------------------------------------


def _settings_fields() -> set[str]:
    text = API_CONFIG.read_text()
    body = text.split("class Settings(BaseSettings):", 1)[1].split("\n    @", 1)[0]
    return {m.group(1) for m in re.finditer(r"^    (\w+):", body, re.M)} - {"model_config"}


@pytest.mark.parametrize("name", ["api", "worker", "migrate"])
def test_every_microgrid_variable_in_compose_is_a_real_setting(name):
    fields = _settings_fields()
    assert {"api_keys", "redis_url", "data_dir", "output_dir"} <= fields
    for key in SERVICES[name]["environment"]:
        if key.startswith("MICROGRID_"):
            assert key.removeprefix("MICROGRID_").lower() in fields, f"{name}: {key} is not read by Settings"


def test_api_keys_are_required_and_come_from_the_env_file():
    """Production settings refuse to start without MICROGRID_API_KEYS (config.py); migrate, api and worker all build
    Settings(), so all three need it."""
    assert "MICROGRID_API_KEYS is required" in API_CONFIG.read_text()
    for name in ("api", "worker", "migrate"):
        assert SERVICES[name]["environment"]["MICROGRID_API_KEYS"] == "${API_KEYS:?set in deploy/.env}"


def test_env_template_api_keys_are_valid_dev_only_json():
    keys = json.loads(_env_file_values()["API_KEYS"])
    assert isinstance(keys, list) and keys
    for entry in keys:
        assert set(entry) == {"subject", "role", "key"}
        assert re.fullmatch(r"[A-Za-z0-9_.@-]{1,64}", entry["subject"])
        assert entry["role"] in ("admin", "client", "demo")
        assert len(entry["key"]) >= 16 and "dev" in entry["key"]
    assert len({e["key"] for e in keys}) == len(keys) and len({e["subject"] for e in keys}) == len(keys)


def test_every_compose_variable_is_in_the_template_and_no_template_entry_is_orphaned():
    used = set(re.findall(r"\$\{(\w+)", COMPOSE_TEXT))
    template = set(_env_file_values())
    assert used <= template, f"compose reads variables the template does not define: {sorted(used - template)}"
    assert template <= used, f"template entries compose never reads: {sorted(template - used)}"


# --- fixture data for the CI smoke run --------------------------------------------------------------------------


def test_ci_overlay_mounts_generated_fixture_data_read_only():
    for name in ("api", "worker"):
        assert CI_OVERLAY["services"][name]["volumes"] == ["./tests/fixtures/data:/data:ro"]


def _server_layout() -> dict[str, str]:
    """The layout dict in the API's server_side_data_section, read from the source so drift fails this test."""
    text = API_RUNS_SERVICE.read_text()
    body = text.split("def server_side_data_section", 1)[1].split("layout = {", 1)[1].split("}", 1)[0]
    layout = dict(re.findall(r'"(\w+)":\s*"([^"]+)"', body))
    assert set(layout) == {"background_dir", "resstock_timeseries_dir", "comstock_timeseries_dir"}, layout
    return layout


def _load_fixture_script():
    spec = importlib.util.spec_from_file_location("_make_fixture_data", FIXTURE_SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def generated_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("compose_fixture") / "data"
    r = subprocess.run([sys.executable, str(FIXTURE_SCRIPT), str(root)], capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stderr
    return root


def test_fixture_script_writes_the_layout_the_server_side_data_section_expects(generated_data):
    for rel in _server_layout().values():
        assert (generated_data / rel).is_dir(), rel
    # The server never sets the characteristics file names, so the engine uses its DataConfig defaults.
    from microgrid.community_load.synthetic.config import DataConfig

    defaults = DataConfig()
    for name in (defaults.resstock_chars_file, defaults.comstock_chars_file):
        assert (generated_data / "background" / name).is_file(), name
    assert list((generated_data / _server_layout()["resstock_timeseries_dir"]).glob("upgrade=0/state=DC/*.parquet"))


def test_fixture_script_refuses_a_non_empty_target_it_did_not_create(tmp_path):
    (tmp_path / "keep.txt").write_text("owner file")
    r = subprocess.run(
        [sys.executable, str(FIXTURE_SCRIPT), str(tmp_path)], capture_output=True, text=True, check=False
    )
    assert r.returncode != 0 and "not empty" in r.stderr
    assert (tmp_path / "keep.txt").read_text() == "owner file"


def test_fixture_script_refuses_the_owner_data_folder():
    module = _load_fixture_script()
    with pytest.raises(SystemExit):
        module.main([str(ROOT / "data")])


def _smoke_request() -> dict:
    return json.loads(SMOKE_RUN.read_text())


def test_smoke_request_is_what_a_client_may_send():
    body = _smoke_request()
    assert set(body) <= {"config", "seeds"} and "config" in body
    cfg = body["config"]
    assert "data" not in cfg, "the server builds the data section"
    assert "base_dir" not in (cfg.get("output") or {})
    flat = json.dumps(cfg)
    for word in ("_dir", "_file", "_path"):
        assert word not in flat, f"path-like key in the smoke config: {word}"
    assert cfg["composition"]["total_buildings"] <= 50 and cfg["ensemble"]["max_runs"] <= 20
    assert all(isinstance(v, int) and 0 <= v < 2**32 for v in body.get("seeds", {}).values())


def test_smoke_request_runs_end_to_end_on_read_only_fixture_data(generated_data, tmp_path):
    """What the worker will do in the container: snapshot = client config + server-built data paths + seeds, then
    run_scenario. The data folder is made read-only, as it is mounted in the container."""
    from microgrid.community_load.synthetic.run import run_scenario

    body = _smoke_request()
    cfg = copy.deepcopy(body["config"])
    cfg["data"] = {k: str((generated_data / rel).resolve()) for k, rel in _server_layout().items()}
    cfg["output"] = {"base_dir": str(tmp_path / "out")}
    cfg["rng"] = dict(body["seeds"])
    modes: list[tuple[Path, int]] = []
    for p in [generated_data, *generated_data.rglob("*")]:
        modes.append((p, p.stat().st_mode))
        p.chmod(p.stat().st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
    try:
        result = run_scenario(cfg)
    finally:
        for p, mode in reversed(modes):
            p.chmod(mode)
    doc = result.to_dict()
    assert "results" in doc, "the worker writes results.json only when the document has a results block"
    assert doc["n_attempted"] >= 1


# --- make up, smoke script, CI ----------------------------------------------------------------------------------


@needs_make
def test_make_up_starts_the_whole_stack_including_worker_and_web():
    r = subprocess.run(
        ["make", "-C", str(ROOT), "-n", "up", "COMPOSE=docker compose"], capture_output=True, text=True, check=False
    )
    assert r.returncode == 0, r.stderr
    line = next(ln for ln in r.stdout.splitlines() if " up " in ln)
    assert "--wait" in line and "--build" in line
    assert line.split()[-6:] == ["db", "redis", "migrate", "api", "worker", "web"]


def test_smoke_script_ci_mode_proves_the_full_round_trip():
    text = SMOKE.read_text()
    code = re.sub(r"#.*", "", text)
    assert "deploy/docker-compose.ci.yml" in code and "deploy/.env.example" in code
    assert "--project-name" in code or " -p " in code, "CI mode must not share volumes with the owner's stack"
    for svc in ("db", "redis", "api", "worker", "web"):
        assert re.search(rf"\b{svc}\b", code)
    assert '"redis":"ok"' in code and '"schema":"ok"' in code and '"db":"ok"' in code
    assert "$WEB/api/v1/runs" in code, "the run is created through the web proxy (proves Authorization passes)"
    assert "/results" in code and "/events" in code and "succeeded" in code
    assert re.search(r"logs\b", code), "failures must dump container logs"
    assert "smoke-run.json" in code
    assert not re.search(r"\bdown\b", code)


def test_smoke_script_parses():
    r = subprocess.run(["bash", "-n", str(SMOKE)], capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stderr


def test_smoke_script_ci_mode_fails_fast_without_generated_fixtures(tmp_path):
    """CI mode needs the generated data; without it the script stops with a clear message before any docker call."""
    work = tmp_path / "repo"
    (work / "deploy" / "tests").mkdir(parents=True)
    for rel in ("deploy/preflight.sh", "deploy/.env.example", "deploy/tests/test_compose_smoke.sh"):
        shutil.copy(ROOT / rel, work / rel)
    bindir = tmp_path / "bin"
    bindir.mkdir()
    log = tmp_path / "docker_calls"
    (bindir / "docker").write_text(f'#!/bin/sh\necho "$@" >> {log}\nexit 1\n')
    (bindir / "docker").chmod(0o755)
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "SMOKE_CI": "1", "PREFLIGHT_MIN_FREE_GB": "0"}
    r = subprocess.run(
        [str(work / "deploy" / "tests" / "test_compose_smoke.sh")], capture_output=True, text=True, env=env
    )
    assert r.returncode != 0
    assert "make_fixture_data.py" in r.stderr
    assert not log.exists()
    assert not (work / "deploy" / ".env").exists(), "CI mode uses the template; it never creates deploy/.env"


def _smoke_job() -> dict:
    return WORKFLOW["jobs"]["compose-smoke"]


def test_compose_smoke_job_is_enabled_blocking_and_bounded():
    job = _smoke_job()
    assert job.get("if") is None, "compose-smoke must run"
    assert "continue-on-error" not in job and all("continue-on-error" not in s for s in job["steps"])
    assert 10 <= int(job["timeout-minutes"]) <= 45


def test_compose_smoke_job_generates_fixtures_then_runs_the_ci_mode_smoke():
    steps = _smoke_job()["steps"]
    runs = [s.get("run", "") for s in steps]
    gen = next(i for i, r in enumerate(runs) if "deploy/ci/make_fixture_data.py tests/fixtures/data" in r)
    smoke = next(i for i, r in enumerate(runs) if "./deploy/tests/test_compose_smoke.sh" in r)
    assert gen < smoke
    env = _smoke_job().get("env", {}) | steps[smoke].get("env", {})
    assert str(env.get("SMOKE_CI")) == "1"
    base = "docker compose --project-directory . --env-file deploy/.env.example"
    overlay = "-f deploy/docker-compose.yml -f deploy/docker-compose.ci.yml"
    failure = [s for s in steps if s.get("if") == "failure()"]
    assert failure and all(base in s["run"] and overlay in s["run"] and "logs" in s["run"] for s in failure)
    assert all("|| true" not in r for r in runs[: smoke + 1])


def test_runbook_describes_the_real_worker_and_web_and_unmeasured_sizes():
    text = (ROOT / "docs" / "runbooks" / "local-stack.md").read_text()
    low = text.lower()
    assert "placeholder that exits" not in low and "no worker is started" not in low
    assert "microgrid-web:dev" in text and "not yet measured" in low
    assert "api_keys" in low and "/api/" in text
    assert "make logs s=worker" in text


# --- the smoke script's control flow against stub docker and curl (no daemon) ----------------------------------

_CURL_STUB = r"""#!{python}
import pathlib, sys
args = sys.argv[1:]
state = pathlib.Path({state!r})
with pathlib.Path({log!r}).open("a") as fh:
    fh.write(" ".join(args) + "\n")
url = next(a for a in args if a.startswith("http://"))
final = "{final}"
RID = "0b6c1d2e-3f40-4a5b-8c6d-7e8f90a1b2c3"
html = '<!doctype html><html><body><div id="root"></div></body></html>'
if "-w" in args and "-X" not in args:
    sys.stdout.write("401"); sys.exit(0)
if "-X" in args:
    sys.stdout.write('{{"id":"%s","status":"queued","owner":"dev-admin"}}\n202' % RID); sys.exit(0)
if "-D" in args:
    sys.stdout.write("HTTP/1.1 200 OK\r\nX-Content-Type-Options: nosniff\r\n\r\n"); sys.exit(0)
if url.endswith("/api/v1/health"):
    sys.stdout.write('{{"status":"ok"}}'); sys.exit(0)
if url.endswith("/api/v1/ready"):
    sys.stdout.write('{{"ready":true,"checks":{{"db":"ok","schema":"ok","redis":"ok"}}}}'); sys.exit(0)
if url.endswith("/results"):
    sys.stdout.write('{{"schema_version":"1","results":{{}}}}'); sys.exit(0)
if url.endswith("/events"):
    sys.stdout.write('event: status\r\ndata: {{"status": "succeeded"}}\r\n\r\n'); sys.exit(0)
if "/api/v1/runs/" in url:
    n = int(state.read_text()) if state.exists() else 0
    state.write_text(str(n + 1))
    status = "running" if n == 0 else final
    sys.stdout.write('{{"id":"%s","status":"%s","owner":"dev-admin"}}' % (RID, status)); sys.exit(0)
if url.startswith("http://127.0.0.1:3000/"):
    sys.stdout.write(html); sys.exit(0)
sys.exit(7)
"""


def _smoke_with_stubs(tmp_path: Path, final: str) -> tuple[subprocess.CompletedProcess[str], str, str]:
    work = tmp_path / "repo"
    (work / "deploy" / "tests").mkdir(parents=True)
    for rel in (
        "deploy/preflight.sh",
        "deploy/.env.example",
        "deploy/tests/test_compose_smoke.sh",
        "deploy/tests/smoke-run.json",
    ):
        shutil.copy(ROOT / rel, work / rel)
    background = work / "tests" / "fixtures" / "data" / "background"
    background.mkdir(parents=True)
    (background / "DC_upgrade0.xlsx").write_text("stub")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    docker_log, curl_log = tmp_path / "docker_calls", tmp_path / "curl_calls"
    docker = bindir / "docker"
    docker.write_text(
        f'#!/bin/sh\necho "$@" >> {docker_log}\ncase "$*" in *"ps -a --format"*) echo "running/healthy" ;; esac\nexit 0\n'
    )
    curl = bindir / "curl"
    curl.write_text(
        _CURL_STUB.format(python=sys.executable, state=str(tmp_path / "polls"), log=str(curl_log), final=final)
    )
    sleep = bindir / "sleep"
    sleep.write_text("#!/bin/sh\nexit 0\n")
    for exe in (docker, curl, sleep):
        exe.chmod(0o755)
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "SMOKE_CI": "1", "PREFLIGHT_MIN_FREE_GB": "0"}
    for var in ("API_PORT", "WEB_PORT", "SMOKE_API_KEY", "API_KEYS", "SMOKE_KEEP_UP"):
        env.pop(var, None)
    r = subprocess.run(
        [str(work / "deploy" / "tests" / "test_compose_smoke.sh")],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        check=False,
    )
    return r, docker_log.read_text(), curl_log.read_text() if curl_log.exists() else ""


def test_smoke_script_ci_happy_path_with_stubs(tmp_path):
    r, docker_calls, curl_calls = _smoke_with_stubs(tmp_path, "succeeded")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "SMOKE OK" in r.stdout and "run succeeded" in r.stdout
    assert "--project-name microgrid-smoke" in docker_calls and "deploy/docker-compose.ci.yml" in docker_calls
    assert "up -d --build db redis migrate api worker web" in docker_calls
    assert docker_calls.rstrip().splitlines()[-1].endswith(" stop"), "the stack is stopped, never downed"
    post = next(ln for ln in curl_calls.splitlines() if "-X POST" in ln)
    assert "http://127.0.0.1:3000/api/v1/runs" in post, "the run is created through the web proxy"
    assert "Authorization: Bearer dev-only-admin-key-change-me" in post
    assert "@deploy/tests/smoke-run.json" in post


@pytest.mark.parametrize("final", ["failed", "cancelled", "bogus"])
def test_smoke_script_ci_fails_and_dumps_logs_when_the_run_does_not_succeed(tmp_path, final):
    r, docker_calls, _ = _smoke_with_stubs(tmp_path, final)
    assert r.returncode != 0 and "SMOKE FAILED" in r.stderr
    assert "SMOKE OK" not in r.stdout
    assert "logs --no-color --timestamps --tail 200" in docker_calls
    if final != "bogus":
        assert re.search(r"--tail 200 worker api", docker_calls)
