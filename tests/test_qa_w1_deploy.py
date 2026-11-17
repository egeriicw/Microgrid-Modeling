"""Independent QA review of W1-DEPLOY (FR-PLAT-2, FR-PLAT-4, carry-over A3).

Hostile checks the builder's own tests do not make:
* the smoke script against a configurable stub ``curl``/``docker``: every way the stack can be broken must make the
  script exit non-zero (a smoke test that passes on a broken stack is worse than none);
* the fixture generator against the owner's data folder reached by symlink, ``..`` and planted symlinks;
* worker entrypoint hostile ``WORKER_CONCURRENCY`` values;
* nginx proxy target, CSP and header inheritance; hardening regressions against the pre-story commit.

Known defects are ``xfail(strict=True)`` with the defect in the reason. No Docker daemon is needed.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"
SMOKE_FILES = (
    "deploy/preflight.sh",
    "deploy/.env.example",
    "deploy/tests/test_compose_smoke.sh",
    "deploy/tests/smoke-run.json",
)
BASE_COMMIT = "4ab1fa5"
RID = "0b6c1d2e-3f40-4a5b-8c6d-7e8f90a1b2c3"
ADMIN_KEY = "dev-only-admin-key-change-me"


# --- smoke script against a configurable stub ----------------------------------------------------------------------

# The stub emulates the curl flags the script uses (-f, -s, -S, -o, -w, -D, -X, -H, --data-binary, -N, --max-time).
# Responses come from a JSON scenario: route -> {"code", "body", "exit"}; "statuses" is the poll sequence.
_CURL = r"""#!{python}
import json, pathlib, sys
scen = json.loads(pathlib.Path({scen!r}).read_text())
args = sys.argv[1:]
with open({log!r}, "a") as fh:
    fh.write(json.dumps(args) + "\n")
takes_value = {{"-o", "-w", "-D", "-X", "-H", "--data-binary", "--max-time"}}
opts, headers, url, i = {{}}, [], None, 0
short = ""
while i < len(args):
    a = args[i]
    if a in takes_value:
        if a == "-H":
            headers.append(args[i + 1])
        else:
            opts[a] = args[i + 1]
        i += 2
        continue
    if a.startswith("http://"):
        url = a
    elif a.startswith("-") and not a.startswith("--"):
        short += a[1:]
    i += 1
method = opts.get("-X", "GET")
host, _, path = url[len("http://"):].partition("/")
path = "/" + path
side = "web" if host.endswith(":3000") else "api"
auth_ok = "Authorization: Bearer {key}" in headers
RID = "{rid}"
html = '<!doctype html><html><head></head><body><div id="root"></div></body></html>'
default = {{
    "web /": (200, html), "web /runs/smoke-deep-link": (200, html),
    "web /api/v1/health": (200, '{{"status":"ok"}}'), "api /api/v1/health": (200, '{{"status":"ok"}}'),
    "api /api/v1/ready": (200, '{{"ready":true,"checks":{{"db":"ok","schema":"ok","redis":"ok"}}}}'),
    "results": (200, '{{"schema_version":"1.1","engine_version":"0.1","results":{{"n_runs":2,'
                     '"time":{{"hourly":{{"start":"2018-01-01T00:00:00","step_minutes":60,"count":2}}}},'
                     '"envelopes":{{"hourly":{{"total":{{"mean_kw":[3,4]}}}}}}}}}}'),
    "events": (200, 'event: progress\r\nid: 1\r\ndata: {{"stage":"run"}}\r\n\r\nevent: status\r\ndata: {{"status": "succeeded"}}\r\n\r\n'),
    "unauth": (401, '{{"detail":"not authenticated"}}'),
    "post": (202, '{{"id":"%s","status":"queued","owner":"dev-admin"}}' % RID),
}}
if path == "/api/v1/runs" and method == "GET":
    route = "unauth" if not auth_ok else "web /api/v1/runs-authed"
elif path == "/api/v1/runs" and method == "POST":
    route = "post" if auth_ok else "unauth"
elif path.endswith("/results"):
    route = "results"
elif path.endswith("/events"):
    route = "events"
elif path.startswith("/api/v1/runs/"):
    route = "detail"
else:
    route = side + " " + path
over = scen.get("routes", {{}}).get(route)
if route == "detail":
    st = pathlib.Path({state!r})
    n = int(st.read_text()) if st.exists() else 0
    st.write_text(str(n + 1))
    seq = scen.get("statuses", ["running", "succeeded"])
    status = seq[min(n, len(seq) - 1)]
    code, body = 200, '{{"id":"%s","status":"%s","owner":"dev-admin"}}' % (RID, status)
    if not auth_ok:
        code, body = default["unauth"]
elif route in default:
    code, body = default[route]
else:
    code, body = 404, '{{"detail":"Not Found"}}'
if over:
    if "exit" in over:
        sys.stderr.write("curl: stub failure\n"); sys.exit(over["exit"])
    code, body = over.get("code", code), over.get("body", body)
if "f" in short and code >= 400:
    sys.stderr.write("curl: (22) The requested URL returned error: %d\n" % code); sys.exit(22)
out = ""
if "-D" in opts:
    out += "HTTP/1.1 %d X\r\n" % code + scen.get(
        "headers", "X-Content-Type-Options: nosniff\r\nContent-Security-Policy: default-src 'self'\r\n"
    ) + "\r\n"
if opts.get("-o") != "/dev/null":
    out += body
if "-w" in opts:
    out += opts["-w"].replace("\\n", "\n").replace("%{{http_code}}", str(code))
sys.stdout.write(out)
"""

_DOCKER = r"""#!{python}
import json, pathlib, sys
scen = json.loads(pathlib.Path({scen!r}).read_text())
args = sys.argv[1:]
with open({log!r}, "a") as fh:
    fh.write(" ".join(args) + "\n")
if " up " in " " + " ".join(args) + " ":
    sys.exit(scen.get("up_exit", 0))
if "ps" in args and "--format" in args:
    svc = args[-1]
    print(scen.get("health", {{}}).get(svc, "running/healthy"))
sys.exit(0)
"""


def _smoke(
    tmp_path: Path, scenario: dict[str, Any] | None = None, *, ci: bool = True, extra_env: dict[str, str] | None = None
) -> tuple[subprocess.CompletedProcess[str], list[str], list[list[str]], Path]:
    work = tmp_path / "repo"
    for rel in SMOKE_FILES:
        (work / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / rel, work / rel)
    bg = work / "tests" / "fixtures" / "data" / "background"
    bg.mkdir(parents=True)
    (bg / "DC_upgrade0.xlsx").write_text("stub")
    owner = work / "data" / "background"
    owner.mkdir(parents=True)
    (owner / "DC_upgrade0.xlsx").write_text("OWNER DATA")
    scen = tmp_path / "scenario.json"
    scen.write_text(json.dumps(scenario or {}))
    bindir = tmp_path / "bin"
    bindir.mkdir()
    dlog, clog = tmp_path / "docker.log", tmp_path / "curl.log"
    py = sys.executable
    (bindir / "curl").write_text(
        _CURL.format(python=py, scen=str(scen), log=str(clog), state=str(tmp_path / "polls"), key=ADMIN_KEY, rid=RID)
    )
    (bindir / "docker").write_text(_DOCKER.format(python=py, scen=str(scen), log=str(dlog)))
    (bindir / "sleep").write_text("#!/bin/sh\nexit 0\n")
    for exe in bindir.iterdir():
        exe.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SMOKE_", "API_", "WEB_", "MICROGRID_"))}
    env |= {"PATH": f"{bindir}:{os.environ['PATH']}", "PREFLIGHT_MIN_FREE_GB": "0", "SMOKE_TIMEOUT": "2"}
    env |= {"SMOKE_RUN_TIMEOUT": "2"}
    if ci:
        env["SMOKE_CI"] = "1"
    env |= extra_env or {}
    r = subprocess.run(
        [str(work / "deploy" / "tests" / "test_compose_smoke.sh")],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )
    docker_calls = dlog.read_text().splitlines() if dlog.exists() else []
    curl_calls = [json.loads(x) for x in clog.read_text().splitlines()] if clog.exists() else []
    return r, docker_calls, curl_calls, work


def _assert_failed(r: subprocess.CompletedProcess[str]) -> None:
    assert r.returncode != 0, f"smoke passed on a broken stack:\n{r.stdout}\n{r.stderr}"
    assert "SMOKE OK" not in r.stdout


def test_stub_happy_path_passes(tmp_path):
    """Control: the stub stack is healthy, so every mutation below is meaningful."""
    r, docker_calls, curl_calls, _ = _smoke(tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "SMOKE OK" in r.stdout
    assert any("-X" in c and "POST" in c for c in curl_calls)


BROKEN = {
    "run stays queued (worker never picks it up)": {"statuses": ["queued"]},
    "run ends failed": {"statuses": ["running", "failed"]},
    "run ends cancelled": {"statuses": ["cancelled"]},
    "api /health 500": {"routes": {"api /api/v1/health": {"code": 500, "body": "Internal Server Error"}}},
    "api /ready 503 redis down": {
        "routes": {
            "api /api/v1/ready": {
                "code": 503,
                "body": '{"ready":false,"checks":{"db":"ok","schema":"ok","redis":"unavailable: ConnectionError"}}',
            }
        }
    },
    "api /ready 200 but redis key missing": {
        "routes": {"api /api/v1/ready": {"body": '{"ready":true,"checks":{"db":"ok","schema":"ok"}}'}}
    },
    "web root 502": {"routes": {"web /": {"code": 502, "body": "<html>502 Bad Gateway</html>"}}},
    "web root is the nginx default page": {"routes": {"web /": {"body": "<h1>Welcome to nginx!</h1>"}}},
    "SPA fallback 404": {"routes": {"web /runs/smoke-deep-link": {"code": 404, "body": "not found"}}},
    "security header missing": {"headers": "Server: nginx\r\n"},
    "Content-Security-Policy missing": {"headers": "X-Content-Type-Options: nosniff\r\n"},
    "SSE has no progress event (only the final status)": {
        "routes": {"events": {"body": 'event: status\r\ndata: {"status": "succeeded"}\r\n\r\n'}}
    },
    "progress reaches the stream only after the run ended (buffering proxy)": {"statuses": ["succeeded"]},
    "results with an empty time axis": {
        "routes": {
            "results": {
                "body": '{"schema_version":"1.1","results":{"n_runs":1,"time":{"hourly":{"count":0}},'
                '"envelopes":{"hourly":{"total":{"mean_kw":[]}}}}}'
            }
        }
    },
    "proxy returns html instead of api json": {"routes": {"web /api/v1/health": {"body": "<html>app</html>"}}},
    "proxy 502": {"routes": {"web /api/v1/health": {"code": 502, "body": "Bad Gateway"}}},
    "auth disabled (unauthenticated list is 200)": {"routes": {"unauth": {"code": 200, "body": '{"items":[]}'}}},
    "POST returns 500": {"routes": {"post": {"code": 500, "body": "Internal Server Error"}}},
    "POST returns html with 202": {"routes": {"post": {"body": "<html>oops</html>"}}},
    "POST 202 with no id": {"routes": {"post": {"body": '{"status":"queued"}'}}},
    "detail returns 500": {"routes": {"detail": {"code": 500, "body": "boom"}}},
    "detail returns html": {"routes": {"detail": {"body": "<html>x</html>"}}},
    "results 404": {"routes": {"results": {"code": 404, "body": '{"detail":"results not available"}'}}},
    "results empty object": {"routes": {"results": {"body": "{}"}}},
    "results lacks results key": {"routes": {"results": {"body": '{"schema_version":"1.1"}'}}},
    "results empty body": {"routes": {"results": {"body": ""}}},
    "SSE ends with failed": {"routes": {"events": {"body": 'event: status\r\ndata: {"status": "failed"}\r\n\r\n'}}},
    "SSE never ends (curl max-time)": {"routes": {"events": {"exit": 28}}},
    "SSE 401 (proxy dropped Authorization)": {"routes": {"events": {"code": 401, "body": "{}"}}},
    "compose up fails (migrate exited 1)": {"up_exit": 1},
    "worker unhealthy": {"health": {"worker": "running/unhealthy"}},
    "worker exited": {"health": {"worker": "exited/"}},
    "web has no healthcheck state": {"health": {"web": "running/"}},
}


@pytest.mark.parametrize("name", sorted(BROKEN))
def test_smoke_fails_on_a_broken_stack(tmp_path, name):
    r, docker_calls, _, _ = _smoke(tmp_path, BROKEN[name])
    _assert_failed(r)
    assert "SMOKE FAILED" in r.stderr
    assert docker_calls and docker_calls[-1].endswith(" stop"), "the stack is stopped even on failure"


def test_smoke_parses_json_so_field_order_and_spacing_do_not_matter_but_garbage_fails(tmp_path):
    """Superseded by the fix pass: the script used to read id/status with sed and fail closed on reordered or
    pretty-printed JSON (a false alarm, never a false pass). It now parses the JSON, so those replies are read
    correctly, and anything that is not a JSON object with the field still fails."""
    for i, body in enumerate(
        ('{"status":"queued","id":"%s"}' % RID, '{\n  "id": "%s",\n  "status": "queued"\n}' % RID)
    ):
        r, _, _, _ = _smoke(tmp_path / f"ok{i}", {"routes": {"post": {"body": body}}})
        assert r.returncode == 0 and "SMOKE OK" in r.stdout, r.stdout + r.stderr
    for i, body in enumerate(('{"id":"%s"' % RID, '["%s"]' % RID, '{"id":""}', '{"id":12}')):
        r, _, _, _ = _smoke(tmp_path / f"bad{i}", {"routes": {"post": {"body": body}}})
        _assert_failed(r)


@pytest.mark.parametrize("body", ['{"schema_version":"1.1","results":{}}', '{"schema_version":"1.1","results":null}'])
def test_smoke_fails_when_the_results_block_is_empty(tmp_path, body):
    r, _, _, _ = _smoke(tmp_path, {"routes": {"results": {"body": body}}})
    _assert_failed(r)


def test_smoke_never_downs_removes_or_prunes_and_leaves_data_untouched(tmp_path):
    for ci in (True, False):
        sub = tmp_path / ("ci" if ci else "owner")
        sub.mkdir()
        r, docker_calls, _, work = _smoke(sub, {"statuses": ["failed"]}, ci=ci, extra_env={"SMOKE_RUN": "1"})
        _assert_failed(r)
        joined = "\n".join(docker_calls)
        for bad in (" down", " rm", "prune", "--volumes", " -v ", "volume "):
            assert bad not in joined, (bad, joined)
        assert (work / "data" / "background" / "DC_upgrade0.xlsx").read_text() == "OWNER DATA"
        assert sorted(p.name for p in (work / "data").rglob("*")) == ["DC_upgrade0.xlsx", "background"]


def test_owner_mode_stops_only_its_own_project(tmp_path):
    r, docker_calls, curl_calls, work = _smoke(tmp_path, ci=False)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "--project-name" not in "\n".join(docker_calls), "owner mode uses the compose file's own project"
    assert all("deploy/docker-compose.ci.yml" not in c for c in docker_calls)
    assert docker_calls[-1].endswith("-f deploy/docker-compose.yml stop")
    assert not any("POST" in c for c in curl_calls), "owner mode submits no run unless SMOKE_RUN=1"
    assert (work / "deploy" / ".env").is_file()


def test_owner_mode_preflight_refusal_stops_before_docker_and_env_file(tmp_path):
    r, docker_calls, _, work = _smoke(tmp_path, ci=False, extra_env={"PREFLIGHT_MIN_FREE_GB": "99999"})
    assert r.returncode != 0
    assert docker_calls == []
    assert not (work / "deploy" / ".env").exists()


# --- fixture generator: never the owner's data -------------------------------------------------------------------


@pytest.fixture
def fake_repo(tmp_path: Path) -> Path:
    """A repo copy whose data/ holds a sentinel owner file; the script's ROOT is this copy."""
    repo = tmp_path / "repo"
    (repo / "deploy" / "ci").mkdir(parents=True)
    (repo / "tests" / "fixtures").mkdir(parents=True)
    shutil.copy(DEPLOY / "ci" / "make_fixture_data.py", repo / "deploy" / "ci" / "make_fixture_data.py")
    shutil.copy(ROOT / "tests" / "fixtures" / "synthetic_data.py", repo / "tests" / "fixtures" / "synthetic_data.py")
    (repo / "data" / "background").mkdir(parents=True)
    (repo / "data" / "background" / "DC_upgrade0.xlsx").write_text("OWNER DATA")
    return repo


def _gen(repo: Path, target: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(repo / "deploy" / "ci" / "make_fixture_data.py"), str(target)],
        capture_output=True,
        text=True,
        cwd=repo,
        timeout=300,
        check=False,
    )


def _owner_untouched(repo: Path) -> None:
    assert (repo / "data" / "background" / "DC_upgrade0.xlsx").read_bytes() == b"OWNER DATA", "owner file overwritten"
    assert sorted(p.name for p in (repo / "data").rglob("*")) == ["DC_upgrade0.xlsx", "background"]


@pytest.mark.parametrize(
    "target",
    ["data", "data/background", "data/new-sub", "tests/../data", "./data/", "tests/fixtures/../../data/x", "link"],
)
def test_fixture_generator_refuses_the_owner_data_folder(fake_repo, target):
    (fake_repo / "link").symlink_to(fake_repo / "data", target_is_directory=True)
    for t in (target, fake_repo / target):
        r = _gen(fake_repo, t)
        assert r.returncode != 0, (t, r.stdout)
        assert "owner's data" in r.stderr or "not empty" in r.stderr
    _owner_untouched(fake_repo)


def test_fixture_generator_refuses_a_case_variant_that_looks_like_data(fake_repo):
    """On the owner's case-insensitive APFS, DATA is data/. The path comparison is case-sensitive, so the guard that
    must hold is the second one: a folder with background/ and no marker is refused."""
    shutil.copytree(fake_repo / "data", fake_repo / "DATA")
    r = _gen(fake_repo, fake_repo / "DATA")
    assert r.returncode != 0 and "not empty" in r.stderr
    assert (fake_repo / "DATA" / "background" / "DC_upgrade0.xlsx").read_text() == "OWNER DATA"


def test_fixture_generator_never_writes_through_a_planted_symlink(fake_repo):
    target = fake_repo / "tests" / "fixtures" / "data"
    target.mkdir()
    (target / ".synthetic-fixture").write_text("marker")
    (target / "background").symlink_to(fake_repo / "data" / "background", target_is_directory=True)
    r = _gen(fake_repo, target)
    assert r.returncode != 0 and "symlink" in r.stderr
    _owner_untouched(fake_repo)


def test_fixture_generator_refuses_a_nested_symlink_even_with_the_marker(fake_repo, tmp_path):
    target = fake_repo / "tests" / "fixtures" / "data"
    (target / "input" / "resstock").mkdir(parents=True)
    (target / ".synthetic-fixture").write_text("marker")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (target / "input" / "resstock" / "timeseries_individual_buildings").symlink_to(elsewhere, target_is_directory=True)
    r = _gen(fake_repo, target)
    assert r.returncode != 0 and "symlink" in r.stderr
    assert list(elsewhere.iterdir()) == []


# --- worker entrypoint -------------------------------------------------------------------------------------------


def _entrypoint(tmp_path: Path, value: str | None) -> subprocess.CompletedProcess[str]:
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    (bindir / "celery").write_text('#!/bin/sh\necho "CELERY $*"\n')
    (bindir / "celery").chmod(0o755)
    env = {k: v for k, v in os.environ.items() if k != "WORKER_CONCURRENCY"}
    env["PATH"] = f"{bindir}:{os.environ['PATH']}"
    if value is not None:
        env["WORKER_CONCURRENCY"] = value
    return subprocess.run(
        ["sh", str(DEPLOY / "worker-entrypoint.sh")], capture_output=True, text=True, env=env, check=False
    )


@pytest.mark.parametrize("value", ["0", "00", "01", "-1", "+2", "1.5", "2 ", " 2", "abc", "2;id", "$(id)", "1e3"])
def test_worker_entrypoint_rejects_bad_concurrency(tmp_path, value):
    r = _entrypoint(tmp_path, value)
    assert r.returncode == 2, r.stdout
    assert "CELERY" not in r.stdout and "WORKER_CONCURRENCY" in r.stderr


@pytest.mark.parametrize(("value", "want"), [(None, "2"), ("", "2"), ("1", "1"), ("16", "16")])
def test_worker_entrypoint_accepts_good_concurrency(tmp_path, value, want):
    """An empty value means "use the default" (same as compose's ${WORKER_CONCURRENCY:-2}); recorded, not a defect."""
    r = _entrypoint(tmp_path, value)
    assert r.returncode == 0, r.stderr
    assert f"--concurrency={want}" in r.stdout and "--beat" in r.stdout
    assert "--schedule=/tmp/" in r.stdout, "the beat schedule must live where the non-root user can write"


def test_worker_healthcheck_targets_the_default_node_name():
    """The healthcheck pings celery@$HOSTNAME; that is the default node name only if the entrypoint never sets -n."""
    text = (DEPLOY / "worker-entrypoint.sh").read_text()
    code = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
    assert not re.search(r"(\s-n\s|--hostname)", code)
    worker = yaml.safe_load((DEPLOY / "docker-compose.yml").read_text())["services"]["worker"]
    assert "celery@$$HOSTNAME" in " ".join(worker["healthcheck"]["test"])
    assert "read_only" not in worker, "a read-only root would need a tmpfs for /tmp (beat schedule)"
    assert "deploy" not in worker and "scale" not in worker, "exactly one worker (embedded beat)"


# --- nginx ---------------------------------------------------------------------------------------------------------

NGINX_CONF = (DEPLOY / "web" / "nginx.conf").read_text()


def _nginx_code() -> str:
    return "\n".join(ln.split("#", 1)[0] for ln in NGINX_CONF.splitlines())


def test_nginx_proxy_target_is_a_constant_internal_host():
    """No request-controlled variable ($host, $http_*, $arg_*, $uri, ...) may reach the upstream address."""
    code = _nginx_code()
    passes = re.findall(r"proxy_pass\s+([^;]+);", code)
    assert passes == ["$api_upstream"]
    assert re.findall(r"set\s+\$api_upstream\s+([^;]+);", code) == ["http://api:8000"]
    assert code.count("proxy_pass") == 1


def test_nginx_add_header_only_at_server_level_and_csp_needs_no_inline_script():
    code = _nginx_code()
    for block in re.findall(r"location[^{]*\{([^}]*)\}", code):
        assert "add_header" not in block
    csp = re.search(r'Content-Security-Policy\s+"([^"]+)"', code)
    assert csp
    policy = dict((part.split()[0], part.split()[1:]) for part in (p.strip() for p in csp.group(1).split(";")) if part)
    assert "'unsafe-inline'" not in policy["script-src"] and "'unsafe-eval'" not in policy["script-src"]
    assert policy["connect-src"] == ["'self'"], "the API is same-origin through the proxy"
    assert policy["frame-ancestors"] == ["'none'"]
    index = (ROOT / "web" / "index.html").read_text()
    for tag in re.findall(r"<script\b[^>]*>(.*?)</script>", index, flags=re.S):
        assert not tag.strip(), "inline script in web/index.html would be blocked by script-src 'self'"


def test_nginx_sse_timeout_outlasts_the_api_ping():
    code = _nginx_code()
    api_loc = re.search(r"location /api/ \{([^}]*)\}", code)
    assert api_loc
    body = api_loc.group(1)
    assert re.search(r"proxy_buffering\s+off;", body)
    m = re.search(r"proxy_read_timeout\s+(\d+)([smh]);", body)
    assert m
    seconds = int(m.group(1)) * {"s": 1, "m": 60, "h": 3600}[m.group(2)]
    assert seconds > 15, "the API pings SSE streams every 15 s"


# --- build context: nothing secret baked into an image -----------------------------------------------------------


def _dockerignored(path: str) -> bool:
    """Minimal .dockerignore matcher (no negations are used in this repo)."""
    pats = [
        ln.strip() for ln in (ROOT / ".dockerignore").read_text().splitlines() if ln.strip() and not ln.startswith("#")
    ]
    assert not any(p.startswith("!") for p in pats)
    parts = path.split("/")
    prefixes = ["/".join(parts[: i + 1]) for i in range(len(parts))]
    for p in pats:
        for cand in (p, p.removeprefix("**/")):
            if any(fnmatch.fnmatchcase(pre, cand) for pre in prefixes):
                return True
            if p.startswith("**/") and any(fnmatch.fnmatchcase(seg, cand) for seg in parts):
                return True
    return False


def test_dockerignore_matcher_sanity():
    assert _dockerignored("data/x.parquet") and _dockerignored("web/.env") and _dockerignored("deploy/.env")
    assert not _dockerignored("web/src/main.tsx")


@pytest.mark.parametrize(
    "name", ["web/.env.local", "web/.env.production", "web/.env.production.local", ".env.local", "api/.env.prod"]
)
def test_vite_env_files_never_enter_the_build_context(name):
    assert _dockerignored(name)


# --- hardening regressions against the pre-story commit ----------------------------------------------------------


def _at_base(rel: str) -> str:
    r = subprocess.run(["git", "-C", str(ROOT), "show", f"{BASE_COMMIT}:{rel}"], capture_output=True, text=True)
    if r.returncode != 0:
        pytest.skip(f"base commit {BASE_COMMIT} not available: {r.stderr.strip()}")
    return r.stdout


def test_no_hardening_regression_against_the_base_commit():
    old = yaml.safe_load(_at_base("deploy/docker-compose.yml"))["services"]
    new = yaml.safe_load((DEPLOY / "docker-compose.yml").read_text())["services"]
    assert set(old) <= set(new)
    for name, svc in new.items():
        for port in svc.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), (name, port)
        for vol in svc.get("volumes", []):
            if str(vol).split(":")[-2:-1] == ["/data"] or ":/data:" in str(vol):
                assert str(vol).endswith(":ro") or name == "redis", (name, vol)
        assert "mem_limit" in svc, name
        assert "privileged" not in svc and "network_mode" not in svc, name
        before = old.get(name, {})
        if "cap_drop" in before:
            assert svc.get("cap_drop") == before["cap_drop"], name
        if "security_opt" in before:
            assert set(before["security_opt"]) <= set(svc.get("security_opt", [])), name
    for name in ("migrate", "api", "worker", "web"):
        assert new[name].get("cap_drop") == ["ALL"], name
        assert "no-new-privileges:true" in new[name].get("security_opt", []), name


def test_lite_stack_and_destructive_targets_unchanged():
    for rel in ("deploy/docker-compose.lite.yml", "deploy/preflight.sh", "deploy/api.Dockerfile"):
        assert (ROOT / rel).read_text() == _at_base(rel), rel

    def targets(text: str, names: tuple[str, ...]) -> dict[str, str]:
        out: dict[str, str] = {}
        for name in names:
            m = re.search(rf"^{re.escape(name)}:.*?(?=^\S|\Z)", text, flags=re.M | re.S)
            assert m, name
            out[name] = m.group(0)
        return out

    names = ("lite-up", "lite-down", "lite-nuke", "nuke", "down")
    old, new = targets(_at_base("Makefile"), names), targets((ROOT / "Makefile").read_text(), names)
    assert old == new
    assert "Type DELETE to continue" in new["nuke"] and '[ "$$ans" = "DELETE" ]' in new["nuke"]


def test_no_secret_is_baked_into_an_image():
    for df in ("deploy/api.Dockerfile", "deploy/web.Dockerfile"):
        text = (ROOT / df).read_text()
        for line in text.splitlines():
            if line.strip().startswith(("ENV", "ARG")):
                assert not re.search(r"(SECRET|PASSWORD|API_KEY|TOKEN|JWT)", line, re.I), (df, line)
        assert "deploy/.env" not in re.sub(r"#.*", "", text)
    web_df = (ROOT / "deploy" / "web.Dockerfile").read_text()
    assert "VITE_API_MODE" not in re.sub(r"#.*", "", web_df), "the image must not select the mock client"


@pytest.mark.xfail(
    strict=True,
    reason="CROSS-BRANCH (integration tripwire): on this branch alone web/src/main.tsx hard-codes createMockClient, "
    "so the 'real' web image serves the mock UI and the runbook/web.Dockerfile claim ('talks to the real API, not the "
    "mock') is false. The W1-WEB branch adds src/api/select.ts (VITE_API_MODE). This XPASSes after the merge: then "
    "drop the xfail marker.",
)
def test_web_image_entry_point_selects_the_real_client():
    main = (ROOT / "web" / "src" / "main.tsx").read_text()
    assert "createMockClient(" not in main
    assert "VITE_API_MODE" in main or "createClientFromEnv" in main


# --- runbook --------------------------------------------------------------------------------------------------------


def test_runbook_make_targets_exist():
    runbook = (ROOT / "docs" / "runbooks" / "local-stack.md").read_text()
    makefile = (ROOT / "Makefile").read_text()
    defined = set(re.findall(r"^([a-z][\w-]*):", makefile, flags=re.M))
    used = set(re.findall(r"\bmake (?:-n )?([a-z][\w-]*)", runbook))
    assert used - defined == set(), used - defined


def test_runbook_describes_the_real_missing_api_keys_failure():
    runbook = (ROOT / "docs" / "runbooks" / "local-stack.md").read_text()
    assert "exits with code 2" not in runbook or "missing a value" in runbook
    # Prominent (before the first section) and actionable: the real compose message and the exact line to add.
    top = runbook.split("\n## ", 1)[0]
    assert "required variable API_KEYS is missing a value" in top
    template_line = next(ln for ln in (DEPLOY / ".env.example").read_text().splitlines() if ln.startswith("API_KEYS="))
    assert template_line in top, "the runbook must give the exact line from the template"


def test_compose_refuses_an_env_file_without_api_keys(tmp_path):
    """Fail-closed check on the real compose CLI (no daemon needed): an older deploy/.env lacking API_KEYS."""
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not installed")
    old_env = "\n".join(
        ln for ln in (DEPLOY / ".env.example").read_text().splitlines() if not ln.startswith("API_KEYS=")
    )
    env_file = tmp_path / "old.env"
    env_file.write_text(old_env + "\n")
    env = {k: v for k, v in os.environ.items() if k != "API_KEYS"}
    r = subprocess.run(
        ["docker", "compose", "--project-directory", str(ROOT), "--env-file", str(env_file),
         "-f", str(DEPLOY / "docker-compose.yml"), "config", "-q"],
        capture_output=True, text=True, env=env, check=False,
    )  # fmt: skip
    if "unknown" in r.stderr and "compose" in r.stderr:
        pytest.skip("docker compose plugin not installed")
    assert r.returncode != 0
    assert "API_KEYS" in r.stderr
