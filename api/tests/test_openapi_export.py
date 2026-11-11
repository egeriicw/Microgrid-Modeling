"""Story A4 (part): the committed OpenAPI schema must match the app (the frontend generates its client from it)."""

from __future__ import annotations

import json

from microgrid_api import openapi_export


def test_committed_schema_is_current():
    assert openapi_export.OPENAPI_PATH.exists(), "run: uv run --project api python -m microgrid_api.openapi_export"
    assert openapi_export.OPENAPI_PATH.read_text() == openapi_export.render(), (
        "api/openapi.json is stale; regenerate with: uv run --project api python -m microgrid_api.openapi_export"
    )


def test_schema_documents_run_endpoints_and_bearer_auth():
    spec = json.loads(openapi_export.render())
    paths = spec["paths"]
    assert "post" in paths["/api/v1/runs"] and "get" in paths["/api/v1/runs"]
    assert "get" in paths["/api/v1/runs/{run_id}"]
    assert "get" in paths["/api/v1/runs/{run_id}/events"]
    assert "post" in paths["/api/v1/runs/{run_id}/cancel"]
    assert "HTTPBearer" in spec["components"]["securitySchemes"]


def test_render_is_deterministic():
    assert openapi_export.render() == openapi_export.render()


def test_check_mode_detects_stale_file(tmp_path):
    stale = tmp_path / "openapi.json"
    stale.write_text("{}")
    assert openapi_export.main(["--check", "--path", str(stale)]) == 1
    assert openapi_export.main(["--path", str(stale)]) == 0
    assert openapi_export.main(["--check", "--path", str(stale)]) == 0
