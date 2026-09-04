"""FR-A3 / FR-A16 — /configs/validate always 200; verdict matches the
engine (snlg.config). Schema-parity across shipped scenario YAMLs.
RED placeholder (docs/test-plan.md §4 Phase 2).
"""
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("microgrid_api", reason="Phase 2: validation not implemented")

pytestmark = [pytest.mark.anyio, pytest.mark.xfail(reason="FR-A3 not implemented", strict=False)]

SCENARIO_DIR = Path(__file__).resolve().parents[2] / "configs" / "scenarios"


async def test_validate_always_200_valid(client):
    body = {
        "name": "ok",
        "composition": {
            "total_buildings": 10, "method": "dirichlet",
            "categories": {
                "mf_small": {"min_fraction": 0.5, "max_fraction": 1.0, "source": "resstock"},
                "mf_mid": {"min_fraction": 0.0, "max_fraction": 0.5, "source": "resstock"},
            },
        },
        "ensemble": {"mode": "fixed", "max_runs": 5},
    }
    r = await client.post("/configs/validate", json={"body": body})
    assert r.status_code == 200
    assert r.json() == {"valid": True, "errors": []}


async def test_validate_reports_simplex_infeasible(client):
    body = {
        "name": "bad",
        "composition": {
            "total_buildings": 10, "method": "dirichlet",
            "categories": {
                "a": {"min_fraction": 0.7, "max_fraction": 0.8, "source": "resstock"},
                "b": {"min_fraction": 0.7, "max_fraction": 0.8, "source": "resstock"},
            },
        },
    }
    r = await client.post("/configs/validate", json={"body": body})
    assert r.status_code == 200
    out = r.json()
    assert out["valid"] is False
    assert any("min_fraction" in e["msg"] for e in out["errors"])


@pytest.mark.parametrize("yaml_path", sorted(SCENARIO_DIR.glob("*.yaml")), ids=lambda p: p.name)
async def test_api_verdict_matches_engine(client, auth_headers, yaml_path):
    """The Pydantic validator and snlg.config.load_config must agree."""
    from snlg.config import load_config

    engine_ok = True
    try:
        load_config(yaml_path)
    except Exception:
        engine_ok = False

    imp = await client.post(
        "/configs/import",
        headers={**auth_headers, "Content-Type": "text/yaml"},
        content=yaml_path.read_text(),
    )
    api_ok = imp.status_code == 201
    assert api_ok == engine_ok, f"{yaml_path.name}: api={api_ok} engine={engine_ok}"
