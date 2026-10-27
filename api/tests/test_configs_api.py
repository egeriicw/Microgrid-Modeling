"""FR-A2 / A4 / A5 — config CRUD, versioning, clone, YAML round-trip.
RED placeholder (docs/test-plan.md §4 Phase 2).
"""
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("microgrid_api", reason="Phase 2: api not implemented")

pytestmark = [pytest.mark.anyio, pytest.mark.xfail(reason="FR-A2 not implemented", strict=False)]

SCENARIO_DIR = Path(__file__).resolve().parents[2] / "configs" / "scenarios"


def _valid_body() -> dict:
    return {
        "name": "unit-test",
        "composition": {
            "total_buildings": 10,
            "method": "dirichlet",
            "categories": {
                "mf_small": {"min_fraction": 0.5, "max_fraction": 1.0, "source": "resstock"},
                "mf_mid": {"min_fraction": 0.0, "max_fraction": 0.5, "source": "resstock"},
            },
        },
        "ensemble": {"mode": "fixed", "max_runs": 5},
    }


async def test_create_requires_token(client):
    r = await client.post("/configs", json={"name": "x", "body": _valid_body()})
    assert r.status_code == 401


async def test_create_and_get(client, auth_headers):
    r = await client.post("/configs", headers=auth_headers, json={"name": "cfg-a", "body": _valid_body()})
    assert r.status_code == 201
    cid = r.json()["id"]
    assert r.json()["version"] == 1

    r2 = await client.get(f"/configs/{cid}")
    assert r2.status_code == 200
    assert r2.json()["body"]["composition"]["total_buildings"] == 10


async def test_create_rejects_invalid_body(client, auth_headers):
    bad = _valid_body()
    bad["composition"]["categories"]["mf_small"]["min_fraction"] = 0.9  # sum(min) > 1
    r = await client.post("/configs", headers=auth_headers, json={"name": "bad", "body": bad})
    assert r.status_code == 422
    assert any("min_fraction" in "/".join(map(str, f["loc"])) for f in r.json()["error"]["fields"])


async def test_put_creates_new_version(client, auth_headers):
    cid = (await client.post("/configs", headers=auth_headers, json={"name": "v", "body": _valid_body()})).json()["id"]
    body = _valid_body()
    body["composition"]["total_buildings"] = 20
    r = await client.put(f"/configs/{cid}", headers=auth_headers, json={"body": body})
    assert r.status_code == 200
    assert r.json()["version"] == 2

    versions = (await client.get(f"/configs/{cid}/versions")).json()["items"]
    assert [v["version"] for v in versions] == [1, 2]


async def test_delete_conflicts_when_runs_exist(client, auth_headers):
    cid = (await client.post("/configs", headers=auth_headers, json={"name": "d", "body": _valid_body()})).json()["id"]
    await client.post("/runs", headers=auth_headers, json={"config_id": cid})
    r = await client.delete(f"/configs/{cid}", headers=auth_headers)
    assert r.status_code == 409


async def test_clone(client, auth_headers):
    cid = (await client.post("/configs", headers=auth_headers, json={"name": "orig", "body": _valid_body()})).json()["id"]
    r = await client.post(f"/configs/{cid}/clone", headers=auth_headers, json={"name": "copy"})
    assert r.status_code == 201
    assert r.json()["name"] == "copy"
    assert r.json()["version"] == 1


@pytest.mark.parametrize("yaml_path", sorted(SCENARIO_DIR.glob("*.yaml")), ids=lambda p: p.name)
async def test_yaml_roundtrip(client, auth_headers, yaml_path):
    text = yaml_path.read_text()
    r = await client.post("/configs/import", headers={**auth_headers, "Content-Type": "text/yaml"}, content=text)
    assert r.status_code == 201, r.text
    cid = r.json()["id"]

    exported = await client.get(f"/configs/{cid}/export")
    assert exported.status_code == 200
    assert exported.headers["content-type"].startswith("text/yaml")

    # re-import the export; bodies must be equivalent
    r2 = await client.post("/configs/import", headers={**auth_headers, "Content-Type": "text/yaml"}, content=exported.text)
    assert r2.json()["body"] == r.json()["body"]
