"""FR-A6 / A7 / A8 / A9 — run lifecycle: create+enqueue, list, detail,
SSE progress, cancel. RED placeholder (docs/test-plan.md §4 Phase 3).
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("microgrid_api", reason="Phase 3: runs not implemented")

pytestmark = [pytest.mark.anyio, pytest.mark.xfail(reason="FR-A6 not implemented", strict=False)]


def _cfg_body() -> dict:
    return {
        "name": "run-cfg",
        "composition": {
            "total_buildings": 4, "method": "dirichlet",
            "categories": {
                "mf_small": {"min_fraction": 0.5, "max_fraction": 1.0, "source": "resstock"},
                "mf_mid": {"min_fraction": 0.0, "max_fraction": 0.5, "source": "resstock"},
            },
        },
        "ensemble": {"mode": "fixed", "max_runs": 6},
        "perturbation": {"enabled": False},
    }


@pytest.fixture
async def config_id(client, auth_headers) -> int:
    r = await client.post("/configs", headers=auth_headers, json={"name": "rc", "body": _cfg_body()})
    return r.json()["id"]


async def test_create_run_enqueues_and_freezes_snapshot(client, auth_headers, config_id, fake_queue):
    r = await client.post(
        "/runs", headers=auth_headers,
        json={"config_id": config_id, "overrides": {"seeds": {"composition_seed": 7, "archetype_seed": 8, "perturbation_seed": 9}}},
    )
    assert r.status_code == 202
    run_id = r.json()["run_id"]
    assert r.json()["status"] == "queued"

    detail = (await client.get(f"/runs/{run_id}")).json()
    assert detail["seeds"]["composition_seed"] == 7
    assert detail["engine_version"]
    assert detail["config_snapshot"]["composition"]["total_buildings"] == 4
    assert len(fake_queue.jobs) == 1  # enqueued exactly once


async def test_create_run_unknown_config_404(client, auth_headers):
    r = await client.post("/runs", headers=auth_headers, json={"config_id": 999999})
    assert r.status_code == 404


async def test_max_runs_over_ceiling_422(client, auth_headers, config_id):
    r = await client.post(
        "/runs", headers=auth_headers,
        json={"config_id": config_id, "overrides": {"ensemble": {"max_runs": 10_000}}},
    )
    assert r.status_code == 422


async def test_list_runs_filters(client, auth_headers, config_id):
    await client.post("/runs", headers=auth_headers, json={"config_id": config_id})
    r = await client.get("/runs", params={"active": True, "config_id": config_id})
    assert r.status_code == 200
    items = r.json()["items"]
    assert items and all(i["status"] in ("queued", "running") for i in items)
    assert all("progress" in i for i in items)


async def test_sse_stream_replays_then_terminates(client, auth_headers, config_id):
    run_id = (await client.post("/runs", headers=auth_headers, json={"config_id": config_id})).json()["run_id"]
    events = []
    async with client.stream("GET", f"/runs/{run_id}/events") as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        async for line in resp.aiter_lines():
            if line.startswith("data:"):
                ev = json.loads(line[5:])
                events.append(ev)
                if ev["stage"] in {"done", "failed", "cancelled"}:
                    break
    assert events[0]["stage"] in {"queued", "running"}
    assert events[-1]["stage"] in {"done", "failed", "cancelled"}


async def test_cancel_queued_run(client, auth_headers, config_id):
    run_id = (await client.post("/runs", headers=auth_headers, json={"config_id": config_id})).json()["run_id"]
    r = await client.post(f"/runs/{run_id}/cancel", headers=auth_headers)
    assert r.status_code == 202
    assert (await client.get(f"/runs/{run_id}")).json()["status"] == "cancelled"


async def test_cancel_terminal_run_409(client, auth_headers, config_id, fake_queue):
    # fake_queue is synchronous -> run finishes immediately
    run_id = (await client.post("/runs", headers=auth_headers, json={"config_id": config_id})).json()["run_id"]
    r = await client.post(f"/runs/{run_id}/cancel", headers=auth_headers)
    assert r.status_code == 409
