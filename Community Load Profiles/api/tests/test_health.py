"""FR-A1 — health & readiness. RED placeholder (docs/test-plan.md §4 Phase 2)."""
from __future__ import annotations

import pytest

pytest.importorskip("microgrid_api", reason="Phase 2: api not implemented")

pytestmark = [pytest.mark.anyio, pytest.mark.xfail(reason="FR-A1 not implemented", strict=False)]


async def test_health_always_ok(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_ready_ok_when_deps_up(client):
    r = await client.get("/ready")
    assert r.status_code == 200
    body = r.json()
    assert body["ready"] is True
    assert body["checks"] == {"db": "ok", "redis": "ok", "storage": "ok"}


async def test_ready_503_when_a_dependency_is_down(client, monkeypatch):
    monkeypatch.setattr("microgrid_api.routers.health._check_redis", lambda: "unreachable")
    r = await client.get("/ready")
    assert r.status_code == 503
    body = r.json()
    assert body["ready"] is False
    assert body["checks"]["redis"] == "unreachable"
