"""Tests for the /api/health endpoint."""

import pytest
import httpx
from httpx import ASGITransport

from mirador.app import app


@pytest.mark.asyncio
async def test_health(init_teide):
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert "version" in body
    assert "teide" in body
    assert body["status"] == "ok"
    assert body["teide"] is True
