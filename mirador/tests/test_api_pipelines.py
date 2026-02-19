import os
import tempfile
import pytest
from httpx import AsyncClient, ASGITransport
from mirador.app import app


@pytest.mark.asyncio
async def test_list_node_types(init_teide):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/nodes")
        assert r.status_code == 200
        data = r.json()
        ids = [n["id"] for n in data]
        assert "csv_source" in ids
        assert "grid" in ids
        assert "query" in ids


@pytest.mark.asyncio
async def test_run_pipeline(init_teide):
    data_csv = "x,y\n1,10\n2,20\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data_csv)
        csv_path = f.name

    try:
        payload = {
            "nodes": [
                {"id": "n1", "type": "csv_source", "config": {"file_path": csv_path}},
                {"id": "n2", "type": "grid", "config": {}},
            ],
            "edges": [{"source": "n1", "target": "n2"}],
        }
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/api/pipelines/run", json=payload)
            assert r.status_code == 200
            data = r.json()
            assert "n2" in data
            assert data["n2"]["total"] == 2
            # Should not contain "df" key (non-serializable)
            assert "df" not in data.get("n1", {})
    finally:
        os.unlink(csv_path)


@pytest.mark.asyncio
async def test_run_pipeline_with_query_groupby(init_teide):
    data_csv = "id,val\na,10\na,20\nb,30\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data_csv)
        csv_path = f.name

    try:
        payload = {
            "nodes": [
                {"id": "src", "type": "csv_source", "config": {"file_path": csv_path}},
                {"id": "qry", "type": "query", "config": {
                    "mode": "form",
                    "groupby": {
                        "keys": ["id"],
                        "aggs": [{"column": "val", "op": "sum"}],
                    },
                }},
                {"id": "out", "type": "grid", "config": {}},
            ],
            "edges": [
                {"source": "src", "target": "qry"},
                {"source": "qry", "target": "out"},
            ],
        }
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/api/pipelines/run", json=payload)
            assert r.status_code == 200
            data = r.json()
            assert data["out"]["total"] == 2
    finally:
        os.unlink(csv_path)
