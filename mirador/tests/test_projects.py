"""Tests for project storage and CRUD API."""

import pytest
from httpx import AsyncClient, ASGITransport

from mirador.app import app
from mirador.storage.projects import ProjectStore


# ---------------------------------------------------------------------------
# Storage layer tests
# ---------------------------------------------------------------------------


class TestProjectStore:
    def test_create_project(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        meta = store.create_project("My Project")
        assert meta["name"] == "My Project"
        assert meta["slug"] == "my_project"
        assert "created_at" in meta
        # Verify directory structure
        proj_dir = tmp_path / "projects" / "my_project"
        assert proj_dir.is_dir()
        assert (proj_dir / "meta.json").exists()
        assert (proj_dir / "data").is_dir()
        assert (proj_dir / "outputs").is_dir()
        assert (proj_dir / "pipelines").is_dir()

    def test_list_projects(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        store.create_project("Alpha")
        store.create_project("Beta")
        projects = store.list_projects()
        slugs = [p["slug"] for p in projects]
        assert "alpha" in slugs
        assert "beta" in slugs
        assert len(projects) == 2

    def test_get_project(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        store.create_project("Test Proj")
        meta = store.get_project("test_proj")
        assert meta is not None
        assert meta["name"] == "Test Proj"
        assert meta["slug"] == "test_proj"

    def test_get_project_not_found(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        assert store.get_project("nonexistent") is None

    def test_delete_project(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        store.create_project("Doomed")
        assert store.delete_project("doomed") is True
        assert not (tmp_path / "projects" / "doomed").exists()

    def test_delete_project_not_found(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        assert store.delete_project("nonexistent") is False

    def test_duplicate_project_raises(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        store.create_project("Dup")
        with pytest.raises(ValueError, match="already exists"):
            store.create_project("Dup")

    def test_save_load_pipeline(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        store.create_project("Proj")
        pipeline = {"nodes": [{"id": "n1", "type": "csv_source"}], "edges": []}
        store.save_pipeline("proj", "my_pipe", pipeline)
        loaded = store.load_pipeline("proj", "my_pipe")
        assert loaded == pipeline

    def test_list_pipelines(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        store.create_project("Proj")
        store.save_pipeline("proj", "pipe_a", {"nodes": [], "edges": []})
        store.save_pipeline("proj", "pipe_b", {"nodes": [], "edges": []})
        names = store.list_pipelines("proj")
        assert "pipe_a" in names
        assert "pipe_b" in names
        assert len(names) == 2

    def test_delete_pipeline(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        store.create_project("Proj")
        store.save_pipeline("proj", "to_delete", {"nodes": [], "edges": []})
        assert store.delete_pipeline("proj", "to_delete") is True
        assert store.load_pipeline("proj", "to_delete") is None

    def test_delete_pipeline_not_found(self, tmp_path):
        store = ProjectStore(root=tmp_path)
        store.create_project("Proj")
        assert store.delete_pipeline("proj", "no_such") is False


# ---------------------------------------------------------------------------
# API tests
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    from mirador.api import projects as proj_mod

    s = ProjectStore(root=tmp_path)
    proj_mod._store = s
    yield s
    proj_mod._store = None


@pytest.mark.asyncio
async def test_api_create_project(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/api/projects", json={"name": "New Project"})
    assert r.status_code == 201
    body = r.json()
    assert body["name"] == "New Project"
    assert body["slug"] == "new_project"


@pytest.mark.asyncio
async def test_api_create_duplicate_project(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/api/projects", json={"name": "Dup"})
        r = await client.post("/api/projects", json={"name": "Dup"})
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_api_list_projects(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/api/projects", json={"name": "A"})
        await client.post("/api/projects", json={"name": "B"})
        r = await client.get("/api/projects")
    assert r.status_code == 200
    slugs = [p["slug"] for p in r.json()]
    assert "a" in slugs
    assert "b" in slugs


@pytest.mark.asyncio
async def test_api_get_project(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/api/projects", json={"name": "Fetch Me"})
        r = await client.get("/api/projects/fetch_me")
    assert r.status_code == 200
    assert r.json()["name"] == "Fetch Me"


@pytest.mark.asyncio
async def test_api_get_project_not_found(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/projects/nonexistent")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_api_delete_project(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/api/projects", json={"name": "Gone"})
        r = await client.delete("/api/projects/gone")
    assert r.status_code == 200
    assert r.json()["status"] == "deleted"


@pytest.mark.asyncio
async def test_api_delete_project_not_found(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.delete("/api/projects/nonexistent")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_api_save_and_get_pipeline(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/api/projects", json={"name": "Pipes"})
        pipeline = {"nodes": [{"id": "n1", "type": "csv_source"}], "edges": []}
        r = await client.put("/api/projects/pipes/pipelines/test_pipe", json=pipeline)
        assert r.status_code == 200
        assert r.json()["status"] == "saved"

        r = await client.get("/api/projects/pipes/pipelines/test_pipe")
        assert r.status_code == 200
        body = r.json()
        assert len(body["nodes"]) == 1
        assert body["nodes"][0]["id"] == "n1"


@pytest.mark.asyncio
async def test_api_save_pipeline_project_not_found(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        pipeline = {"nodes": [], "edges": []}
        r = await client.put("/api/projects/nope/pipelines/p", json=pipeline)
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_api_get_pipeline_not_found(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/api/projects", json={"name": "Empty"})
        r = await client.get("/api/projects/empty/pipelines/missing")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_api_delete_pipeline(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/api/projects", json={"name": "Del"})
        pipeline = {"nodes": [], "edges": []}
        await client.put("/api/projects/del/pipelines/bye", json=pipeline)
        r = await client.delete("/api/projects/del/pipelines/bye")
    assert r.status_code == 200
    assert r.json()["status"] == "deleted"


@pytest.mark.asyncio
async def test_api_delete_pipeline_not_found(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/api/projects", json={"name": "No Pipe"})
        r = await client.delete("/api/projects/no_pipe/pipelines/nope")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_api_list_pipelines(init_teide, store):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/api/projects", json={"name": "Multi"})
        await client.put(
            "/api/projects/multi/pipelines/p1", json={"nodes": [], "edges": []}
        )
        await client.put(
            "/api/projects/multi/pipelines/p2", json={"nodes": [], "edges": []}
        )
        r = await client.get("/api/projects/multi/pipelines")
    assert r.status_code == 200
    names = r.json()
    assert "p1" in names
    assert "p2" in names
