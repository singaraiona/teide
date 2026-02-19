"""Projects API — CRUD for projects and their pipelines."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from mirador.storage.projects import ProjectStore

router = APIRouter(prefix="/api/projects", tags=["projects"])

_store: ProjectStore | None = None


def get_store() -> ProjectStore:
    global _store
    if _store is None:
        _store = ProjectStore()
    return _store


class CreateProjectRequest(BaseModel):
    name: str


class SavePipelineRequest(BaseModel):
    nodes: list[dict]
    edges: list[dict] = []


@router.get("")
def list_projects():
    return get_store().list_projects()


@router.post("", status_code=201)
def create_project(body: CreateProjectRequest):
    try:
        return get_store().create_project(body.name)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.get("/{slug}")
def get_project(slug: str):
    project = get_store().get_project(slug)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.delete("/{slug}")
def delete_project(slug: str):
    if not get_store().delete_project(slug):
        raise HTTPException(status_code=404, detail="Project not found")
    return {"status": "deleted"}


@router.get("/{slug}/pipelines")
def list_pipelines(slug: str):
    return get_store().list_pipelines(slug)


@router.get("/{slug}/pipelines/{name}")
def get_pipeline(slug: str, name: str):
    pipeline = get_store().load_pipeline(slug, name)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline


@router.put("/{slug}/pipelines/{name}")
def save_pipeline(slug: str, name: str, body: SavePipelineRequest):
    store = get_store()
    if not store.get_project(slug):
        raise HTTPException(status_code=404, detail="Project not found")
    store.save_pipeline(slug, name, body.model_dump())
    return {"status": "saved"}


@router.delete("/{slug}/pipelines/{name}")
def delete_pipeline(slug: str, name: str):
    if not get_store().delete_pipeline(slug, name):
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return {"status": "deleted"}
