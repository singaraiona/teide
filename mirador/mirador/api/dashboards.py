"""Dashboard API — CRUD for dashboards and data refresh."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from mirador.storage.projects import ProjectStore

router = APIRouter(prefix="/api/projects", tags=["dashboards"])

_store: ProjectStore | None = None


def get_store() -> ProjectStore:
    global _store
    if _store is None:
        _store = ProjectStore()
    return _store


class SaveDashboardRequest(BaseModel):
    name: str
    data_sources: list[dict] = []
    widgets: list[dict] = []
    grid_cols: int = 12


@router.get("/{slug}/dashboards")
def list_dashboards(slug: str):
    return get_store().list_dashboards(slug)


@router.get("/{slug}/dashboards/{name}")
def get_dashboard(slug: str, name: str):
    dashboard = get_store().load_dashboard(slug, name)
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return dashboard


@router.put("/{slug}/dashboards/{name}")
def save_dashboard(slug: str, name: str, body: SaveDashboardRequest):
    store = get_store()
    if not store.get_project(slug):
        raise HTTPException(status_code=404, detail="Project not found")
    store.save_dashboard(slug, name, body.model_dump())
    return {"status": "saved"}


@router.delete("/{slug}/dashboards/{name}")
def delete_dashboard(slug: str, name: str):
    if not get_store().delete_dashboard(slug, name):
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return {"status": "deleted"}


@router.post("/{slug}/dashboards/{name}/refresh")
def refresh_dashboard(slug: str, name: str):
    """Run source workflows and return fresh data for dashboard widgets."""
    store = get_store()
    dashboard = store.load_dashboard(slug, name)
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    # For each data source, load and run the source pipeline
    data: dict[str, dict] = {}
    for ds in dashboard.get("data_sources", []):
        alias = ds.get("alias", "")
        workflow_name = ds.get("workflow_name", "")
        node_id = ds.get("node_id", "")

        pipeline = store.load_pipeline(slug, workflow_name)
        if not pipeline:
            data[alias] = {"rows": [], "columns": [], "error": f"Workflow '{workflow_name}' not found"}
            continue

        # Execute the pipeline to get results
        try:
            from mirador.engine.executor import PipelineExecutor
            from mirador.api.nodes import get_registry
            # Transform React Flow format to executor format
            exec_pipeline = {
                "nodes": [
                    {"id": n["id"], "type": n["data"]["nodeType"], "config": n["data"].get("config", {})}
                    for n in pipeline.get("nodes", [])
                ],
                "edges": [
                    {"source": e["source"], "target": e["target"]}
                    for e in pipeline.get("edges", [])
                ],
            }
            executor = PipelineExecutor(get_registry())
            results = executor.run(exec_pipeline)
            node_result = results.get(node_id, {})
            # Extract actual row data from the Table df object
            table = node_result.get("df")
            if table is not None and hasattr(table, "to_dict"):
                columns = node_result.get("columns", table.columns if hasattr(table, "columns") else [])
                table_data = table.to_dict()
                n = len(table)
                rows = [{col: table_data[col][i] for col in columns} for i in range(min(n, 500))]
                data[alias] = {"rows": rows, "columns": list(columns)}
            else:
                data[alias] = {
                    "rows": node_result.get("rows", []),
                    "columns": node_result.get("columns", []),
                }
        except Exception as e:
            data[alias] = {"rows": [], "columns": [], "error": str(e)}

    return data
