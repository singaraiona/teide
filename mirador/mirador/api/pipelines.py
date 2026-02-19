"""Pipeline API — run pipelines."""

from typing import Any
from fastapi import APIRouter
from pydantic import BaseModel, Field

from mirador.engine.executor import PipelineExecutor
from mirador.api.nodes import get_registry


router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])


class PipelineNode(BaseModel):
    id: str
    type: str
    config: dict[str, Any] = Field(default_factory=dict)


class PipelineEdge(BaseModel):
    source: str
    target: str


class PipelinePayload(BaseModel):
    nodes: list[PipelineNode]
    edges: list[PipelineEdge] = Field(default_factory=list)


@router.post("/run")
def run_pipeline(payload: PipelinePayload):
    """Execute a pipeline and return results for each node."""
    pipeline = {
        "nodes": [n.model_dump() for n in payload.nodes],
        "edges": [e.model_dump() for e in payload.edges],
    }
    registry = get_registry()
    executor = PipelineExecutor(registry)
    results = executor.run(pipeline)
    return _serialize_results(results)


def _serialize_results(results: dict[str, Any]) -> dict[str, Any]:
    """Remove non-serializable objects (Table), keep JSON-safe data."""
    clean: dict[str, Any] = {}
    for node_id, output in results.items():
        clean[node_id] = {
            k: v for k, v in output.items()
            if k != "df"  # exclude raw Table objects
        }
    return clean
