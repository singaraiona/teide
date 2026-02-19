"""Pipeline API — run pipelines."""

import json
import queue
import threading
from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
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
    session_id: str | None = None
    start_from: str | None = None


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


@router.post("/run-stream")
def run_pipeline_stream(payload: PipelinePayload):
    """Execute a pipeline with SSE progress events."""
    pipeline = {
        "nodes": [n.model_dump() for n in payload.nodes],
        "edges": [e.model_dump() for e in payload.edges],
    }

    event_queue: queue.Queue[dict | None] = queue.Queue()

    def on_node_start(node_id: str) -> None:
        event_queue.put({"type": "node_start", "node_id": node_id})

    def on_node_done(node_id: str, output: dict) -> None:
        safe = {k: v for k, v in output.items() if k != "df"}
        event_queue.put({"type": "node_done", "node_id": node_id, **safe})

    def on_node_error(node_id: str, exc: Exception) -> None:
        event_queue.put({"type": "node_error", "node_id": node_id,
                         "error": str(exc)})

    def run_in_thread() -> None:
        try:
            registry = get_registry()
            executor = PipelineExecutor(registry)
            results = executor.run(
                pipeline,
                on_node_start=on_node_start,
                on_node_done=on_node_done,
                on_node_error=on_node_error,
                session_id=payload.session_id,
                start_from=payload.start_from,
            )
            event_queue.put({"type": "complete",
                             "results": _serialize_results(results)})
        except Exception as exc:
            event_queue.put({"type": "error", "error": str(exc)})
        finally:
            event_queue.put(None)  # sentinel

    threading.Thread(target=run_in_thread, daemon=True).start()

    def event_generator():
        while True:
            event = event_queue.get()
            if event is None:
                break
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _serialize_results(results: dict[str, Any]) -> dict[str, Any]:
    """Remove non-serializable objects (Table), keep JSON-safe data."""
    clean: dict[str, Any] = {}
    for node_id, output in results.items():
        clean[node_id] = {
            k: v for k, v in output.items()
            if k != "df"  # exclude raw Table objects
        }
    return clean
