"""WebSocket endpoint — runs pipelines with per-node status updates."""

import asyncio
import queue
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from mirador.engine.executor import PipelineExecutor
from mirador.api.nodes import get_registry


router = APIRouter()


def _serialize_preview(output: dict[str, Any]) -> dict[str, Any]:
    """Strip non-serializable keys (like 'df') from node output."""
    return {k: v for k, v in output.items() if k != "df"}


def _serialize_results(results: dict[str, Any]) -> dict[str, Any]:
    """Remove non-serializable objects from all node results."""
    return {
        node_id: _serialize_preview(output)
        for node_id, output in results.items()
    }


@router.websocket("/ws/run")
async def ws_run(ws: WebSocket):
    """Run a pipeline over WebSocket, streaming per-node status messages."""
    await ws.accept()

    try:
        payload = await ws.receive_json()
    except WebSocketDisconnect:
        return

    # Validate minimal structure
    if not isinstance(payload, dict) or "nodes" not in payload:
        await ws.send_json({"type": "error", "error": "Invalid pipeline payload"})
        await ws.close()
        return

    pipeline = {
        "nodes": payload["nodes"],
        "edges": payload.get("edges", []),
    }

    # Queue for sync callbacks -> async WebSocket sender
    msg_queue: queue.Queue[dict[str, Any]] = queue.Queue()

    def on_node_start(node_id: str) -> None:
        msg_queue.put({"type": "node_start", "node_id": node_id})

    def on_node_done(node_id: str, output: dict[str, Any]) -> None:
        msg_queue.put({
            "type": "node_done",
            "node_id": node_id,
            "preview": _serialize_preview(output),
        })

    def on_node_error(node_id: str, exc: Exception) -> None:
        msg_queue.put({
            "type": "node_error",
            "node_id": node_id,
            "error": str(exc),
        })

    registry = get_registry()
    executor = PipelineExecutor(registry)

    # Run executor in a thread (it is synchronous)
    async def run_executor() -> dict[str, Any]:
        return await asyncio.to_thread(
            executor.run,
            pipeline,
            on_node_start=on_node_start,
            on_node_done=on_node_done,
            on_node_error=on_node_error,
        )

    # Start executor task
    executor_task = asyncio.create_task(run_executor())

    # Drain messages from the queue until executor finishes
    try:
        while not executor_task.done():
            # Poll the queue with a short timeout to allow checking executor_task
            try:
                msg = await asyncio.to_thread(msg_queue.get, timeout=0.05)
                await ws.send_json(msg)
            except queue.Empty:
                continue

        # Drain any remaining messages after the executor finishes
        while not msg_queue.empty():
            msg = msg_queue.get_nowait()
            await ws.send_json(msg)

        results = await executor_task
        await ws.send_json({
            "type": "pipeline_done",
            "results": _serialize_results(results),
        })
    except WebSocketDisconnect:
        executor_task.cancel()
        return
    except Exception as exc:
        try:
            await ws.send_json({"type": "error", "error": str(exc)})
        except Exception:
            pass

    await ws.close()
