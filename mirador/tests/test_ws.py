"""Tests for the /ws/run WebSocket endpoint."""

import os
import tempfile

from starlette.testclient import TestClient

from mirador.app import app


def _make_csv(content: str) -> str:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


def test_ws_csv_to_grid(init_teide):
    """Connect via WebSocket, run CSV->Grid pipeline, verify status messages."""
    csv_path = _make_csv("x,y\n1,10\n2,20\n")
    try:
        payload = {
            "nodes": [
                {"id": "n1", "type": "csv_source", "config": {"file_path": csv_path}},
                {"id": "n2", "type": "grid", "config": {}},
            ],
            "edges": [{"source": "n1", "target": "n2"}],
        }
        client = TestClient(app)
        with client.websocket_connect("/ws/run") as ws:
            ws.send_json(payload)

            messages = []
            # Read all messages until the connection closes
            while True:
                try:
                    msg = ws.receive_json()
                    messages.append(msg)
                    if msg["type"] in ("pipeline_done", "error"):
                        break
                except Exception:
                    break

        # Should have: node_start(n1), node_done(n1), node_start(n2), node_done(n2), pipeline_done
        types = [m["type"] for m in messages]
        assert "node_start" in types
        assert "node_done" in types
        assert "pipeline_done" in types

        # Verify node_start messages
        starts = [m for m in messages if m["type"] == "node_start"]
        start_ids = [m["node_id"] for m in starts]
        assert "n1" in start_ids
        assert "n2" in start_ids

        # Verify node_done messages have preview without 'df'
        dones = [m for m in messages if m["type"] == "node_done"]
        for d in dones:
            assert "preview" in d
            assert "df" not in d["preview"]

        # Verify pipeline_done
        pipeline_done = [m for m in messages if m["type"] == "pipeline_done"][0]
        assert "results" in pipeline_done
        results = pipeline_done["results"]
        assert "n2" in results
        assert results["n2"]["total"] == 2
        # No raw df in serialized results
        for node_id, output in results.items():
            assert "df" not in output
    finally:
        os.unlink(csv_path)


def test_ws_error_bad_node_type(init_teide):
    """Send a pipeline with an unknown node type — verify error response."""
    payload = {
        "nodes": [
            {"id": "n1", "type": "nonexistent_node_type", "config": {}},
        ],
        "edges": [],
    }
    client = TestClient(app)
    with client.websocket_connect("/ws/run") as ws:
        ws.send_json(payload)

        messages = []
        while True:
            try:
                msg = ws.receive_json()
                messages.append(msg)
                if msg["type"] in ("pipeline_done", "error"):
                    break
            except Exception:
                break

    # Unknown node type raises KeyError before callbacks fire, so the
    # executor propagates an exception caught by the WS handler as a
    # top-level error message.
    types = [m["type"] for m in messages]
    assert "error" in types
    error_msg = [m for m in messages if m["type"] == "error"][0]
    assert "error" in error_msg
    assert len(error_msg["error"]) > 0


def test_ws_error_node_execution(init_teide):
    """Send a pipeline where a node fails during execution — verify per-node error."""
    # CSV source with a nonexistent file path will fail during execute()
    payload = {
        "nodes": [
            {"id": "n1", "type": "csv_source", "config": {"file_path": "/nonexistent.csv"}},
            {"id": "n2", "type": "grid", "config": {}},
        ],
        "edges": [{"source": "n1", "target": "n2"}],
    }
    client = TestClient(app)
    with client.websocket_connect("/ws/run") as ws:
        ws.send_json(payload)

        messages = []
        while True:
            try:
                msg = ws.receive_json()
                messages.append(msg)
                if msg["type"] in ("pipeline_done", "error"):
                    break
            except Exception:
                break

    types = [m["type"] for m in messages]

    # Should see: node_start for n1, then node_error for n1, then pipeline_done
    assert "node_start" in types
    assert "node_error" in types
    assert "pipeline_done" in types

    # Verify the error is on n1
    errors = [m for m in messages if m["type"] == "node_error"]
    assert len(errors) == 1
    assert errors[0]["node_id"] == "n1"
    assert len(errors[0]["error"]) > 0

    # n2 should not have started (executor stops on first error)
    start_ids = [m["node_id"] for m in messages if m["type"] == "node_start"]
    assert "n2" not in start_ids


def test_ws_invalid_payload(init_teide):
    """Send invalid payload (missing 'nodes') — verify error response."""
    client = TestClient(app)
    with client.websocket_connect("/ws/run") as ws:
        ws.send_json({"bad_key": "bad_value"})

        msg = ws.receive_json()
        assert msg["type"] == "error"
        assert "invalid" in msg["error"].lower() or "payload" in msg["error"].lower()
