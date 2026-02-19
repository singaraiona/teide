import os
import tempfile
from mirador.engine.executor import PipelineExecutor
from mirador.engine.registry import NodeRegistry


def _make_csv(content):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(content)
        return f.name


def _make_registry():
    reg = NodeRegistry()
    reg.discover()
    return reg


def test_csv_to_grid(init_teide):
    """Simple: CSV → Grid."""
    path = _make_csv("x,y\n1,10\n2,20\n3,30\n")
    try:
        pipeline = {
            "nodes": [
                {"id": "n1", "type": "csv_source", "config": {"file_path": path}},
                {"id": "n2", "type": "grid", "config": {"page_size": 10}},
            ],
            "edges": [{"source": "n1", "target": "n2"}],
        }
        executor = PipelineExecutor(_make_registry())
        results = executor.run(pipeline)

        assert "n1" in results
        assert "n2" in results
        assert results["n2"]["total"] == 3
        assert len(results["n2"]["rows"]) == 3
    finally:
        os.unlink(path)


def test_csv_query_groupby_grid(init_teide):
    """CSV → Query(groupby) → Grid."""
    path = _make_csv("id,val\na,10\na,20\nb,30\n")
    try:
        pipeline = {
            "nodes": [
                {"id": "src", "type": "csv_source", "config": {"file_path": path}},
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
        executor = PipelineExecutor(_make_registry())
        results = executor.run(pipeline)

        assert results["out"]["total"] == 2  # a, b
    finally:
        os.unlink(path)


def test_csv_query_filter_grid(init_teide):
    """CSV → Query(filter) → Grid."""
    path = _make_csv("id,val\na,10\na,20\nb,30\nb,40\n")
    try:
        pipeline = {
            "nodes": [
                {"id": "src", "type": "csv_source", "config": {"file_path": path}},
                {"id": "qry", "type": "query", "config": {
                    "mode": "form",
                    "filter": {"column": "val", "operator": "gt", "value": "20"},
                }},
                {"id": "out", "type": "grid", "config": {}},
            ],
            "edges": [
                {"source": "src", "target": "qry"},
                {"source": "qry", "target": "out"},
            ],
        }
        executor = PipelineExecutor(_make_registry())
        results = executor.run(pipeline)

        assert results["out"]["total"] == 2  # 30, 40
    finally:
        os.unlink(path)


def test_error_handling(init_teide):
    """Pipeline should stop on error and report it."""
    pipeline = {
        "nodes": [
            {"id": "n1", "type": "csv_source", "config": {"file_path": "/nonexistent.csv"}},
            {"id": "n2", "type": "grid", "config": {}},
        ],
        "edges": [{"source": "n1", "target": "n2"}],
    }
    executor = PipelineExecutor(_make_registry())
    results = executor.run(pipeline)

    assert "error" in results["n1"]
    # n2 should not have run
    assert "n2" not in results


def test_cycle_detection(init_teide):
    """Pipeline with a cycle should raise ValueError."""
    pipeline = {
        "nodes": [
            {"id": "a", "type": "csv_source", "config": {"file_path": "x.csv"}},
            {"id": "b", "type": "grid", "config": {}},
        ],
        "edges": [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "a"},
        ],
    }
    executor = PipelineExecutor(_make_registry())
    try:
        executor.run(pipeline)
        assert False, "Should raise ValueError for cycle"
    except ValueError as e:
        assert "cycle" in str(e).lower()


def test_callbacks(init_teide):
    """Callbacks should fire for each node."""
    path = _make_csv("x\n1\n2\n")
    started = []
    finished = []
    try:
        pipeline = {
            "nodes": [
                {"id": "n1", "type": "csv_source", "config": {"file_path": path}},
                {"id": "n2", "type": "grid", "config": {}},
            ],
            "edges": [{"source": "n1", "target": "n2"}],
        }
        executor = PipelineExecutor(_make_registry())
        executor.run(
            pipeline,
            on_node_start=lambda nid: started.append(nid),
            on_node_done=lambda nid, out: finished.append(nid),
        )

        assert started == ["n1", "n2"]
        assert finished == ["n1", "n2"]
    finally:
        os.unlink(path)
