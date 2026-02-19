# Mirador Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build Mirador — a visual analytics pipeline builder powered by Teide. First vertical slice: CSV input → compute nodes → Grid output, both backend and frontend.

**Architecture:** Python FastAPI backend with Teide ctypes bindings (existing at `py/teide/`). React + React Flow frontend. Dict-based universal wire format between nodes. Eager node-by-node execution with WebSocket progress updates.

**Tech Stack:** Python 3.11+, FastAPI, uvicorn, React 18, React Flow, Vite, TypeScript, Apache ECharts

**Existing code:** Python bindings at `py/teide/__init__.py` (TeideLib, ctypes) and `py/teide/api.py` (Context, Table, Series, Query, Expr). Tests at `test/test_api.py`.

---

## Phase 1: Backend Scaffold

### Task 1: Create project structure and pyproject.toml

**Files:**
- Create: `mirador/pyproject.toml`
- Create: `mirador/mirador/__init__.py`
- Create: `mirador/mirador/app.py`

**Step 1: Create directory structure**

```bash
mkdir -p mirador/mirador/{api,engine,nodes/{inputs,compute,generic,outputs},storage}
mkdir -p mirador/tests
```

**Step 2: Write pyproject.toml**

```toml
[project]
name = "mirador"
version = "0.1.0"
description = "Visual analytics pipeline builder powered by Teide"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "websockets>=13.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "httpx>=0.27",  # for FastAPI test client
]

[project.scripts]
mirador = "mirador.app:main"
```

**Step 3: Write app.py — minimal FastAPI server**

```python
"""Mirador — Visual analytics pipeline builder."""

import os
import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Mirador", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite dev server
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ensure teide is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "py"))


@app.get("/api/health")
def health():
    return {"status": "ok", "version": "0.1.0"}


def main():
    import uvicorn
    uvicorn.run("mirador.app:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
```

**Step 4: Write mirador/__init__.py**

```python
"""Mirador — Visual analytics pipeline builder powered by Teide."""
__version__ = "0.1.0"
```

**Step 5: Test the server starts**

```bash
cd mirador && pip install -e ".[dev]" && python -m mirador.app
# Verify: curl http://localhost:8000/api/health → {"status":"ok","version":"0.1.0"}
```

**Step 6: Commit**

```bash
git add mirador/
git commit -m "feat(mirador): scaffold backend with FastAPI"
```

---

### Task 2: Initialize Teide context as a FastAPI lifespan

**Files:**
- Modify: `mirador/mirador/app.py`

Teide requires `sym_init()` and `heap_init()` on startup, and `pool_destroy()` + `heap_destroy()` on shutdown. Use FastAPI's lifespan pattern.

**Step 1: Write failing test**

Create `mirador/tests/test_health.py`:

```python
import pytest
from httpx import AsyncClient, ASGITransport
from mirador.app import app

@pytest.mark.asyncio
async def test_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert "teide" in data  # should report teide loaded
```

**Step 2: Run test to verify it fails**

```bash
cd mirador && python -m pytest tests/test_health.py -v
# Expected: FAIL — "teide" not in response
```

**Step 3: Implement Teide lifespan in app.py**

```python
from contextlib import asynccontextmanager
from teide import TeideLib

_teide: TeideLib | None = None

def get_teide() -> TeideLib:
    assert _teide is not None, "Teide not initialized"
    return _teide

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _teide
    lib_path = os.environ.get("TEIDE_LIB")
    _teide = TeideLib(lib_path)
    _teide.sym_init()
    _teide.arena_init()
    yield
    _teide.pool_destroy()
    _teide.sym_destroy()
    _teide.arena_destroy_all()
    _teide = None

# Update FastAPI app to use lifespan
app = FastAPI(title="Mirador", version="0.1.0", lifespan=lifespan)
```

Update health endpoint to report teide:

```python
@app.get("/api/health")
def health():
    return {"status": "ok", "version": "0.1.0", "teide": _teide is not None}
```

**Step 4: Run test**

```bash
TEIDE_LIB=../build_release/libteide.so python -m pytest tests/test_health.py -v
# Expected: PASS
```

**Step 5: Commit**

```bash
git add -A && git commit -m "feat(mirador): add Teide lifespan initialization"
```

---

## Phase 2: Node System

### Task 3: Create BaseNode class and node registry

**Files:**
- Create: `mirador/mirador/nodes/base.py`
- Create: `mirador/mirador/engine/registry.py`
- Create: `mirador/tests/test_registry.py`

**Step 1: Write failing test**

```python
# mirador/tests/test_registry.py
from mirador.engine.registry import NodeRegistry
from mirador.nodes.base import BaseNode

def test_registry_discovers_nodes():
    reg = NodeRegistry()
    reg.discover()
    # Should find at least the csv_source node
    assert "csv_source" in reg.node_types

def test_base_node_interface():
    """BaseNode.execute must be overridden."""
    node = BaseNode()
    try:
        node.execute({}, {})
        assert False, "Should raise NotImplementedError"
    except NotImplementedError:
        pass
```

**Step 2: Run test — expect FAIL**

```bash
cd mirador && python -m pytest tests/test_registry.py -v
```

**Step 3: Implement BaseNode**

```python
# mirador/mirador/nodes/base.py
from dataclasses import dataclass, field
from typing import Any

@dataclass
class NodePort:
    """Describes an input or output port on a node."""
    name: str
    description: str = ""

@dataclass
class NodeMeta:
    """Metadata describing a node type for the frontend palette."""
    id: str                        # e.g. "csv_source"
    label: str                     # e.g. "CSV File"
    category: str                  # "input" | "compute" | "generic" | "output"
    description: str = ""
    inputs: list[NodePort] = field(default_factory=list)
    outputs: list[NodePort] = field(default_factory=list)
    config_schema: dict = field(default_factory=dict)  # JSON Schema for config panel

class BaseNode:
    """Base class for all Mirador pipeline nodes."""

    meta: NodeMeta  # subclasses must define

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """Execute this node. Receives upstream dicts merged, returns output dict."""
        raise NotImplementedError
```

**Step 4: Implement NodeRegistry**

```python
# mirador/mirador/engine/registry.py
import importlib
import pkgutil
from mirador.nodes.base import BaseNode

class NodeRegistry:
    """Discovers and indexes all available node types."""

    def __init__(self):
        self.node_types: dict[str, type[BaseNode]] = {}

    def discover(self):
        """Scan mirador.nodes.* packages for BaseNode subclasses."""
        import mirador.nodes.inputs as inputs_pkg
        import mirador.nodes.compute as compute_pkg
        import mirador.nodes.generic as generic_pkg
        import mirador.nodes.outputs as outputs_pkg

        for pkg in [inputs_pkg, compute_pkg, generic_pkg, outputs_pkg]:
            for importer, modname, ispkg in pkgutil.iter_modules(pkg.__path__):
                mod = importlib.import_module(f"{pkg.__name__}.{modname}")
                for attr_name in dir(mod):
                    attr = getattr(mod, attr_name)
                    if (isinstance(attr, type)
                        and issubclass(attr, BaseNode)
                        and attr is not BaseNode
                        and hasattr(attr, 'meta')):
                        self.node_types[attr.meta.id] = attr

    def get(self, node_type_id: str) -> type[BaseNode]:
        return self.node_types[node_type_id]

    def list_meta(self) -> list[dict]:
        """Return metadata for all registered nodes (for frontend palette)."""
        from dataclasses import asdict
        return [asdict(cls.meta) for cls in self.node_types.values()]
```

**Step 5: Create __init__.py files for node packages**

```python
# Create empty __init__.py in each nodes subpackage
# mirador/mirador/nodes/__init__.py
# mirador/mirador/nodes/inputs/__init__.py
# mirador/mirador/nodes/compute/__init__.py
# mirador/mirador/nodes/generic/__init__.py
# mirador/mirador/nodes/outputs/__init__.py
# mirador/mirador/engine/__init__.py
```

**Step 6: Run tests — expect PASS (after Task 4 adds csv_source)**

This test depends on Task 4. Run after Task 4.

**Step 7: Commit**

```bash
git add -A && git commit -m "feat(mirador): add BaseNode and NodeRegistry"
```

---

### Task 4: Implement CSV Source node

**Files:**
- Create: `mirador/mirador/nodes/inputs/csv_source.py`
- Create: `mirador/tests/test_csv_node.py`

**Step 1: Write failing test**

```python
# mirador/tests/test_csv_node.py
import os
import tempfile
import pytest
from mirador.nodes.inputs.csv_source import CsvSourceNode

@pytest.fixture
def csv_file():
    data = "id,name,value\n1,a,10.5\n2,b,20.0\n3,c,30.5\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        yield f.name
    os.unlink(f.name)

def test_csv_source_loads(csv_file):
    node = CsvSourceNode()
    result = node.execute({}, {"file_path": csv_file})
    assert "df" in result
    assert result["rows"] == 3
    assert "id" in result["columns"]
    assert "value" in result["columns"]
```

**Step 2: Run test — expect FAIL**

```bash
TEIDE_LIB=../build_release/libteide.so python -m pytest tests/test_csv_node.py -v
```

**Step 3: Implement CsvSourceNode**

```python
# mirador/mirador/nodes/inputs/csv_source.py
from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort

class CsvSourceNode(BaseNode):
    meta = NodeMeta(
        id="csv_source",
        label="CSV File",
        category="input",
        description="Load data from a CSV file",
        inputs=[],
        outputs=[NodePort(name="out", description="Loaded dataframe")],
        config_schema={
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "title": "File Path"},
            },
            "required": ["file_path"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        from mirador.app import get_teide
        from teide.api import Table

        lib = get_teide()
        path = config["file_path"]
        tbl_ptr = lib.read_csv(path)
        if not tbl_ptr or tbl_ptr < 32:
            raise RuntimeError(f"Failed to read CSV: {path}")

        table = Table(lib, tbl_ptr)
        return {
            "df": table,
            "rows": len(table),
            "columns": table.columns,
        }
```

Note: This node depends on Teide being initialized. For unit testing, we need a test fixture that initializes Teide. Create a shared conftest:

```python
# mirador/tests/conftest.py
import os
import sys
import pytest

# Ensure teide and mirador are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "py"))

from teide import TeideLib

@pytest.fixture(scope="session", autouse=True)
def init_teide():
    """Initialize Teide for all tests."""
    import mirador.app as app_mod
    lib_path = os.environ.get("TEIDE_LIB")
    lib = TeideLib(lib_path)
    lib.sym_init()
    lib.arena_init()
    app_mod._teide = lib
    yield lib
    lib.pool_destroy()
    lib.sym_destroy()
    lib.arena_destroy_all()
```

**Step 4: Run test**

```bash
TEIDE_LIB=../build_release/libteide.so python -m pytest tests/test_csv_node.py -v
# Expected: PASS
```

**Step 5: Commit**

```bash
git add -A && git commit -m "feat(mirador): add CSV source node"
```

---

### Task 5: Implement Grid Output node

**Files:**
- Create: `mirador/mirador/nodes/outputs/grid.py`
- Create: `mirador/tests/test_grid_node.py`

**Step 1: Write failing test**

```python
# mirador/tests/test_grid_node.py
from mirador.nodes.outputs.grid import GridNode
from teide.api import Table

def test_grid_node_returns_rows(init_teide):
    """Grid node should convert dataframe to JSON-serializable rows."""
    # Build a small table via teide
    lib = init_teide
    from teide.api import Context
    import tempfile, os

    data = "x,y\n1,10.5\n2,20.0\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        path = f.name

    tbl_ptr = lib.read_csv(path)
    table = Table(lib, tbl_ptr)
    os.unlink(path)

    node = GridNode()
    result = node.execute({"df": table, "columns": ["x", "y"]}, {"page_size": 100})

    assert "rows" in result
    assert "columns" in result
    assert len(result["rows"]) == 2
    assert result["rows"][0]["x"] == 1
```

**Step 2: Run test — expect FAIL**

**Step 3: Implement GridNode**

```python
# mirador/mirador/nodes/outputs/grid.py
from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort

class GridNode(BaseNode):
    meta = NodeMeta(
        id="grid",
        label="Data Grid",
        category="output",
        description="Display data as an interactive table",
        inputs=[NodePort(name="in", description="Dataframe to display")],
        outputs=[],
        config_schema={
            "type": "object",
            "properties": {
                "page_size": {"type": "integer", "title": "Page Size", "default": 100},
            },
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        table = inputs.get("df")
        if table is None:
            return {"rows": [], "columns": [], "total": 0}

        page_size = config.get("page_size", 100)
        columns = inputs.get("columns", table.columns if hasattr(table, 'columns') else [])

        # Convert to list of dicts (JSON-serializable)
        data = table.to_dict()
        n = len(table)
        rows = []
        for i in range(min(n, page_size)):
            row = {col: data[col][i] for col in columns}
            rows.append(row)

        return {
            "rows": rows,
            "columns": columns,
            "total": n,
        }
```

**Step 4: Run test**

```bash
TEIDE_LIB=../build_release/libteide.so python -m pytest tests/test_grid_node.py -v
# Expected: PASS
```

**Step 5: Commit**

```bash
git add -A && git commit -m "feat(mirador): add Grid output node"
```

---

### Task 6: Implement Filter and Group By compute nodes

**Files:**
- Create: `mirador/mirador/nodes/compute/filter.py`
- Create: `mirador/mirador/nodes/compute/groupby.py`
- Create: `mirador/tests/test_compute_nodes.py`

**Step 1: Write failing test**

```python
# mirador/tests/test_compute_nodes.py
import os, tempfile
from teide.api import Table
from mirador.nodes.compute.filter import FilterNode
from mirador.nodes.compute.groupby import GroupByNode

def _make_table(init_teide):
    lib = init_teide
    data = "id,val\na,10\na,20\nb,30\nb,40\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        path = f.name
    tbl_ptr = lib.read_csv(path)
    table = Table(lib, tbl_ptr)
    os.unlink(path)
    return table

def test_filter_node(init_teide):
    table = _make_table(init_teide)
    node = FilterNode()
    result = node.execute(
        {"df": table, "columns": table.columns},
        {"column": "id", "operator": "eq", "value": "a"}
    )
    assert result["rows"] == 2

def test_groupby_node(init_teide):
    table = _make_table(init_teide)
    node = GroupByNode()
    result = node.execute(
        {"df": table, "columns": table.columns},
        {"keys": ["id"], "aggs": [{"column": "val", "op": "sum"}]}
    )
    assert result["rows"] == 2  # a, b
```

**Step 2: Run test — expect FAIL**

**Step 3: Implement FilterNode**

```python
# mirador/mirador/nodes/compute/filter.py
from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort

class FilterNode(BaseNode):
    meta = NodeMeta(
        id="filter",
        label="Filter",
        category="compute",
        description="Filter rows by condition",
        inputs=[NodePort(name="in", description="Input dataframe")],
        outputs=[NodePort(name="out", description="Filtered dataframe")],
        config_schema={
            "type": "object",
            "properties": {
                "column": {"type": "string", "title": "Column"},
                "operator": {"type": "string", "enum": ["eq", "ne", "gt", "lt", "ge", "le"]},
                "value": {"title": "Value"},
            },
            "required": ["column", "operator", "value"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        from teide.api import col, lit
        table = inputs["df"]
        column = config["column"]
        operator = config["operator"]
        value = config["value"]

        op_map = {
            "eq": lambda c, v: c == v,
            "ne": lambda c, v: c != v,
            "gt": lambda c, v: c > v,
            "lt": lambda c, v: c < v,
            "ge": lambda c, v: c >= v,
            "le": lambda c, v: c <= v,
        }

        expr = op_map[operator](col(column), lit(value))
        result = table.filter(expr).collect()
        return {
            "df": result,
            "rows": len(result),
            "columns": result.columns,
        }
```

**Step 4: Implement GroupByNode**

```python
# mirador/mirador/nodes/compute/groupby.py
from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort

class GroupByNode(BaseNode):
    meta = NodeMeta(
        id="groupby",
        label="Group By",
        category="compute",
        description="Group rows and aggregate",
        inputs=[NodePort(name="in", description="Input dataframe")],
        outputs=[NodePort(name="out", description="Aggregated dataframe")],
        config_schema={
            "type": "object",
            "properties": {
                "keys": {"type": "array", "items": {"type": "string"}, "title": "Group Keys"},
                "aggs": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "column": {"type": "string"},
                            "op": {"type": "string", "enum": ["sum", "avg", "min", "max", "count"]},
                        },
                    },
                    "title": "Aggregations",
                },
            },
            "required": ["keys", "aggs"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        from teide.api import col
        table = inputs["df"]
        keys = config["keys"]
        aggs = config["aggs"]

        agg_map = {"sum": "sum", "avg": "mean", "min": "min", "max": "max", "count": "count"}
        agg_exprs = [getattr(col(a["column"]), agg_map[a["op"]])() for a in aggs]

        result = table.group_by(*keys).agg(*agg_exprs).collect()
        return {
            "df": result,
            "rows": len(result),
            "columns": result.columns,
        }
```

**Step 5: Run tests**

```bash
TEIDE_LIB=../build_release/libteide.so python -m pytest tests/test_compute_nodes.py -v
```

**Step 6: Commit**

```bash
git add -A && git commit -m "feat(mirador): add Filter and GroupBy compute nodes"
```

---

### Task 7: Implement Sort, Join, and Window compute nodes

**Files:**
- Create: `mirador/mirador/nodes/compute/sort.py`
- Create: `mirador/mirador/nodes/compute/join.py`
- Create: `mirador/mirador/nodes/compute/window.py`

Follow the same pattern as Task 6. Each node wraps the corresponding Teide API:

- **SortNode**: `table.sort(*cols, descending=descs).collect()`
- **JoinNode**: `table.join(right_table, on=keys, how=join_type)` — note: takes two input ports
- **WindowNode**: Uses Teide's window function API

Config schemas for each:

- Sort: `{columns: [{name: str, desc: bool}]}`
- Join: `{keys: [str], how: "inner"|"left"}` — two inputs (left, right)
- Window: `{partition_by: [str], order_by: [str], function: str, output_name: str}`

Test each with small CSV fixtures. Commit.

---

## Phase 3: Pipeline Executor

### Task 8: Implement topological sort and pipeline executor

**Files:**
- Create: `mirador/mirador/engine/executor.py`
- Create: `mirador/tests/test_executor.py`

**Step 1: Write failing test**

```python
# mirador/tests/test_executor.py
import os, tempfile
from mirador.engine.executor import PipelineExecutor
from mirador.engine.registry import NodeRegistry

def test_simple_pipeline(init_teide):
    """CSV → Grid pipeline should execute and produce rows."""
    data = "x,y\n1,10\n2,20\n3,30\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        csv_path = f.name

    pipeline = {
        "nodes": [
            {"id": "n1", "type": "csv_source", "config": {"file_path": csv_path}},
            {"id": "n2", "type": "grid", "config": {"page_size": 10}},
        ],
        "edges": [
            {"source": "n1", "target": "n2"},
        ],
    }

    registry = NodeRegistry()
    registry.discover()
    executor = PipelineExecutor(registry)
    results = executor.run(pipeline)

    os.unlink(csv_path)

    assert "n1" in results
    assert "n2" in results
    assert results["n2"]["total"] == 3
    assert len(results["n2"]["rows"]) == 3


def test_csv_groupby_grid_pipeline(init_teide):
    """CSV → GroupBy → Grid pipeline."""
    data = "id,val\na,10\na,20\nb,30\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        csv_path = f.name

    pipeline = {
        "nodes": [
            {"id": "n1", "type": "csv_source", "config": {"file_path": csv_path}},
            {"id": "n2", "type": "groupby", "config": {
                "keys": ["id"],
                "aggs": [{"column": "val", "op": "sum"}]
            }},
            {"id": "n3", "type": "grid", "config": {}},
        ],
        "edges": [
            {"source": "n1", "target": "n2"},
            {"source": "n2", "target": "n3"},
        ],
    }

    registry = NodeRegistry()
    registry.discover()
    executor = PipelineExecutor(registry)
    results = executor.run(pipeline)
    os.unlink(csv_path)

    assert results["n3"]["total"] == 2  # groups: a, b
```

**Step 2: Run test — expect FAIL**

**Step 3: Implement PipelineExecutor**

```python
# mirador/mirador/engine/executor.py
from collections import defaultdict
from typing import Any, Callable
from mirador.engine.registry import NodeRegistry

class PipelineExecutor:
    """Walks a pipeline DAG in topological order, executing each node eagerly."""

    def __init__(self, registry: NodeRegistry):
        self.registry = registry

    def run(
        self,
        pipeline: dict,
        on_node_start: Callable[[str], None] | None = None,
        on_node_done: Callable[[str, dict], None] | None = None,
        on_node_error: Callable[[str, Exception], None] | None = None,
    ) -> dict[str, Any]:
        """Execute the pipeline, return {node_id: output_dict}."""
        nodes = {n["id"]: n for n in pipeline["nodes"]}
        edges = pipeline["edges"]

        # Build adjacency: node_id -> list of upstream node_ids
        upstream = defaultdict(list)
        for e in edges:
            upstream[e["target"]].append(e["source"])

        # Topological sort (Kahn's algorithm)
        in_degree = defaultdict(int)
        for n_id in nodes:
            in_degree[n_id]  # ensure all nodes present
        for e in edges:
            in_degree[e["target"]] += 1

        queue = [n_id for n_id in nodes if in_degree[n_id] == 0]
        order = []
        while queue:
            n_id = queue.pop(0)
            order.append(n_id)
            for e in edges:
                if e["source"] == n_id:
                    in_degree[e["target"]] -= 1
                    if in_degree[e["target"]] == 0:
                        queue.append(e["target"])

        if len(order) != len(nodes):
            raise ValueError("Pipeline has a cycle")

        # Execute in order
        results: dict[str, Any] = {}
        for n_id in order:
            node_def = nodes[n_id]
            node_cls = self.registry.get(node_def["type"])
            node = node_cls()

            # Merge upstream outputs into input dict
            inputs = {}
            for up_id in upstream[n_id]:
                up_out = results[up_id]
                inputs.update(up_out)

            if on_node_start:
                on_node_start(n_id)

            try:
                output = node.execute(inputs, node_def.get("config", {}))
                results[n_id] = output
                if on_node_done:
                    on_node_done(n_id, output)
            except Exception as exc:
                if on_node_error:
                    on_node_error(n_id, exc)
                results[n_id] = {"error": str(exc)}
                break  # stop on first error

        return results
```

**Step 4: Run tests**

```bash
TEIDE_LIB=../build_release/libteide.so python -m pytest tests/test_executor.py -v
```

**Step 5: Commit**

```bash
git add -A && git commit -m "feat(mirador): add pipeline executor with topological sort"
```

---

## Phase 4: REST API

### Task 9: Pipeline CRUD and execution API

**Files:**
- Create: `mirador/mirador/api/pipelines.py`
- Create: `mirador/mirador/api/nodes.py`
- Modify: `mirador/mirador/app.py` — register routers
- Create: `mirador/tests/test_api_pipelines.py`

**Step 1: Write failing test**

```python
# mirador/tests/test_api_pipelines.py
import pytest
from httpx import AsyncClient, ASGITransport
from mirador.app import app

@pytest.mark.asyncio
async def test_list_node_types():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/nodes")
        assert r.status_code == 200
        data = r.json()
        assert any(n["id"] == "csv_source" for n in data)
        assert any(n["id"] == "grid" for n in data)

@pytest.mark.asyncio
async def test_run_pipeline(tmp_path):
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("x,y\n1,10\n2,20\n")

    pipeline = {
        "nodes": [
            {"id": "n1", "type": "csv_source", "config": {"file_path": str(csv_file)}},
            {"id": "n2", "type": "grid", "config": {}},
        ],
        "edges": [{"source": "n1", "target": "n2"}],
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/api/pipelines/run", json=pipeline)
        assert r.status_code == 200
        data = r.json()
        assert "n2" in data
        assert data["n2"]["total"] == 2
```

**Step 2: Run test — expect FAIL**

**Step 3: Implement API routers**

```python
# mirador/mirador/api/nodes.py
from fastapi import APIRouter
from mirador.engine.registry import NodeRegistry

router = APIRouter(prefix="/api/nodes", tags=["nodes"])

_registry: NodeRegistry | None = None

def get_registry() -> NodeRegistry:
    global _registry
    if _registry is None:
        _registry = NodeRegistry()
        _registry.discover()
    return _registry

@router.get("")
def list_node_types():
    return get_registry().list_meta()
```

```python
# mirador/mirador/api/pipelines.py
from fastapi import APIRouter
from mirador.engine.executor import PipelineExecutor
from mirador.api.nodes import get_registry

router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])

@router.post("/run")
def run_pipeline(pipeline: dict):
    registry = get_registry()
    executor = PipelineExecutor(registry)
    results = executor.run(pipeline)
    # Strip non-serializable objects (Table) from results
    return _serialize_results(results)

def _serialize_results(results: dict) -> dict:
    """Remove Teide Table objects, keep JSON-serializable data."""
    clean = {}
    for node_id, output in results.items():
        clean[node_id] = {
            k: v for k, v in output.items()
            if k != "df"  # exclude raw Table objects
        }
    return clean
```

Register routers in `app.py`:

```python
from mirador.api.nodes import router as nodes_router
from mirador.api.pipelines import router as pipelines_router

app.include_router(nodes_router)
app.include_router(pipelines_router)
```

**Step 4: Run tests**

```bash
TEIDE_LIB=../build_release/libteide.so python -m pytest tests/test_api_pipelines.py -v
```

**Step 5: Commit**

```bash
git add -A && git commit -m "feat(mirador): add pipeline execution REST API"
```

---

### Task 10: WebSocket endpoint for live execution progress

**Files:**
- Create: `mirador/mirador/api/ws.py`
- Modify: `mirador/mirador/app.py` — register WS route

**Step 1: Implement WebSocket endpoint**

```python
# mirador/mirador/api/ws.py
import json
from fastapi import APIRouter, WebSocket
from mirador.engine.executor import PipelineExecutor
from mirador.api.nodes import get_registry
from mirador.api.pipelines import _serialize_results

router = APIRouter()

@router.websocket("/ws/run")
async def ws_run(websocket: WebSocket):
    await websocket.accept()
    data = await websocket.receive_json()
    pipeline = data.get("pipeline", data)

    registry = get_registry()
    executor = PipelineExecutor(registry)

    async def on_start(node_id: str):
        await websocket.send_json({"type": "node_start", "node_id": node_id})

    async def on_done(node_id: str, output: dict):
        # Send preview (exclude df)
        preview = {k: v for k, v in output.items() if k != "df"}
        await websocket.send_json({"type": "node_done", "node_id": node_id, "preview": preview})

    async def on_error(node_id: str, exc: Exception):
        await websocket.send_json({"type": "node_error", "node_id": node_id, "error": str(exc)})

    # Run executor (sync for now — async wrapper in future)
    results = executor.run(pipeline)
    serialized = _serialize_results(results)
    await websocket.send_json({"type": "pipeline_done", "results": serialized})
    await websocket.close()
```

Note: The executor callbacks are sync in Task 8. For the WebSocket to send updates during execution, the executor needs to be made async or run in a thread with callback bridging. For v1, we send the final result only. Real-time per-node updates can be added in a follow-up task.

**Step 2: Register in app.py and commit**

```bash
git add -A && git commit -m "feat(mirador): add WebSocket endpoint for pipeline execution"
```

---

## Phase 5: Frontend Scaffold

### Task 11: Create React + Vite + React Flow project

**Files:**
- Create: `mirador/frontend/` — full React project

**Step 1: Scaffold the project**

```bash
cd mirador
npm create vite@latest frontend -- --template react-ts
cd frontend
npm install @xyflow/react
npm install axios
npm install -D @types/node
```

**Step 2: Clean up defaults, set up base layout**

Replace `src/App.tsx` with the four-zone layout shell:

```tsx
// mirador/frontend/src/App.tsx
import { ReactFlowProvider } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Canvas } from './canvas/Canvas';
import { Sidebar } from './panels/Sidebar';
import { PreviewPanel } from './panels/PreviewPanel';
import { Toolbar } from './panels/Toolbar';
import './App.css';

function App() {
  return (
    <ReactFlowProvider>
      <div className="app-layout">
        <Toolbar />
        <div className="app-main">
          <Sidebar />
          <Canvas />
        </div>
        <PreviewPanel />
      </div>
    </ReactFlowProvider>
  );
}

export default App;
```

**Step 3: Create placeholder components**

Create stub files for `Canvas.tsx`, `Sidebar.tsx`, `PreviewPanel.tsx`, `Toolbar.tsx` with basic div placeholders.

**Step 4: Add CSS layout**

```css
/* mirador/frontend/src/App.css */
.app-layout {
  display: flex;
  flex-direction: column;
  height: 100vh;
}
.app-main {
  display: flex;
  flex: 1;
  overflow: hidden;
}
```

**Step 5: Verify it starts**

```bash
cd mirador/frontend && npm run dev
# Opens http://localhost:5173 — should show layout skeleton
```

**Step 6: Commit**

```bash
git add -A && git commit -m "feat(mirador): scaffold React + React Flow frontend"
```

---

### Task 12: Build the Canvas with React Flow

**Files:**
- Create: `mirador/frontend/src/canvas/Canvas.tsx`
- Create: `mirador/frontend/src/canvas/useStore.ts` — Zustand or React state for nodes/edges
- Create: `mirador/frontend/src/canvas/nodeTypes.ts`

**Step 1: Install zustand for state management**

```bash
cd mirador/frontend && npm install zustand
```

**Step 2: Create pipeline store**

```typescript
// mirador/frontend/src/canvas/useStore.ts
import { create } from 'zustand';
import { Node, Edge, Connection, addEdge, applyNodeChanges, applyEdgeChanges } from '@xyflow/react';

interface PipelineStore {
  nodes: Node[];
  edges: Edge[];
  selectedNodeId: string | null;
  nodeResults: Record<string, any>;
  onNodesChange: (changes: any) => void;
  onEdgesChange: (changes: any) => void;
  onConnect: (connection: Connection) => void;
  addNode: (node: Node) => void;
  selectNode: (id: string | null) => void;
  setNodeResults: (results: Record<string, any>) => void;
}

export const useStore = create<PipelineStore>((set, get) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,
  nodeResults: {},
  onNodesChange: (changes) => set({ nodes: applyNodeChanges(changes, get().nodes) }),
  onEdgesChange: (changes) => set({ edges: applyEdgeChanges(changes, get().edges) }),
  onConnect: (connection) => set({ edges: addEdge(connection, get().edges) }),
  addNode: (node) => set({ nodes: [...get().nodes, node] }),
  selectNode: (id) => set({ selectedNodeId: id }),
  setNodeResults: (results) => set({ nodeResults: results }),
}));
```

**Step 3: Create Canvas component**

```tsx
// mirador/frontend/src/canvas/Canvas.tsx
import { useCallback } from 'react';
import { ReactFlow, Background, Controls, MiniMap } from '@xyflow/react';
import { useStore } from './useStore';

export function Canvas() {
  const { nodes, edges, onNodesChange, onEdgesChange, onConnect, selectNode } = useStore();

  const onNodeClick = useCallback((_: any, node: any) => {
    selectNode(node.id);
  }, [selectNode]);

  return (
    <div style={{ flex: 1 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        fitView
      >
        <Background />
        <Controls />
        <MiniMap />
      </ReactFlow>
    </div>
  );
}
```

**Step 4: Commit**

```bash
git add -A && git commit -m "feat(mirador): add React Flow canvas with Zustand store"
```

---

### Task 13: Build Sidebar — Node Palette + Inspector

**Files:**
- Create: `mirador/frontend/src/panels/Sidebar.tsx`
- Create: `mirador/frontend/src/panels/NodePalette.tsx`
- Create: `mirador/frontend/src/panels/Inspector.tsx`
- Create: `mirador/frontend/src/api/client.ts`

**Step 1: Create API client**

```typescript
// mirador/frontend/src/api/client.ts
import axios from 'axios';

const api = axios.create({ baseURL: 'http://localhost:8000/api' });

export async function fetchNodeTypes() {
  const { data } = await api.get('/nodes');
  return data;
}

export async function runPipeline(pipeline: any) {
  const { data } = await api.post('/pipelines/run', pipeline);
  return data;
}
```

**Step 2: Implement NodePalette**

Fetches node types from backend on mount, displays them grouped by category. Draggable items that add nodes to the canvas.

**Step 3: Implement Inspector**

When a node is selected, shows its config form based on `config_schema`. Uses the store to read/write node data.

**Step 4: Implement Sidebar as composition**

```tsx
// mirador/frontend/src/panels/Sidebar.tsx
import { NodePalette } from './NodePalette';
import { Inspector } from './Inspector';

export function Sidebar() {
  return (
    <div className="sidebar">
      <NodePalette />
      <Inspector />
    </div>
  );
}
```

**Step 5: Commit**

```bash
git add -A && git commit -m "feat(mirador): add Node Palette and Inspector panels"
```

---

### Task 14: Build Preview Panel and Toolbar

**Files:**
- Create: `mirador/frontend/src/panels/PreviewPanel.tsx`
- Create: `mirador/frontend/src/panels/Toolbar.tsx`

**Step 1: Implement PreviewPanel**

Shows the selected node's result. If result has `rows` + `columns`, render a data table. Otherwise show raw JSON.

**Step 2: Implement Toolbar**

Top bar with project name, Run button, Save button. Run button calls `runPipeline()` with the current store's nodes + edges.

**Step 3: Commit**

```bash
git add -A && git commit -m "feat(mirador): add Preview Panel and Toolbar with Run button"
```

---

## Phase 6: Integration & End-to-End

### Task 15: Wire Run button to backend pipeline execution

**Files:**
- Modify: `mirador/frontend/src/panels/Toolbar.tsx`
- Modify: `mirador/frontend/src/canvas/useStore.ts`

**Step 1: Convert React Flow nodes/edges to backend pipeline format**

The store has React Flow nodes (with `type`, `data.config`, `position`) and edges. The Run button converts these to the backend format:

```typescript
function toPipelinePayload(nodes: Node[], edges: Edge[]) {
  return {
    nodes: nodes.map(n => ({
      id: n.id,
      type: n.data.nodeType,  // "csv_source", "groupby", etc.
      config: n.data.config || {},
    })),
    edges: edges.map(e => ({
      source: e.source,
      target: e.target,
    })),
  };
}
```

**Step 2: Call backend on Run click, store results**

```typescript
const handleRun = async () => {
  const payload = toPipelinePayload(nodes, edges);
  const results = await runPipeline(payload);
  setNodeResults(results);
};
```

**Step 3: Show node status on canvas (green check for done, red X for error)**

Update node data with status from results. React Flow custom nodes render status indicators.

**Step 4: Test end-to-end manually**

1. Start backend: `TEIDE_LIB=build_release/libteide.so cd mirador && python -m mirador.app`
2. Start frontend: `cd mirador/frontend && npm run dev`
3. Drag CSV Source node, configure with a file path
4. Drag Grid node, connect them
5. Click Run → Grid shows data in Preview Panel

**Step 5: Commit**

```bash
git add -A && git commit -m "feat(mirador): end-to-end pipeline execution via Run button"
```

---

### Task 16: Add remaining compute nodes (Sort, Formula, Python Script)

**Files:**
- Create: `mirador/mirador/nodes/compute/sort.py`
- Create: `mirador/mirador/nodes/compute/formula.py`
- Create: `mirador/mirador/nodes/generic/script.py`
- Create: `mirador/mirador/nodes/generic/dict_transform.py`
- Create: `mirador/mirador/nodes/generic/conditional.py`

Follow the same BaseNode pattern. Each with tests.

- **SortNode**: wraps `table.sort()`, config: `{columns: [{name, desc}]}`
- **FormulaNode**: parses expression string like `revenue - cost` into `col("revenue") - col("cost")`, adds computed column
- **ScriptNode**: `exec()` user Python code in sandbox, receives/returns dict
- **DictTransformNode**: rename/pick/reshape dict fields
- **ConditionalNode**: if/else routing — has two output ports

**Commit after each node implementation.**

---

### Task 17: Add Chart and Export output nodes

**Files:**
- Create: `mirador/mirador/nodes/outputs/chart.py`
- Create: `mirador/mirador/nodes/outputs/export.py`

- **ChartNode**: generates ECharts option JSON from input dataframe. Config: chart type, x/y columns, grouping. Frontend renders with ECharts.
- **ExportNode**: writes CSV/JSON/PDF to project outputs directory. Returns `{path, size, format}`.

For PDF export, use `reportlab` or `weasyprint` (add to optional deps).

**Commit after each.**

---

### Task 18: Frontend — Custom node components + chart rendering

**Files:**
- Create: `mirador/frontend/src/nodes/` — custom React Flow node components per type

```bash
cd mirador/frontend && npm install echarts echarts-for-react
```

Each node type gets a custom React component with:
- Colored header by category (blue=input, green=compute, purple=generic, orange=output)
- Input/output port handles
- Status indicator (idle/running/done/error)
- Mini config summary on the node face

Chart preview uses `echarts-for-react` in the Preview Panel when a Chart node is selected.

**Commit.**

---

### Task 19: Bundled deployment — serve frontend from FastAPI

**Files:**
- Modify: `mirador/mirador/app.py` — serve static files
- Modify: `mirador/pyproject.toml` — include frontend dist

**Step 1: Build frontend**

```bash
cd mirador/frontend && npm run build  # outputs to dist/
```

**Step 2: Copy dist to mirador package and serve**

```python
# In app.py, after API routes:
frontend_dir = Path(__file__).parent / "frontend" / "dist"
if frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dir), html=True))
```

**Step 3: Test single-process deployment**

```bash
TEIDE_LIB=build_release/libteide.so python -m mirador.app
# Open http://localhost:8000 — should serve the full Mirador UI
```

**Step 4: Commit**

```bash
git add -A && git commit -m "feat(mirador): bundled deployment — frontend served from FastAPI"
```

---

### Task 20: Project storage with Teide splayed tables

**Files:**
- Create: `mirador/mirador/storage/projects.py`
- Create: `mirador/mirador/storage/pipelines.py`
- Create: `mirador/mirador/api/projects.py`

**Step 1: Implement project storage**

Projects stored as directories under `mirador_data/projects/`. Pipeline definitions stored as JSON files (simpler for v1 — can migrate to Teide splayed tables later when the JSON→Table serialization is more mature).

```python
# mirador/mirador/storage/projects.py
import json
from pathlib import Path

DATA_ROOT = Path("mirador_data")

class ProjectStore:
    def __init__(self, root: Path = DATA_ROOT):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def list_projects(self) -> list[dict]:
        projects = []
        proj_dir = self.root / "projects"
        if proj_dir.exists():
            for p in sorted(proj_dir.iterdir()):
                if p.is_dir():
                    meta = self._read_meta(p)
                    projects.append(meta)
        return projects

    def create_project(self, name: str) -> dict:
        slug = name.lower().replace(" ", "_")
        proj_dir = self.root / "projects" / slug
        proj_dir.mkdir(parents=True, exist_ok=True)
        (proj_dir / "data").mkdir(exist_ok=True)
        (proj_dir / "outputs").mkdir(exist_ok=True)
        (proj_dir / "pipelines").mkdir(exist_ok=True)
        meta = {"name": name, "slug": slug}
        (proj_dir / "meta.json").write_text(json.dumps(meta))
        return meta

    def save_pipeline(self, project_slug: str, pipeline_name: str, pipeline: dict):
        path = self.root / "projects" / project_slug / "pipelines" / f"{pipeline_name}.json"
        path.write_text(json.dumps(pipeline, indent=2))

    def load_pipeline(self, project_slug: str, pipeline_name: str) -> dict:
        path = self.root / "projects" / project_slug / "pipelines" / f"{pipeline_name}.json"
        return json.loads(path.read_text())

    def _read_meta(self, proj_dir: Path) -> dict:
        meta_file = proj_dir / "meta.json"
        if meta_file.exists():
            return json.loads(meta_file.read_text())
        return {"name": proj_dir.name, "slug": proj_dir.name}
```

**Step 2: Add project CRUD API endpoints**

```python
# mirador/mirador/api/projects.py
from fastapi import APIRouter
from mirador.storage.projects import ProjectStore

router = APIRouter(prefix="/api/projects", tags=["projects"])
store = ProjectStore()

@router.get("")
def list_projects():
    return store.list_projects()

@router.post("")
def create_project(body: dict):
    return store.create_project(body["name"])

@router.get("/{slug}/pipelines/{name}")
def get_pipeline(slug: str, name: str):
    return store.load_pipeline(slug, name)

@router.put("/{slug}/pipelines/{name}")
def save_pipeline(slug: str, name: str, pipeline: dict):
    store.save_pipeline(slug, name, pipeline)
    return {"status": "saved"}
```

**Step 3: Test and commit**

```bash
git add -A && git commit -m "feat(mirador): add project storage and CRUD API"
```

---

## Summary

| Phase | Tasks | Description |
|-------|-------|-------------|
| 1 | 1-2 | Backend scaffold, Teide lifespan |
| 2 | 3-7 | Node system, all node implementations |
| 3 | 8 | Pipeline executor (topological sort + eager) |
| 4 | 9-10 | REST API + WebSocket |
| 5 | 11-14 | Frontend scaffold, canvas, panels |
| 6 | 15-20 | Integration, more nodes, charts, storage |

**Critical path for first demo:** Tasks 1 → 2 → 3 → 4 → 5 → 8 → 9 → 11 → 12 → 13 → 14 → 15

After Task 15, you have a working end-to-end: drag CSV + Grid nodes, connect, click Run, see data. Everything else (more nodes, charts, storage) builds on that foundation.
