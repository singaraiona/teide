# Mirador — Visual Analytics Pipeline Builder

*Design Document — 2026-02-19*

## 1. Overview

Mirador is a visual analytics pipeline builder powered by Teide's C engine. Users drag nodes onto a canvas, connect them, configure parameters, and click Run to produce outputs — tables, charts, PDFs, CSVs. Think n8n, but purpose-built for data analytics.

**Name**: Mirador — Spanish for "viewpoint". A viewpoint onto your data.

**Business model**: Open-core. Self-hosted is free, cloud-hosted SaaS is paid (n8n model).

**Target users**: Business users — analysts, risk managers, report builders. Approachable drag-and-drop UX, no code required for most workflows. Power users get formula expressions and Python script nodes.

## 2. Tech Stack

| Layer | Technology | Rationale |
|-------|-----------|-----------|
| Backend | Python / FastAPI | Python script nodes run natively; data ecosystem (pandas, numpy, plotly) available; Teide ctypes bindings exist |
| Frontend | React + React Flow | React Flow provides production-grade node canvas; large ecosystem |
| Engine | Teide via ctypes | Fast C engine for filter, group-by, sort, join, window operations |
| Storage | Teide splayed/parted tables | Dog-fooding — no external database dependency |
| Charts | Apache ECharts (JS) | Feature-rich, open source, good React integration |
| Code editor | Monaco / CodeMirror | For formula and Python script nodes |

## 3. Data Model

**Dict is the universal wire format.** Every node receives a dict, returns a dict. Fields vary by context:

```python
# File source output
{"df": <teide_dataframe>, "rows": 10000, "columns": ["id1", "v1", ...]}

# Group-by output
{"df": <teide_dataframe>, "rows": 100, "columns": ["id1", "sum_v1"]}

# Scalar result
{"value": 42, "label": "total_revenue"}

# Export output
{"path": "/outputs/report.pdf", "size": 1024}
```

Teide dataframes live inside the dict as one field. Generic nodes pass dicts around. No type coercion — dicts all the way down.

## 4. System Architecture

```
┌─────────────────────────────────────────────────┐
│                   Browser                        │
│  ┌─────────────────────────────────────────────┐ │
│  │         React + React Flow Canvas           │ │
│  │  ┌───┐    ┌───┐    ┌───┐    ┌───┐          │ │
│  │  │CSV│───→│Grp│───→│Fml│───→│Grd│          │ │
│  │  └───┘    └───┘    └───┘    └───┘          │ │
│  └─────────────────────────────────────────────┘ │
└──────────────────────┬──────────────────────────┘
                       │ REST + WebSocket
┌──────────────────────▼──────────────────────────┐
│              FastAPI Backend                      │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ Pipeline  │  │ Executor │  │  Project      │  │
│  │ Manager   │  │ Engine   │  │  Manager      │  │
│  └──────────┘  └────┬─────┘  └───────────────┘  │
│                     │                            │
│              ┌──────▼──────┐                     │
│              │ Teide Engine │                     │
│              │  (ctypes)    │                     │
│              └──────┬──────┘                     │
│                     │                            │
│              ┌──────▼──────┐                     │
│              │  File Store  │                     │
│              │ (splayed/    │                     │
│              │  parted)     │                     │
│              └─────────────┘                     │
└──────────────────────────────────────────────────┘
```

**Three backend services:**

- **Pipeline Manager** — CRUD for pipeline definitions (nodes, edges, configs). Stores pipeline JSON as Teide tables.
- **Executor Engine** — takes a pipeline definition, walks the DAG topologically, executes each node eagerly via Teide, caches intermediate results in memory.
- **Project Manager** — workspaces, uploaded files, output history. Each project is a directory with splayed Teide tables.

**Communication:** REST for CRUD operations, WebSocket for live execution updates (node status: pending → running → done, intermediate result previews).

## 5. Node Types

### 5.1 Input Nodes

| Node | Config | Output |
|------|--------|--------|
| CSV Source | file path, delimiter, header, encoding | `{"df": ..., "rows": N, "columns": [...]}` |
| JSON Source | file path, root key | `{"data": ..., "type": "dict\|list"}` |
| Parquet Source | file path | `{"df": ..., "rows": N, "columns": [...]}` |
| Excel Source | file path, sheet name | `{"df": ..., "rows": N, "columns": [...]}` |

### 5.2 Teide Compute Nodes

| Node | Config | Maps to |
|------|--------|---------|
| Filter | column, operator, value | `td_filter` |
| Group By | key columns, aggregations per column | `td_group` |
| Sort | columns + ASC/DESC | `td_sort_op` |
| Join | join type, left keys, right keys | `td_join` |
| Window | partition cols, order cols, function | `td_window` |
| Formula | output column name + expression | Parsed → `td_add`, `td_mul`, etc. |

### 5.3 Generic Nodes

| Node | Description |
|------|-------------|
| Dict Transform | Rename, pick, or reshape dict fields |
| Conditional | If/else branching based on a condition — routes data to one of two outputs |
| Python Script | Code editor; receives `input` dict, returns output dict |

### 5.4 Output Nodes

| Node | Config | Result |
|------|--------|--------|
| Grid | page size, sortable columns | Interactive table in preview panel |
| Chart | chart type (bar/line/pie/scatter), axes, grouping | ECharts spec rendered in preview |
| Export | format (CSV, JSON, PDF) | Downloadable file |

## 6. Pipeline Execution

When the user clicks **Run**:

1. **Topological sort** — walk the node graph, determine execution order respecting edges.
2. **Execute node-by-node** — for each node in order:
   - Gather input dicts from upstream nodes (already executed).
   - Call the node's handler: `def execute(input: dict, config: dict) -> dict`.
   - Store the output dict in memory.
   - Push status update to frontend via WebSocket.
3. **Output nodes** — render grid data, generate chart specs, write export files.
4. **Cache results** — intermediate dicts stay in memory so users can click any node and inspect its output without re-running.

**Error handling:** If a node fails, execution stops. The failed node turns red in the UI, error message displayed. Upstream results preserved — user can fix the broken node and re-run from that point forward.

## 7. Frontend Layout

```
┌─────────────────────────────────────────────────────────┐
│ ┌──────────┐                                            │
│ │ Projects │  Mirador            [Run ▶] [Save] [Export]│
│ ├──────────┤ ┌─────────────────────────────────────────┐│
│ │ Node     │ │                                         ││
│ │ Palette  │ │       ┌───┐    ┌───┐    ┌───┐          ││
│ │          │ │       │CSV│───→│Grp│───→│Grd│          ││
│ │ ▸ Input  │ │       └───┘    └───┘    └───┘          ││
│ │ ▸ Compute│ │                                         ││
│ │ ▸ Generic│ │            Canvas                       ││
│ │ ▸ Output │ │                                         ││
│ │          │ │                                         ││
│ ├──────────┤ └─────────────────────────────────────────┘│
│ │ Inspector│ ┌─────────────────────────────────────────┐│
│ │          │ │  Preview Panel                          ││
│ │ Config   │ │  ┌─────┬────┬────┐                     ││
│ │ panel for│ │  │ id1 │ v1 │sum │  (click any node    ││
│ │ selected │ │  ├─────┼────┼────┤   to see its data)  ││
│ │ node     │ │  │ a   │ 10 │ 50 │                     ││
│ └──────────┘ └─────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

**Four zones:**

- **Left sidebar** — project browser (top), node palette (middle, drag to canvas), inspector/config panel (bottom, configures selected node).
- **Center canvas** — React Flow graph. Drag/drop nodes, draw edges, zoom/pan.
- **Bottom panel** — preview of selected node's output (data grid, chart preview, or raw dict).
- **Top bar** — project name, Run/Save/Export buttons, execution status.

## 8. Project Storage

```
mirador_data/
├── _meta/                    # Global metadata
│   ├── projects/             # Splayed table: project list
│   └── users/                # Splayed table: user accounts (cloud only)
│
├── projects/
│   ├── risk_report_q4/
│   │   ├── _meta/            # Project metadata (name, created, modified)
│   │   ├── pipelines/
│   │   │   ├── main/         # Pipeline definition (splayed)
│   │   │   │   ├── nodes     # Column: node defs as JSON strings
│   │   │   │   ├── edges     # Column: edge connections
│   │   │   │   └── config    # Column: per-node config blobs
│   │   │   └── variant_a/    # Multiple pipelines per project
│   │   ├── data/             # Uploaded source files
│   │   │   ├── sales.csv
│   │   │   └── inventory.parquet
│   │   └── outputs/          # Generated exports
│   │       ├── report.pdf
│   │       └── summary.csv
```

All metadata stored in Teide splayed tables. No SQLite, no external database.

## 9. Python Package Structure

```
mirador/
├── pyproject.toml
├── mirador/
│   ├── __init__.py
│   ├── app.py                  # FastAPI entry point
│   ├── api/                    # REST + WebSocket endpoints
│   │   ├── projects.py
│   │   ├── pipelines.py
│   │   └── ws.py
│   ├── engine/                 # Pipeline execution
│   │   ├── executor.py         # Topological walk, node dispatch
│   │   ├── registry.py         # Node type autodiscovery
│   │   └── sandbox.py          # Python script sandboxing
│   ├── nodes/                  # Node implementations
│   │   ├── base.py             # BaseNode: execute(input, config) -> dict
│   │   ├── inputs/
│   │   │   ├── csv_source.py
│   │   │   ├── json_source.py
│   │   │   └── parquet_source.py
│   │   ├── compute/
│   │   │   ├── filter.py
│   │   │   ├── groupby.py
│   │   │   ├── sort.py
│   │   │   ├── join.py
│   │   │   ├── window.py
│   │   │   └── formula.py
│   │   ├── generic/
│   │   │   ├── dict_transform.py
│   │   │   ├── conditional.py
│   │   │   └── script.py
│   │   └── outputs/
│   │       ├── grid.py
│   │       ├── chart.py
│   │       └── export.py
│   ├── storage/                # Teide-backed persistence
│   │   ├── projects.py
│   │   └── pipelines.py
│   └── frontend/
│       └── dist/               # Bundled React app
├── frontend/                   # React source
│   ├── package.json
│   └── src/
│       ├── App.tsx
│       ├── canvas/
│       ├── nodes/
│       ├── panels/
│       └── api/
```

Each node type is one file with a simple interface. Node registry autodiscovers files — adding a node type = adding one file.

## 10. v1 Scope

### Ship

- Canvas editor — drag nodes, connect edges, configure via inspector
- File input nodes: CSV, JSON, Parquet, Excel
- Teide compute nodes: filter, group-by, sort, join, window
- Formula node with expression parser
- Python script node (sandboxed)
- Generic nodes: dict transform, conditional
- Output nodes: grid, chart (ECharts), export (CSV, JSON, PDF)
- Eager execution with WebSocket progress
- Node output preview (click any node)
- Project management: create, save, open, delete
- All metadata in Teide splayed tables
- Single `pip install mirador` deployment

### Don't Ship (v2+)

- Scheduling / cron
- Database connectors
- API / webhook inputs
- Multi-user collaboration
- Version history / undo
- Cloud SaaS infrastructure
- Authentication / permissions
