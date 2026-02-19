"""Data grid output node."""

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
        n = len(table)
        data = table.to_dict()

        rows = []
        for i in range(min(n, page_size)):
            row = {col: data[col][i] for col in columns}
            rows.append(row)

        return {
            "rows": rows,
            "columns": columns,
            "total": n,
        }
