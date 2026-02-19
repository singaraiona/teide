"""Sort node — sorts rows by one or more columns."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class SortNode(BaseNode):
    meta = NodeMeta(
        id="sort",
        label="Sort",
        category="compute",
        description="Sort rows by columns",
        inputs=[NodePort(name="in", description="Input dataframe")],
        outputs=[NodePort(name="out", description="Sorted dataframe")],
        config_schema={
            "type": "object",
            "properties": {
                "columns": {
                    "type": "array",
                    "title": "Sort Columns",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "descending": {"type": "boolean", "default": False},
                        },
                        "required": ["name"],
                    },
                },
            },
            "required": ["columns"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        table = inputs["df"]
        columns = config["columns"]
        col_names = [c["name"] for c in columns]
        descs = [c.get("descending", False) for c in columns]

        result = table.sort(*col_names, descending=descs).collect()
        return {
            "df": result,
            "rows": len(result),
            "columns": result.columns,
        }
