"""Filter node — filters rows by condition."""

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
                "operator": {"type": "string", "title": "Operator",
                             "enum": ["eq", "ne", "gt", "lt", "ge", "le"]},
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

        # Try to convert value to number if it looks numeric
        try:
            value = int(value)
        except (ValueError, TypeError):
            try:
                value = float(value)
            except (ValueError, TypeError):
                pass

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
