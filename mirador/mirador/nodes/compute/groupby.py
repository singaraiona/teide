"""Group By node — groups rows and aggregates."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class GroupByNode(BaseNode):
    meta = NodeMeta(
        id="groupby",
        label="Group By",
        category="compute",
        description="Group rows and aggregate values",
        inputs=[NodePort(name="in", description="Input dataframe")],
        outputs=[NodePort(name="out", description="Aggregated dataframe")],
        config_schema={
            "type": "object",
            "properties": {
                "keys": {"type": "array", "items": {"type": "string"}, "title": "Group Keys"},
                "aggs": {
                    "type": "array",
                    "title": "Aggregations",
                    "items": {
                        "type": "object",
                        "properties": {
                            "column": {"type": "string"},
                            "op": {"type": "string",
                                   "enum": ["sum", "avg", "min", "max", "count"]},
                        },
                        "required": ["column", "op"],
                    },
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
