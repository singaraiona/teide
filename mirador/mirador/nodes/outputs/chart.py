"""Chart output node — generates Apache ECharts option specs."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class ChartNode(BaseNode):
    meta = NodeMeta(
        id="chart",
        label="Chart",
        category="output",
        description="Visualize data as a chart (bar, line, pie, scatter)",
        inputs=[NodePort(name="in", description="Dataframe to visualize")],
        outputs=[],
        config_schema={
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "title": "Chart Type",
                    "enum": ["bar", "line", "pie", "scatter"],
                    "default": "bar",
                },
                "x_column": {"type": "string", "title": "X Column"},
                "y_column": {"type": "string", "title": "Y Column"},
                "title": {"type": "string", "title": "Title", "default": ""},
            },
            "required": ["chart_type", "x_column", "y_column"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        table = inputs.get("df")
        if table is None:
            return {"chart_type": None, "options": {}, "columns": [], "rows": 0}

        chart_type = config.get("chart_type", "bar")
        x_col = config.get("x_column")
        y_col = config.get("y_column")
        title = config.get("title", "")

        columns = inputs.get("columns", table.columns if hasattr(table, "columns") else [])
        n = len(table)
        data = table.to_dict()

        if x_col not in data:
            raise ValueError(f"x_column '{x_col}' not found in table columns: {list(data.keys())}")
        if y_col not in data:
            raise ValueError(f"y_column '{y_col}' not found in table columns: {list(data.keys())}")

        x_data = data[x_col]
        y_data = data[y_col]

        options = _build_options(chart_type, x_data, y_data, title)

        return {
            "chart_type": chart_type,
            "options": options,
            "columns": columns,
            "rows": n,
        }


def _build_options(
    chart_type: str,
    x_data: list,
    y_data: list,
    title: str,
) -> dict[str, Any]:
    """Build an ECharts option spec for the given chart type."""
    opts: dict[str, Any] = {}

    if title:
        opts["title"] = {"text": title}

    if chart_type in ("bar", "line"):
        opts["xAxis"] = {"type": "category", "data": list(x_data)}
        opts["yAxis"] = {"type": "value"}
        opts["series"] = [{"type": chart_type, "data": list(y_data)}]
        opts["tooltip"] = {"trigger": "axis"}

    elif chart_type == "pie":
        opts["series"] = [
            {
                "type": "pie",
                "data": [
                    {"name": x_data[i], "value": y_data[i]}
                    for i in range(len(x_data))
                ],
            }
        ]
        opts["tooltip"] = {"trigger": "item"}

    elif chart_type == "scatter":
        opts["xAxis"] = {"type": "value"}
        opts["yAxis"] = {"type": "value"}
        opts["series"] = [
            {
                "type": "scatter",
                "data": [[x_data[i], y_data[i]] for i in range(len(x_data))],
            }
        ]
        opts["tooltip"] = {"trigger": "item"}

    return opts
