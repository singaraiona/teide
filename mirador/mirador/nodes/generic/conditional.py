"""Conditional node — routes data based on a field condition."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


_OPS = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "gt": lambda a, b: a > b,
    "lt": lambda a, b: a < b,
    "ge": lambda a, b: a >= b,
    "le": lambda a, b: a <= b,
}


class ConditionalNode(BaseNode):
    meta = NodeMeta(
        id="conditional",
        label="Conditional",
        category="generic",
        description="Route data based on a field condition",
        inputs=[NodePort(name="in", description="Input data")],
        outputs=[
            NodePort(name="true", description="Output when condition is true"),
            NodePort(name="false", description="Output when condition is false"),
        ],
        config_schema={
            "type": "object",
            "properties": {
                "field": {"type": "string", "title": "Field",
                          "description": "Field name to check"},
                "operator": {"type": "string", "title": "Operator",
                             "enum": ["eq", "ne", "gt", "lt", "ge", "le"]},
                "value": {"title": "Value",
                          "description": "Value to compare against"},
            },
            "required": ["field", "operator", "value"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        field = config["field"]
        operator = config["operator"]
        value = config["value"]

        actual = inputs.get(field)
        if actual is None:
            raise KeyError(f"Field '{field}' not found in inputs")

        # Coerce value to match the type of the actual field
        if isinstance(actual, (int, float)) and isinstance(value, str):
            try:
                value = type(actual)(value)
            except (ValueError, TypeError):
                pass

        op_fn = _OPS.get(operator)
        if op_fn is None:
            raise ValueError(f"Unknown operator: {operator}")

        condition_met = op_fn(actual, value)
        return {
            **inputs,
            "branch": "true" if condition_met else "false",
            "condition_met": condition_met,
        }
