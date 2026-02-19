"""Join node — joins two tables."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class JoinNode(BaseNode):
    meta = NodeMeta(
        id="join",
        label="Join",
        category="compute",
        description="Join with another table",
        inputs=[NodePort(name="in", description="Left table")],
        outputs=[NodePort(name="out", description="Joined table")],
        config_schema={
            "type": "object",
            "properties": {
                "right_file": {"type": "string", "title": "Right Table (CSV path)"},
                "keys": {"type": "array", "items": {"type": "string"}, "title": "Join Keys"},
                "how": {"type": "string", "enum": ["inner", "left"], "default": "inner"},
            },
            "required": ["right_file", "keys"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        from mirador.app import get_teide
        from teide.api import Table

        left = inputs["df"]
        lib = get_teide()

        # Load right table from CSV
        right_ptr = lib.read_csv(config["right_file"])
        if not right_ptr or right_ptr < 32:
            raise RuntimeError(f"Failed to load right table: {config['right_file']}")
        right = Table(lib, right_ptr)

        keys = config["keys"]
        how = config.get("how", "inner")
        result = left.join(right, on=keys, how=how)
        return {
            "df": result,
            "rows": len(result),
            "columns": result.columns,
        }
