"""Dict Transform node — rename, pick, or drop fields from a dict."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class DictTransformNode(BaseNode):
    meta = NodeMeta(
        id="dict_transform",
        label="Dict Transform",
        category="generic",
        description="Rename, pick, or drop fields from a dict",
        inputs=[NodePort(name="in", description="Input dict")],
        outputs=[NodePort(name="out", description="Transformed dict")],
        config_schema={
            "type": "object",
            "properties": {
                "rename": {"type": "object", "title": "Rename Fields",
                           "description": "Map of old_name -> new_name",
                           "additionalProperties": {"type": "string"}},
                "pick": {"type": "array", "items": {"type": "string"},
                         "title": "Pick Fields",
                         "description": "Keep only these fields"},
                "drop": {"type": "array", "items": {"type": "string"},
                         "title": "Drop Fields",
                         "description": "Remove these fields"},
            },
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        result = dict(inputs)

        # Drop fields first
        for key in config.get("drop", []):
            result.pop(key, None)

        # Pick fields (keep only these)
        pick = config.get("pick")
        if pick:
            result = {k: v for k, v in result.items() if k in pick}

        # Rename fields last
        for old, new in config.get("rename", {}).items():
            if old in result:
                result[new] = result.pop(old)

        return result
