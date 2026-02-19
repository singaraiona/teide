"""Script node — executes user Python code in a restricted sandbox."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


# Allowlisted builtins for the sandbox
_SAFE_BUILTINS = {
    "len": len, "sum": sum, "min": min, "max": max, "range": range,
    "int": int, "float": float, "str": str, "list": list, "dict": dict,
    "tuple": tuple, "set": set, "bool": bool,
    "sorted": sorted, "enumerate": enumerate, "zip": zip, "map": map,
    "filter": filter, "abs": abs, "round": round, "print": print,
    "isinstance": isinstance, "type": type,
    "True": True, "False": False, "None": None,
}


class ScriptNode(BaseNode):
    meta = NodeMeta(
        id="script",
        label="Script",
        category="generic",
        description="Execute Python code in a sandbox",
        inputs=[NodePort(name="in", description="Input data")],
        outputs=[NodePort(name="out", description="Script output")],
        config_schema={
            "type": "object",
            "properties": {
                "code": {"type": "string", "title": "Python Code",
                         "description": "Set 'output' variable with result dict"},
            },
            "required": ["code"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        code = config["code"]
        sandbox = {
            "input": inputs,
            "output": {},
            "__builtins__": dict(_SAFE_BUILTINS),
        }
        compiled = compile(code, '<script>', 'exec')
        exec(compiled, sandbox)
        result = sandbox["output"]
        if not isinstance(result, dict):
            raise TypeError(f"Script 'output' must be a dict, got {type(result).__name__}")
        return result
