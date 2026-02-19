"""AI node — calls an LLM with a prompt template over input data."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class AiNode(BaseNode):
    meta = NodeMeta(
        id="ai",
        label="AI",
        category="generic",
        description="Process data with an AI model",
        inputs=[NodePort(name="in", description="Input data")],
        outputs=[NodePort(name="out", description="AI output")],
        config_schema={
            "type": "object",
            "properties": {
                "prompt": {"type": "string", "title": "Prompt",
                           "description": "Prompt template (use {{column}} for interpolation)"},
                "model": {"type": "string", "title": "Model",
                          "description": "Model identifier (e.g. gpt-4, claude-3)"},
            },
            "required": ["prompt"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        # Stub: in production this would call an LLM API
        prompt = config.get("prompt", "")
        model = config.get("model", "stub")
        return {
            "prompt": prompt,
            "model": model,
            "result": f"[AI stub] Would process {len(inputs)} input(s) with model={model}",
            **inputs,
        }
