"""Gmail node — send or read emails via Gmail API."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class GmailNode(BaseNode):
    meta = NodeMeta(
        id="gmail",
        label="Gmail",
        category="generic",
        description="Send or read emails via Gmail",
        inputs=[NodePort(name="in", description="Input data")],
        outputs=[NodePort(name="out", description="Email result")],
        config_schema={
            "type": "object",
            "properties": {
                "action": {"type": "string", "title": "Action",
                           "enum": ["send", "read"],
                           "description": "Send or read emails"},
                "to": {"type": "string", "title": "To",
                       "description": "Recipient email address"},
                "subject": {"type": "string", "title": "Subject"},
                "body": {"type": "string", "title": "Body"},
            },
            "required": ["action"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        action = config.get("action", "read")
        return {
            "action": action,
            "result": f"[Gmail stub] Would {action} email",
            **inputs,
        }
