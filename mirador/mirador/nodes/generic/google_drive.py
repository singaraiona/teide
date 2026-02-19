"""Google Drive node — upload, download, or list files."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class GoogleDriveNode(BaseNode):
    meta = NodeMeta(
        id="google_drive",
        label="Google Drive",
        category="generic",
        description="Upload, download, or list files in Google Drive",
        inputs=[NodePort(name="in", description="Input data")],
        outputs=[NodePort(name="out", description="Drive result")],
        config_schema={
            "type": "object",
            "properties": {
                "action": {"type": "string", "title": "Action",
                           "enum": ["upload", "download", "list"],
                           "description": "Drive operation to perform"},
                "file_id": {"type": "string", "title": "File ID",
                            "description": "Google Drive file ID (for download)"},
                "folder_id": {"type": "string", "title": "Folder ID",
                              "description": "Target folder ID (for upload/list)"},
            },
            "required": ["action"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        action = config.get("action", "list")
        return {
            "action": action,
            "result": f"[Google Drive stub] Would {action} files",
            **inputs,
        }
