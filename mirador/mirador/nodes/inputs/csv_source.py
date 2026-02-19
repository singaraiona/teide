"""CSV file source node."""

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class CsvSourceNode(BaseNode):
    meta = NodeMeta(
        id="csv_source",
        label="CSV File",
        category="input",
        description="Load data from a CSV file",
        inputs=[],
        outputs=[NodePort(name="out", description="Loaded dataframe")],
        config_schema={
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "title": "File Path"},
            },
            "required": ["file_path"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        from mirador.app import get_teide
        from teide.api import Table

        lib = get_teide()
        path = config["file_path"]
        tbl_ptr = lib.read_csv(path)
        if not tbl_ptr or tbl_ptr < 32:
            raise RuntimeError(f"Failed to read CSV: {path}")

        table = Table(lib, tbl_ptr)
        return {
            "df": table,
            "rows": len(table),
            "columns": table.columns,
        }
