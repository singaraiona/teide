"""Export output node — writes dataframe to CSV or JSON files."""

import csv
import json
import os
from typing import Any

from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class ExportNode(BaseNode):
    meta = NodeMeta(
        id="export",
        label="Export",
        category="output",
        description="Export data to a file (CSV or JSON)",
        inputs=[NodePort(name="in", description="Dataframe to export")],
        outputs=[],
        config_schema={
            "type": "object",
            "properties": {
                "format": {
                    "type": "string",
                    "title": "Format",
                    "enum": ["csv", "json"],
                    "default": "csv",
                },
                "output_path": {"type": "string", "title": "Output Path"},
            },
            "required": ["format", "output_path"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        table = inputs.get("df")
        if table is None:
            raise ValueError("No input dataframe provided (missing 'df' in inputs)")

        fmt = config.get("format", "csv")
        output_path = config.get("output_path")
        if not output_path:
            raise ValueError("output_path is required")

        columns = inputs.get("columns", table.columns if hasattr(table, "columns") else [])
        n = len(table)
        data = table.to_dict()

        # Build list of row dicts
        rows = []
        for i in range(n):
            row = {col: data[col][i] for col in columns}
            rows.append(row)

        if fmt == "csv":
            _write_csv(output_path, columns, rows)
        elif fmt == "json":
            _write_json(output_path, rows)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        size = os.path.getsize(output_path)

        return {
            "path": output_path,
            "size": size,
            "format": fmt,
            "rows": n,
        }


def _write_csv(path: str, columns: list[str], rows: list[dict]) -> None:
    """Write rows to a CSV file."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: str, rows: list[dict]) -> None:
    """Write rows to a JSON file."""
    with open(path, "w") as f:
        json.dump(rows, f)
