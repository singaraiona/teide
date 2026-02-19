"""Base node definitions for Mirador pipeline nodes."""

from typing import Any
from pydantic import BaseModel, Field


class NodePort(BaseModel):
    """Describes an input or output port on a node."""
    name: str
    description: str = ""


class NodeMeta(BaseModel):
    """Metadata describing a node type for the frontend palette."""
    id: str                                          # e.g. "csv_source"
    label: str                                       # e.g. "CSV File"
    category: str                                    # "input" | "compute" | "generic" | "output"
    description: str = ""
    inputs: list[NodePort] = Field(default_factory=list)
    outputs: list[NodePort] = Field(default_factory=list)
    config_schema: dict[str, Any] = Field(default_factory=dict)  # JSON Schema


class BaseNode:
    """Base class for all Mirador pipeline nodes.

    Subclasses must define a `meta` class attribute (NodeMeta) and override `execute()`.
    """

    meta: NodeMeta  # subclasses define this

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """Execute this node. Receives merged upstream dicts, returns output dict."""
        raise NotImplementedError
