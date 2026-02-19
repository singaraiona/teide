"""Node types API — lists available node types for the frontend palette."""

from fastapi import APIRouter

from mirador.engine.registry import NodeRegistry

router = APIRouter(prefix="/api/nodes", tags=["nodes"])

_registry: NodeRegistry | None = None


def get_registry() -> NodeRegistry:
    global _registry
    if _registry is None:
        _registry = NodeRegistry()
        _registry.discover()
    return _registry


@router.get("")
def list_node_types():
    """Return metadata for all available node types."""
    return get_registry().list_meta()
