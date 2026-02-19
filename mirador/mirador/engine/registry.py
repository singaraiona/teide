"""Node type registry with autodiscovery."""

import importlib
import pkgutil
from typing import Any

from mirador.nodes.base import BaseNode


class NodeRegistry:
    """Discovers and indexes all available node types."""

    def __init__(self):
        self.node_types: dict[str, type[BaseNode]] = {}

    def discover(self):
        """Scan mirador.nodes.* packages for BaseNode subclasses."""
        import mirador.nodes.inputs as inputs_pkg
        import mirador.nodes.compute as compute_pkg
        import mirador.nodes.generic as generic_pkg
        import mirador.nodes.outputs as outputs_pkg

        for pkg in [inputs_pkg, compute_pkg, generic_pkg, outputs_pkg]:
            for _importer, modname, _ispkg in pkgutil.iter_modules(pkg.__path__):
                mod = importlib.import_module(f"{pkg.__name__}.{modname}")
                for attr_name in dir(mod):
                    attr = getattr(mod, attr_name)
                    if (isinstance(attr, type)
                        and issubclass(attr, BaseNode)
                        and attr is not BaseNode
                        and hasattr(attr, 'meta')):
                        self.node_types[attr.meta.id] = attr

    def get(self, node_type_id: str) -> type[BaseNode]:
        """Get a node class by type ID. Raises KeyError if not found."""
        return self.node_types[node_type_id]

    def list_meta(self) -> list[dict[str, Any]]:
        """Return metadata for all registered nodes (for frontend palette)."""
        return [cls.meta.model_dump() for cls in self.node_types.values()]
