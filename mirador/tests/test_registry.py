from mirador.nodes.base import BaseNode, NodeMeta, NodePort
from mirador.engine.registry import NodeRegistry


def test_base_node_not_implemented():
    """BaseNode.execute must be overridden."""
    node = BaseNode()
    try:
        node.execute({}, {})
        assert False, "Should raise NotImplementedError"
    except NotImplementedError:
        pass


def test_node_meta_pydantic():
    """NodeMeta should be a valid Pydantic model."""
    meta = NodeMeta(id="test", label="Test", category="input")
    data = meta.model_dump()
    assert data["id"] == "test"
    assert data["inputs"] == []


def test_registry_empty():
    """Registry with no node files should still work."""
    reg = NodeRegistry()
    reg.discover()
    # May find nodes if any exist, but should not crash
    assert isinstance(reg.node_types, dict)
