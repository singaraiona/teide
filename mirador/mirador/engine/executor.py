"""Pipeline executor — walks DAG topologically and executes nodes eagerly."""

from collections import defaultdict
from typing import Any, Callable
from mirador.engine.registry import NodeRegistry

# In-memory session cache: session_id -> {node_id: full_output_dict}
# Keeps raw DataFrames alive so resume can feed them to downstream nodes.
_session_cache: dict[str, dict[str, Any]] = {}
_MAX_SESSIONS = 20


class PipelineExecutor:
    """Walks a pipeline DAG in topological order, executing each node eagerly."""

    def __init__(self, registry: NodeRegistry):
        self.registry = registry

    def run(
        self,
        pipeline: dict[str, Any],
        on_node_start: Callable[[str], None] | None = None,
        on_node_done: Callable[[str, dict], None] | None = None,
        on_node_error: Callable[[str, Exception], None] | None = None,
        session_id: str | None = None,
        start_from: str | None = None,
    ) -> dict[str, Any]:
        """Execute the pipeline, return {node_id: output_dict}."""
        nodes = {n["id"]: n for n in pipeline["nodes"]}
        edges = pipeline.get("edges", [])

        # Build adjacency: target -> list of source node_ids
        upstream = defaultdict(list)
        downstream = defaultdict(list)
        for e in edges:
            upstream[e["target"]].append(e["source"])
            downstream[e["source"]].append(e["target"])

        # Topological sort (Kahn's algorithm)
        in_degree = {n_id: 0 for n_id in nodes}
        for e in edges:
            in_degree[e["target"]] += 1

        queue = [n_id for n_id in nodes if in_degree[n_id] == 0]
        order = []
        while queue:
            n_id = queue.pop(0)
            order.append(n_id)
            for target in downstream[n_id]:
                in_degree[target] -= 1
                if in_degree[target] == 0:
                    queue.append(target)

        if len(order) != len(nodes):
            raise ValueError("Pipeline has a cycle")

        # Determine start index for resume
        results: dict[str, Any] = {}
        start_idx = 0

        if start_from and session_id and session_id in _session_cache:
            cached = _session_cache[session_id]
            if start_from in order:
                start_idx = order.index(start_from)
                # Restore cached results for nodes before start_from
                for n_id in order[:start_idx]:
                    if n_id in cached:
                        results[n_id] = cached[n_id]

        # Execute from start_idx onward
        for n_id in order[start_idx:]:
            node_def = nodes[n_id]
            node_cls = self.registry.get(node_def["type"])
            node = node_cls()

            # Merge upstream outputs into input dict
            inputs: dict[str, Any] = {}
            for up_id in upstream[n_id]:
                up_out = results.get(up_id, {})
                inputs.update(up_out)

            if on_node_start:
                on_node_start(n_id)

            try:
                output = node.execute(inputs, node_def.get("config", {}))
                results[n_id] = output
                if on_node_done:
                    on_node_done(n_id, output)
            except Exception as exc:
                results[n_id] = {"error": str(exc)}
                if on_node_error:
                    on_node_error(n_id, exc)
                break  # stop on first error

        # Cache results for future resume
        if session_id:
            _session_cache[session_id] = results
            if len(_session_cache) > _MAX_SESSIONS:
                oldest = next(iter(_session_cache))
                del _session_cache[oldest]

        return results
