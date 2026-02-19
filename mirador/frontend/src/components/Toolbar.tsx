import useStore from '../store/useStore';
import { runPipeline } from '../api/client';

export default function Toolbar() {
  const nodes = useStore((s) => s.nodes);
  const edges = useStore((s) => s.edges);
  const isRunning = useStore((s) => s.isRunning);
  const setIsRunning = useStore((s) => s.setIsRunning);
  const setNodeResults = useStore((s) => s.setNodeResults);
  const clearPipeline = useStore((s) => s.clearPipeline);

  const handleRun = async () => {
    setIsRunning(true);
    try {
      const payload = {
        nodes: nodes.map((n) => ({
          id: n.id,
          type: n.data.nodeType,
          config: n.data.config || {},
        })),
        edges: edges.map((e) => ({
          source: e.source,
          target: e.target,
        })),
      };
      const results = await runPipeline(payload);
      setNodeResults(results);
    } catch (err: any) {
      console.error('Pipeline execution failed:', err);
      // If there's a response with detail, set it as an error for display
      const detail = err?.response?.data?.detail;
      if (detail) {
        setNodeResults({ _error: { error: String(detail) } });
      }
    } finally {
      setIsRunning(false);
    }
  };

  return (
    <div className="toolbar">
      <h1>Mirador</h1>
      <div className="toolbar-spacer" />
      <button
        className="run-btn"
        onClick={handleRun}
        disabled={isRunning || nodes.length === 0}
      >
        {isRunning && <span className="spinner" />}
        {isRunning ? 'Running...' : 'Run \u25B6'}
      </button>
      <button onClick={clearPipeline}>Clear</button>
    </div>
  );
}
