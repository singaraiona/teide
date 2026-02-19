import { useReactFlow } from '@xyflow/react';
import useStore from '../store/useStore';
import { runPipelineStream, savePipelineState, type SSEEvent } from '../api/client';

export default function Toolbar() {
  const nodeCount = useStore((s) => s.nodes.length);
  const isRunning = useStore((s) => s.isRunning);
  const isDirty = useStore((s) => s.isDirty);
  const lastFailedNodeId = useStore((s) => s.lastFailedNodeId);
  const setIsRunning = useStore((s) => s.setIsRunning);
  const setNodeResults = useStore((s) => s.setNodeResults);
  const setExecutingNodeId = useStore((s) => s.setExecutingNodeId);
  const clearPipeline = useStore((s) => s.clearPipeline);
  const setView = useStore((s) => s.setView);
  const pipelineName = useStore((s) => s.currentWorkflowName);
  const { fitView } = useReactFlow();

  const handleSave = () => {
    const s = useStore.getState();
    if (!s.currentProjectSlug || !s.currentWorkflowName) return;
    const saveNodes = s.nodes.map((n) => ({
      id: n.id, type: n.type, position: n.position, data: n.data,
    }));
    const saveEdges = s.edges.map((e) => ({
      id: e.id, source: e.source, target: e.target,
      sourceHandle: e.sourceHandle, targetHandle: e.targetHandle,
    }));
    savePipelineState(s.currentProjectSlug, s.currentWorkflowName, saveNodes, saveEdges)
      .then(() => useStore.getState().setDirty(false))
      .catch(() => {});
  };

  const executePipeline = async (resumeFrom?: string) => {
    setIsRunning(true);
    setExecutingNodeId(null);

    const store = useStore.getState();
    const log = store.addConsoleMessage;
    const findLabel = (id?: string) => {
      if (!id) return '?';
      const n = useStore.getState().nodes.find((nd) => nd.id === id);
      return n?.data.label ?? id;
    };

    // Generate or reuse session ID
    let sessionId = store.pipelineSessionId;
    if (!resumeFrom || !sessionId) {
      sessionId = crypto.randomUUID();
      store.setPipelineSessionId(sessionId);
    }

    if (!resumeFrom) {
      // Fresh run: clear results and failed state
      setNodeResults({});
      store.setLastFailedNodeId(null);
    } else {
      // Resume: clear only the failed node's error result
      store.setLastFailedNodeId(null);
    }

    const { nodes, edges } = store;
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
      session_id: sessionId,
      start_from: resumeFrom,
    };

    log('info', resumeFrom
      ? `Resuming from: ${findLabel(resumeFrom)}`
      : `Pipeline started (${nodes.length} nodes)`);
    store.setBottomTab('console');

    try {
      await runPipelineStream(payload, (event: SSEEvent) => {
        switch (event.type) {
          case 'node_start':
            setExecutingNodeId(event.node_id ?? null);
            log('info', `Executing: ${findLabel(event.node_id)}`);
            break;
          case 'node_done':
            setExecutingNodeId(null);
            log('info', `Completed: ${findLabel(event.node_id)}`);
            break;
          case 'node_error':
            setExecutingNodeId(null);
            useStore.getState().setLastFailedNodeId(event.node_id ?? null);
            log('error', `Error in ${findLabel(event.node_id)}: ${event.error ?? 'Unknown'}`);
            if (event.node_id) {
              useStore.getState().addNodeResult(event.node_id, {
                error: event.error ?? 'Node execution failed',
              });
            }
            break;
          case 'complete':
            log('info', 'Pipeline completed successfully');
            if (event.results) {
              setNodeResults(event.results);
            }
            break;
          case 'error':
            log('error', `Pipeline error: ${event.error ?? 'Unknown'}`);
            setNodeResults({ _error: { error: event.error ?? 'Unknown error' } });
            break;
        }
      });
    } catch (err: any) {
      console.error('Pipeline execution failed:', err);
      log('error', `Execution failed: ${err?.message ?? String(err)}`);
      setNodeResults({ _error: { error: String(err?.message ?? err) } });
    } finally {
      setIsRunning(false);
      setExecutingNodeId(null);
    }
  };

  const handleRun = () => executePipeline();
  const handleResume = () => {
    if (lastFailedNodeId) executePipeline(lastFailedNodeId);
  };

  return (
    <div className="toolbar">
      {/* Left group: navigation + identity */}
      <button className="back-btn" onClick={() => setView('workflows')} title="Back to pipelines">
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M9 2L4 7l5 5"/>
        </svg>
      </button>
      <span className="toolbar-pipeline-name">{pipelineName ?? 'Untitled'}</span>
      {isDirty && <span className="save-indicator" title="Unsaved changes"><span className="dirty-dot" /></span>}
      <button
        className="toolbar-icon-btn"
        onClick={handleSave}
        disabled={!isDirty}
        title="Save (Ctrl+S)"
      >
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round">
          <path d="M11 13H3a1 1 0 01-1-1V2a1 1 0 011-1h6l3 3v8a1 1 0 01-1 1z"/>
          <path d="M9 13V8H5v5M5 1v3h3"/>
        </svg>
      </button>

      <div className="toolbar-spacer" />

      {/* Center info */}
      {nodeCount > 0 && (
        <span className="toolbar-node-count">{nodeCount} node{nodeCount !== 1 ? 's' : ''}</span>
      )}

      <span className="toolbar-divider" />

      {/* Right group: view + actions */}
      <button
        className="toolbar-icon-btn"
        onClick={() => fitView({ padding: 0.2, duration: 300 })}
        title="Fit to view"
      >
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round">
          <path d="M1 5V2a1 1 0 011-1h3M9 1h3a1 1 0 011 1v3M13 9v3a1 1 0 01-1 1h-3M5 13H2a1 1 0 01-1-1v-3"/>
        </svg>
      </button>

      <span className="toolbar-divider" />

      <button
        className="run-btn"
        onClick={handleRun}
        disabled={isRunning || nodeCount === 0}
      >
        {isRunning && <span className="spinner" />}
        {isRunning ? 'Running...' : (
          <>
            <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor" style={{ marginRight: 5, verticalAlign: -1 }}>
              <path d="M2 1l9 5-9 5V1z"/>
            </svg>
            Run
          </>
        )}
      </button>

      {lastFailedNodeId && !isRunning && (
        <button className="resume-btn" onClick={handleResume} title="Resume from failed node">
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 4, verticalAlign: -1 }}>
            <path d="M1 6a5 5 0 019-3M11 1v4H7"/>
          </svg>
          Resume
        </button>
      )}

      <button className="clear-btn" onClick={clearPipeline}>
        <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" style={{ marginRight: 4, verticalAlign: -1 }}>
          <path d="M2 2l8 8M10 2l-8 8"/>
        </svg>
        Clear
      </button>
    </div>
  );
}
