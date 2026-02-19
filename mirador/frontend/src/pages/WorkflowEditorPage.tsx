import { useCallback, useEffect, useRef } from 'react';
import { ReactFlowProvider } from '@xyflow/react';
import Toolbar from '../components/Toolbar';
import Sidebar from '../components/Sidebar';
import Canvas from '../components/Canvas';
import PreviewPanel from '../components/PreviewPanel';
import Inspector from '../components/Inspector';
import useStore from '../store/useStore';
import { fetchPipeline, savePipelineState } from '../api/client';

const AUTO_SAVE_INTERVAL = 30_000; // 30 seconds

function saveCurrent() {
  const s = useStore.getState();
  if (!s.currentProjectSlug || !s.currentWorkflowName || s.nodes.length === 0) return;
  const saveNodes = s.nodes.map((n) => ({
    id: n.id, type: n.type, position: n.position, data: n.data,
  }));
  const saveEdges = s.edges.map((e) => ({
    id: e.id, source: e.source, target: e.target,
    sourceHandle: e.sourceHandle, targetHandle: e.targetHandle,
  }));
  savePipelineState(s.currentProjectSlug, s.currentWorkflowName, saveNodes, saveEdges)
    .then(() => useStore.getState().setDirty(false))
    .catch(() => {}); // silent — will retry on next interval
}

export default function WorkflowEditorPage() {
  const slug = useStore((s) => s.currentProjectSlug);
  const name = useStore((s) => s.currentWorkflowName);
  const loaded = useRef(false);

  // Load pipeline on mount
  useEffect(() => {
    if (!slug || !name || loaded.current) return;
    loaded.current = true;

    fetchPipeline(slug, name)
      .then((data) => {
        const nodes = data.nodes ?? [];
        const edges = data.edges ?? [];
        if (nodes.length > 0 || edges.length > 0) {
          useStore.getState().loadPipeline(nodes, edges);
        }
      })
      .catch(() => {});
  }, [slug, name]);

  // Auto-save on interval when dirty
  useEffect(() => {
    const timer = setInterval(() => {
      if (useStore.getState().isDirty) saveCurrent();
    }, AUTO_SAVE_INTERVAL);
    return () => clearInterval(timer);
  }, []);

  // Save on unmount (navigation away)
  useEffect(() => {
    return () => saveCurrent();
  }, []);

  // Keyboard shortcuts
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    // Escape — blur input first, then deselect node
    if (e.key === 'Escape') {
      const tag = (e.target as HTMLElement).tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') {
        (e.target as HTMLElement).blur();
      } else {
        useStore.getState().selectNode(null);
      }
    }
    // Ctrl+S / Cmd+S — save
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
      e.preventDefault();
      saveCurrent();
    }
  }, []);

  useEffect(() => {
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  return (
    <ReactFlowProvider>
      <div className="editor-layout">
        <Toolbar />
        <div className="app-main">
          <Sidebar />
          <Canvas />
        </div>
        <PreviewPanel />
      </div>
      <Inspector />
    </ReactFlowProvider>
  );
}
