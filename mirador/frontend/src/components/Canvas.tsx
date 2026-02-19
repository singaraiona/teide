import { useCallback, useRef, type DragEvent, type MouseEvent } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MarkerType,
  type Node,
  type Edge,
  type ReactFlowInstance,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import useStore, { type NodeData } from '../store/useStore';
import PipelineNode from './PipelineNode';

const nodeTypes = { pipeline: PipelineNode };

const defaultEdgeOptions = {
  type: 'smoothstep',
  style: { stroke: '#b0c4cf', strokeWidth: 1 },
  markerEnd: { type: MarkerType.ArrowClosed, width: 8, height: 8, color: '#b0c4cf' },
};

export default function Canvas() {
  const nodes = useStore((s) => s.nodes);
  const edges = useStore((s) => s.edges);
  const onNodesChange = useStore((s) => s.onNodesChange);
  const onEdgesChange = useStore((s) => s.onEdgesChange);
  const onConnect = useStore((s) => s.onConnect);
  const addNode = useStore((s) => s.addNode);
  const selectNode = useStore((s) => s.selectNode);
  const setEditingNodeId = useStore((s) => s.setEditingNodeId);

  const reactFlowInstance = useRef<ReactFlowInstance<Node<NodeData>, Edge> | null>(null);
  const clickTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const onDragOver = useCallback((event: DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: DragEvent) => {
      event.preventDefault();
      const raw = event.dataTransfer.getData('application/reactflow');
      if (!raw) return;

      const { nodeType, label, category } = JSON.parse(raw);
      if (!nodeType) return;

      const bounds = (event.target as HTMLElement)
        .closest('.react-flow')
        ?.getBoundingClientRect();
      if (!bounds) return;

      const position = reactFlowInstance.current
        ? reactFlowInstance.current.screenToFlowPosition({
            x: event.clientX,
            y: event.clientY,
          })
        : { x: event.clientX - bounds.left, y: event.clientY - bounds.top };

      addNode(nodeType, label, category, position);
    },
    [addNode]
  );

  const onNodeClick = useCallback(
    (_: MouseEvent, node: any) => {
      // Delay selection to allow double-click to cancel it
      if (clickTimer.current) {
        clearTimeout(clickTimer.current);
        clickTimer.current = null;
        return;
      }
      clickTimer.current = setTimeout(() => {
        clickTimer.current = null;
        selectNode(node.id);
      }, 250);
    },
    [selectNode]
  );

  const onNodeDoubleClick = useCallback(
    (_: MouseEvent, node: any) => {
      // Cancel the pending single-click selection
      if (clickTimer.current) {
        clearTimeout(clickTimer.current);
        clickTimer.current = null;
      }
      setEditingNodeId(node.id);
    },
    [setEditingNodeId]
  );

  const onPaneClick = useCallback(() => {
    selectNode(null);
    setEditingNodeId(null);
  }, [selectNode, setEditingNodeId]);

  return (
    <div className="canvas-container">
      {nodes.length === 0 && (
        <div className="canvas-empty">
          <div className="canvas-empty-content">
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <rect x="3" y="3" width="7" height="7" rx="1" />
              <rect x="14" y="3" width="7" height="7" rx="1" />
              <rect x="3" y="14" width="7" height="7" rx="1" />
              <path d="M17.5 14v7M14 17.5h7" />
            </svg>
            <p>Drag nodes from the palette to build your pipeline</p>
            <span>or click a node type to add it</span>
          </div>
        </div>
      )}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onNodeDoubleClick={onNodeDoubleClick}
        onPaneClick={onPaneClick}
        onDragOver={onDragOver}
        onDrop={onDrop}
        onInit={(instance) => {
          reactFlowInstance.current = instance;
        }}
        nodeTypes={nodeTypes}
        defaultEdgeOptions={defaultEdgeOptions}
        fitView
        deleteKeyCode="Delete"
      >
        <Background color="#dce8ee" gap={24} size={1.5} />
        <Controls />
      </ReactFlow>
    </div>
  );
}
