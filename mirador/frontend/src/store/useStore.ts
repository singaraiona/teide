import { create } from 'zustand';
import {
  type Node,
  type Edge,
  type NodeChange,
  type EdgeChange,
  type Connection,
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
} from '@xyflow/react';

let nodeIdCounter = 0;

export interface NodeData {
  nodeType: string;
  label: string;
  category: string;
  config: Record<string, any>;
  [key: string]: unknown;
}

export interface PipelineState {
  nodes: Node<NodeData>[];
  edges: Edge[];
  selectedNodeId: string | null;
  nodeResults: Record<string, any>;
  isRunning: boolean;

  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;
  addNode: (
    type: string,
    label: string,
    category: string,
    position: { x: number; y: number }
  ) => void;
  selectNode: (id: string | null) => void;
  updateNodeConfig: (nodeId: string, config: Record<string, any>) => void;
  setNodeResults: (results: Record<string, any>) => void;
  setIsRunning: (running: boolean) => void;
  clearPipeline: () => void;
}

const useStore = create<PipelineState>((set, get) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,
  nodeResults: {},
  isRunning: false,

  onNodesChange: (changes) => {
    set({ nodes: applyNodeChanges(changes, get().nodes) as Node<NodeData>[] });
  },

  onEdgesChange: (changes) => {
    set({ edges: applyEdgeChanges(changes, get().edges) });
  },

  onConnect: (connection) => {
    set({ edges: addEdge(connection, get().edges) });
  },

  addNode: (type, label, category, position) => {
    const id = `node_${++nodeIdCounter}`;
    const newNode: Node<NodeData> = {
      id,
      type: 'pipeline',
      position,
      data: {
        nodeType: type,
        label,
        category,
        config: {},
      },
    };
    set({ nodes: [...get().nodes, newNode] });
  },

  selectNode: (id) => {
    set({ selectedNodeId: id });
  },

  updateNodeConfig: (nodeId, config) => {
    set({
      nodes: get().nodes.map((n) =>
        n.id === nodeId
          ? { ...n, data: { ...n.data, config: { ...n.data.config, ...config } } }
          : n
      ),
    });
  },

  setNodeResults: (results) => {
    set({ nodeResults: results });
  },

  setIsRunning: (running) => {
    set({ isRunning: running });
  },

  clearPipeline: () => {
    nodeIdCounter = 0;
    set({
      nodes: [],
      edges: [],
      selectedNodeId: null,
      nodeResults: {},
      isRunning: false,
    });
  },
}));

export default useStore;
