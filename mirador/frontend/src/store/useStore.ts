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

export type AppView = 'workflows' | 'dashboards' | 'workflow-editor' | 'dashboard-editor';

export interface NodeData {
  nodeType: string;
  label: string;
  category: string;
  config: Record<string, any>;
  [key: string]: unknown;
}

/* ---- Dashboard types ---- */

export interface DashboardDataSource {
  workflow_name: string;
  node_id: string;
  alias: string;
}

export interface WidgetLayout {
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface DashboardWidget {
  id: string;
  type: 'table' | 'bar_chart' | 'line_chart' | 'stat_card' | 'pie_chart';
  title: string;
  layout: WidgetLayout;
  data_source: string;
  config: Record<string, any>;
}

export interface Dashboard {
  name: string;
  data_sources: DashboardDataSource[];
  widgets: DashboardWidget[];
  grid_cols: number;
}

/* ---- Console messages ---- */

export interface ConsoleMessage {
  timestamp: number;
  level: 'info' | 'warn' | 'error';
  text: string;
}

/* ---- Combined store interface ---- */

export interface PipelineState {
  // Navigation
  currentView: AppView;
  currentProjectSlug: string | null;
  currentWorkflowName: string | null;
  currentDashboardName: string | null;

  setView: (view: AppView) => void;
  openWorkflow: (slug: string, name: string) => void;
  openDashboard: (slug: string, name: string) => void;

  // Pipeline editor
  nodes: Node<NodeData>[];
  edges: Edge[];
  selectedNodeId: string | null;
  nodeResults: Record<string, any>;
  isRunning: boolean;
  executingNodeId: string | null;
  isDirty: boolean;
  lastFailedNodeId: string | null;
  pipelineSessionId: string | null;

  // Console + bottom tab
  consoleMessages: ConsoleMessage[];
  bottomTab: 'preview' | 'console';

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
  editingNodeId: string | null;
  setEditingNodeId: (id: string | null) => void;
  updateNodeLabel: (nodeId: string, label: string) => void;
  updateNodeConfig: (nodeId: string, config: Record<string, any>) => void;
  setNodeResults: (results: Record<string, any>) => void;
  addNodeResult: (nodeId: string, result: any) => void;
  setIsRunning: (running: boolean) => void;
  setExecutingNodeId: (id: string | null) => void;
  setDirty: (dirty: boolean) => void;
  setLastFailedNodeId: (id: string | null) => void;
  setPipelineSessionId: (id: string | null) => void;
  addConsoleMessage: (level: ConsoleMessage['level'], text: string) => void;
  clearConsole: () => void;
  setBottomTab: (tab: 'preview' | 'console') => void;
  loadPipeline: (nodes: Node<NodeData>[], edges: Edge[]) => void;
  clearPipeline: () => void;

  // Dashboard editor
  dashboardDef: Dashboard | null;
  dashboardData: Record<string, { rows: any[]; columns: string[] }>;
  selectedWidgetId: string | null;

  setDashboardDef: (def: Dashboard | null) => void;
  setDashboardData: (data: Record<string, { rows: any[]; columns: string[] }>) => void;
  selectWidget: (id: string | null) => void;
  addWidget: (widget: DashboardWidget) => void;
  removeWidget: (id: string) => void;
  updateWidgetConfig: (id: string, config: Record<string, any>) => void;
  updateWidgetLayout: (id: string, layout: WidgetLayout) => void;
  addDataSource: (ds: DashboardDataSource) => void;
  removeDataSource: (alias: string) => void;
}

const useStore = create<PipelineState>((set, get) => ({
  // Navigation
  currentView: 'workflows',
  currentProjectSlug: null,
  currentWorkflowName: null,
  currentDashboardName: null,

  setView: (view) => set({ currentView: view }),

  openWorkflow: (slug, name) => {
    nodeIdCounter = 0;
    set({
      currentView: 'workflow-editor',
      currentProjectSlug: slug,
      currentWorkflowName: name,
      nodes: [],
      edges: [],
      selectedNodeId: null,
      nodeResults: {},
      isRunning: false,
      executingNodeId: null,
      consoleMessages: [],
      bottomTab: 'preview',
    });
  },

  openDashboard: (slug, name) => set({
    currentView: 'dashboard-editor',
    currentProjectSlug: slug,
    currentDashboardName: name,
  }),

  // Pipeline editor
  nodes: [],
  edges: [],
  selectedNodeId: null,
  nodeResults: {},
  isRunning: false,
  executingNodeId: null,
  isDirty: false,
  lastFailedNodeId: null,
  pipelineSessionId: null,
  consoleMessages: [],
  bottomTab: 'preview',

  onNodesChange: (changes) => {
    const state = get();
    const removedIds = changes
      .filter((c): c is NodeChange & { type: 'remove'; id: string } => c.type === 'remove')
      .map((c) => c.id);
    const updates: Partial<PipelineState> = {
      nodes: applyNodeChanges(changes, state.nodes) as Node<NodeData>[],
      isDirty: true,
    };
    if (state.selectedNodeId && removedIds.includes(state.selectedNodeId)) {
      updates.selectedNodeId = null;
    }
    set(updates);
  },

  onEdgesChange: (changes) => {
    set({ edges: applyEdgeChanges(changes, get().edges), isDirty: true });
  },

  onConnect: (connection) => {
    set({ edges: addEdge(connection, get().edges), isDirty: true });
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
    set({ nodes: [...get().nodes, newNode], isDirty: true });
  },

  selectNode: (id) => {
    set({ selectedNodeId: id });
  },

  editingNodeId: null,

  setEditingNodeId: (id) => set({ editingNodeId: id }),

  updateNodeLabel: (nodeId, label) => {
    set({
      nodes: get().nodes.map((n) =>
        n.id === nodeId ? { ...n, data: { ...n.data, label } } : n
      ),
      isDirty: true,
    });
  },

  updateNodeConfig: (nodeId, config) => {
    set({
      nodes: get().nodes.map((n) =>
        n.id === nodeId
          ? { ...n, data: { ...n.data, config: { ...n.data.config, ...config } } }
          : n
      ),
      isDirty: true,
    });
  },

  setNodeResults: (results) => {
    set({ nodeResults: results });
  },

  addNodeResult: (nodeId, result) => {
    set({ nodeResults: { ...get().nodeResults, [nodeId]: result } });
  },

  setIsRunning: (running) => {
    set({ isRunning: running });
  },

  setExecutingNodeId: (id) => {
    set({ executingNodeId: id });
  },

  setDirty: (dirty) => {
    set({ isDirty: dirty });
  },

  setLastFailedNodeId: (id) => {
    set({ lastFailedNodeId: id });
  },

  setPipelineSessionId: (id) => {
    set({ pipelineSessionId: id });
  },

  addConsoleMessage: (level, text) => {
    set({ consoleMessages: [...get().consoleMessages, { timestamp: Date.now(), level, text }] });
  },

  clearConsole: () => {
    set({ consoleMessages: [] });
  },

  setBottomTab: (tab) => {
    set({ bottomTab: tab });
  },

  loadPipeline: (nodes, edges) => {
    // Restore nodeIdCounter to avoid ID collisions with loaded nodes
    let maxId = 0;
    for (const n of nodes) {
      const m = n.id.match(/^node_(\d+)$/);
      if (m) maxId = Math.max(maxId, parseInt(m[1], 10));
    }
    nodeIdCounter = maxId;
    set({
      nodes: nodes as Node<NodeData>[],
      edges,
      selectedNodeId: null,
      nodeResults: {},
      isRunning: false,
      executingNodeId: null,
      isDirty: false,
    });
  },

  clearPipeline: () => {
    nodeIdCounter = 0;
    set({
      nodes: [],
      edges: [],
      selectedNodeId: null,
      nodeResults: {},
      isRunning: false,
      executingNodeId: null,
      isDirty: false,
      consoleMessages: [],
    });
  },

  // Dashboard editor
  dashboardDef: null,
  dashboardData: {},
  selectedWidgetId: null,

  setDashboardDef: (def) => set({ dashboardDef: def }),

  setDashboardData: (data) => set({ dashboardData: data }),

  selectWidget: (id) => set({ selectedWidgetId: id }),

  addWidget: (widget) => {
    const def = get().dashboardDef;
    if (!def) return;
    set({ dashboardDef: { ...def, widgets: [...def.widgets, widget] } });
  },

  removeWidget: (id) => {
    const def = get().dashboardDef;
    if (!def) return;
    set({
      dashboardDef: { ...def, widgets: def.widgets.filter((w) => w.id !== id) },
      selectedWidgetId: get().selectedWidgetId === id ? null : get().selectedWidgetId,
    });
  },

  updateWidgetConfig: (id, config) => {
    const def = get().dashboardDef;
    if (!def) return;
    set({
      dashboardDef: {
        ...def,
        widgets: def.widgets.map((w) =>
          w.id === id ? { ...w, config: { ...w.config, ...config } } : w
        ),
      },
    });
  },

  updateWidgetLayout: (id, layout) => {
    const def = get().dashboardDef;
    if (!def) return;
    set({
      dashboardDef: {
        ...def,
        widgets: def.widgets.map((w) =>
          w.id === id ? { ...w, layout } : w
        ),
      },
    });
  },

  addDataSource: (ds) => {
    const def = get().dashboardDef;
    if (!def) return;
    set({ dashboardDef: { ...def, data_sources: [...def.data_sources, ds] } });
  },

  removeDataSource: (alias) => {
    const def = get().dashboardDef;
    if (!def) return;
    set({
      dashboardDef: {
        ...def,
        data_sources: def.data_sources.filter((d) => d.alias !== alias),
      },
    });
  },
}));

export default useStore;
