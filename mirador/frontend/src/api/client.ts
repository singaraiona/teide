import axios from 'axios';

export const api = axios.create({ baseURL: 'http://localhost:8000/api' });

export interface NodeTypeMeta {
  id: string;
  label: string;
  category: string;
  description: string;
  inputs: { name: string; description: string }[];
  outputs: { name: string; description: string }[];
  config_schema: Record<string, any>;
}

export interface PipelinePayload {
  nodes: { id: string; type: string; config: Record<string, any> }[];
  edges: { source: string; target: string }[];
  session_id?: string;
  start_from?: string;
}

export interface FileEntry {
  name: string;
  type: 'dir' | 'file';
  path: string;
  size?: number;
}

export interface BrowseResult {
  path: string;
  parent: string | null;
  error?: string;
  entries: FileEntry[];
}

export async function fetchNodeTypes(): Promise<NodeTypeMeta[]> {
  return (await api.get('/nodes')).data;
}

export async function runPipeline(
  pipeline: PipelinePayload
): Promise<Record<string, any>> {
  return (await api.post('/pipelines/run', pipeline)).data;
}

export interface SSEEvent {
  type: 'node_start' | 'node_done' | 'node_error' | 'complete' | 'error';
  node_id?: string;
  results?: Record<string, any>;
  error?: string;
  rows?: number;
  columns?: string[];
  [key: string]: any;
}

export async function runPipelineStream(
  pipeline: PipelinePayload,
  onEvent: (event: SSEEvent) => void
): Promise<void> {
  const response = await fetch('http://localhost:8000/api/pipelines/run-stream', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(pipeline),
  });

  const reader = response.body?.getReader();
  if (!reader) throw new Error('No response body');

  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() ?? '';

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        try {
          const event: SSEEvent = JSON.parse(line.slice(6));
          onEvent(event);
        } catch {
          // skip malformed lines
        }
      }
    }
  }
}

export async function browseFiles(path: string = '~'): Promise<BrowseResult> {
  return (await api.get('/files/browse', { params: { path } })).data;
}

/* ---- Pipeline CRUD ---- */

export async function fetchPipeline(slug: string, name: string): Promise<{ nodes: any[]; edges: any[] }> {
  return (await api.get(`/projects/${slug}/pipelines/${name}`)).data;
}

export async function savePipelineState(slug: string, name: string, nodes: any[], edges: any[]): Promise<void> {
  await api.put(`/projects/${slug}/pipelines/${name}`, { nodes, edges });
}

/* ---- Dashboard API ---- */

export async function fetchDashboards(slug: string): Promise<string[]> {
  return (await api.get(`/projects/${slug}/dashboards`)).data;
}

export async function fetchDashboard(slug: string, name: string): Promise<any> {
  return (await api.get(`/projects/${slug}/dashboards/${name}`)).data;
}

export async function saveDashboard(slug: string, name: string, data: any): Promise<void> {
  await api.put(`/projects/${slug}/dashboards/${name}`, data);
}

export async function deleteDashboard(slug: string, name: string): Promise<void> {
  await api.delete(`/projects/${slug}/dashboards/${name}`);
}

export async function refreshDashboardData(slug: string, name: string): Promise<Record<string, { rows: any[]; columns: string[] }>> {
  return (await api.post(`/projects/${slug}/dashboards/${name}/refresh`)).data;
}
