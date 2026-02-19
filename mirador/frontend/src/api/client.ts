import axios from 'axios';

const api = axios.create({ baseURL: 'http://localhost:8000/api' });

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
}

export async function fetchNodeTypes(): Promise<NodeTypeMeta[]> {
  return (await api.get('/nodes')).data;
}

export async function runPipeline(
  pipeline: PipelinePayload
): Promise<Record<string, any>> {
  return (await api.post('/pipelines/run', pipeline)).data;
}
