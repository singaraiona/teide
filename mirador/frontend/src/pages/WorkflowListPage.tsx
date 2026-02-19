import { useCallback, useEffect, useState } from 'react';
import useStore from '../store/useStore';
import { api } from '../api/client';
import ConfirmDialog from '../components/ConfirmDialog';

interface ProjectInfo {
  name: string;
  slug: string;
  created_at: number;
}

export default function WorkflowListPage() {
  const [projects, setProjects] = useState<ProjectInfo[]>([]);
  const [pipelines, setPipelines] = useState<Record<string, string[]>>({});
  const [newName, setNewName] = useState('');
  const [newPipeName, setNewPipeName] = useState<Record<string, string>>({});
  const [showPipeInput, setShowPipeInput] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [confirmAction, setConfirmAction] = useState<{ message: string; action: () => void } | null>(null);
  const openWorkflow = useStore((s) => s.openWorkflow);

  const loadProjects = useCallback(async () => {
    try {
      setError(null);
      const res = await api.get('/projects');
      const projs: ProjectInfo[] = res.data;
      setProjects(projs);
      const pipeMap: Record<string, string[]> = {};
      await Promise.all(
        projs.map(async (p) => {
          try {
            const pRes = await api.get(`/projects/${p.slug}/pipelines`);
            pipeMap[p.slug] = pRes.data;
          } catch {
            pipeMap[p.slug] = [];
          }
        })
      );
      setPipelines(pipeMap);
    } catch (err: any) {
      setError(err?.message ?? 'Failed to load projects');
    }
  }, []);

  useEffect(() => { loadProjects(); }, [loadProjects]);

  const createProject = async () => {
    const name = newName.trim();
    if (!name) return;
    try {
      await api.post('/projects', { name });
      setNewName('');
      loadProjects();
    } catch (err: any) {
      setError(err?.response?.data?.detail ?? 'Failed to create project');
    }
  };

  const deleteProject = (slug: string, name: string) => {
    setConfirmAction({
      message: `Delete project "${name}" and all its pipelines? This cannot be undone.`,
      action: async () => {
        try {
          await api.delete(`/projects/${slug}`);
          loadProjects();
        } catch (err: any) {
          setError(err?.message ?? 'Failed to delete project');
        }
        setConfirmAction(null);
      },
    });
  };

  const startCreatePipeline = (slug: string) => {
    setShowPipeInput(slug);
    setNewPipeName((p) => ({ ...p, [slug]: '' }));
  };

  const submitCreatePipeline = async (slug: string) => {
    const raw = (newPipeName[slug] ?? '').trim();
    if (!raw) return;
    const pipeName = raw.toLowerCase().replace(/\s+/g, '_');
    try {
      await api.put(`/projects/${slug}/pipelines/${pipeName}`, { nodes: [], edges: [] });
      setShowPipeInput(null);
      openWorkflow(slug, pipeName);
    } catch (err: any) {
      setError(err?.response?.data?.detail ?? 'Failed to create pipeline');
    }
  };

  const deletePipeline = (slug: string, pipeName: string) => {
    setConfirmAction({
      message: `Delete pipeline "${pipeName}"? This cannot be undone.`,
      action: async () => {
        try {
          await api.delete(`/projects/${slug}/pipelines/${pipeName}`);
          loadProjects();
        } catch (err: any) {
          setError(err?.message ?? 'Failed to delete pipeline');
        }
        setConfirmAction(null);
      },
    });
  };

  return (
    <div className="list-page">
      {error && (
        <div className="list-error">
          <span>{error}</span>
          <button className="list-error-dismiss" onClick={() => setError(null)}>&times;</button>
        </div>
      )}
      <div className="list-header">
        <h2>Pipelines</h2>
        <div className="list-create">
          <input
            type="text"
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            placeholder="New project name..."
            onKeyDown={(e) => e.key === 'Enter' && createProject()}
          />
          <button onClick={createProject}>+ Create Project</button>
        </div>
      </div>

      <div className="list-grid">
        {projects.map((p) => (
          <div key={p.slug} className="list-card">
            <div className="list-card-header">
              <h3>{p.name}</h3>
              <button className="list-card-delete" onClick={() => deleteProject(p.slug, p.name)} title="Delete project">&times;</button>
            </div>
            <div className="list-card-body">
              {(pipelines[p.slug] ?? []).length === 0 && showPipeInput !== p.slug && (
                <div className="list-card-empty">No pipelines yet</div>
              )}
              {(pipelines[p.slug] ?? []).length > 0 && (
                <ul className="list-card-items">
                  {(pipelines[p.slug] ?? []).map((name) => (
                    <li key={name}>
                      <button className="list-item-link" onClick={() => openWorkflow(p.slug, name)}>
                        {name}
                      </button>
                      <button className="list-item-delete" onClick={() => deletePipeline(p.slug, name)}>&times;</button>
                    </li>
                  ))}
                </ul>
              )}
              {showPipeInput === p.slug && (
                <div className="inline-create-row">
                  <input
                    type="text"
                    value={newPipeName[p.slug] ?? ''}
                    onChange={(e) => setNewPipeName((prev) => ({ ...prev, [p.slug]: e.target.value }))}
                    placeholder="Pipeline name..."
                    autoFocus
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') submitCreatePipeline(p.slug);
                      if (e.key === 'Escape') setShowPipeInput(null);
                    }}
                  />
                  <button className="inline-create-ok" onClick={() => submitCreatePipeline(p.slug)}>Create</button>
                  <button className="inline-create-cancel" onClick={() => setShowPipeInput(null)}>&times;</button>
                </div>
              )}
            </div>
            <div className="list-card-footer">
              <button className="list-card-action" onClick={() => startCreatePipeline(p.slug)}>
                + New Pipeline
              </button>
            </div>
          </div>
        ))}
        {projects.length === 0 && (
          <div className="list-empty">No projects yet. Create one above to get started.</div>
        )}
      </div>

      {confirmAction && (
        <ConfirmDialog
          message={confirmAction.message}
          onConfirm={confirmAction.action}
          onCancel={() => setConfirmAction(null)}
        />
      )}
    </div>
  );
}
