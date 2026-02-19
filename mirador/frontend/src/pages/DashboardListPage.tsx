import { useCallback, useEffect, useState } from 'react';
import useStore from '../store/useStore';
import { api, fetchDashboards, deleteDashboard, saveDashboard } from '../api/client';
import ConfirmDialog from '../components/ConfirmDialog';

interface ProjectInfo {
  name: string;
  slug: string;
}

export default function DashboardListPage() {
  const [projects, setProjects] = useState<ProjectInfo[]>([]);
  const [dashboards, setDashboards] = useState<Record<string, string[]>>({});
  const [showInput, setShowInput] = useState<string | null>(null);
  const [newDashName, setNewDashName] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);
  const [confirmAction, setConfirmAction] = useState<{ message: string; action: () => void } | null>(null);
  const openDashboard = useStore((s) => s.openDashboard);

  const load = useCallback(async () => {
    try {
      setError(null);
      const res = await api.get('/projects');
      const projs: ProjectInfo[] = res.data;
      setProjects(projs);
      const dashMap: Record<string, string[]> = {};
      await Promise.all(
        projs.map(async (p) => {
          try {
            const list = await fetchDashboards(p.slug);
            dashMap[p.slug] = list;
          } catch {
            dashMap[p.slug] = [];
          }
        })
      );
      setDashboards(dashMap);
    } catch (err: any) {
      setError(err?.message ?? 'Failed to load projects');
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const startCreate = (slug: string) => {
    setShowInput(slug);
    setNewDashName((p) => ({ ...p, [slug]: '' }));
  };

  const submitCreate = async (slug: string) => {
    const raw = (newDashName[slug] ?? '').trim();
    if (!raw) return;
    const dashName = raw.toLowerCase().replace(/\s+/g, '_');
    try {
      await saveDashboard(slug, dashName, {
        name: dashName,
        data_sources: [],
        widgets: [],
        grid_cols: 12,
      });
      setShowInput(null);
      openDashboard(slug, dashName);
    } catch (err: any) {
      setError(err?.response?.data?.detail ?? 'Failed to create dashboard');
    }
  };

  const removeDashboard = (slug: string, dashName: string) => {
    setConfirmAction({
      message: `Delete dashboard "${dashName}"? This cannot be undone.`,
      action: async () => {
        try {
          await deleteDashboard(slug, dashName);
          load();
        } catch (err: any) {
          setError(err?.message ?? 'Failed to delete dashboard');
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
        <h2>Dashboards</h2>
      </div>

      <div className="list-grid">
        {projects.map((p) => (
          <div key={p.slug} className="list-card">
            <div className="list-card-header">
              <h3>{p.name}</h3>
            </div>
            <div className="list-card-body">
              {(dashboards[p.slug] ?? []).length === 0 && showInput !== p.slug && (
                <div className="list-card-empty">No dashboards yet</div>
              )}
              {(dashboards[p.slug] ?? []).length > 0 && (
                <ul className="list-card-items">
                  {(dashboards[p.slug] ?? []).map((name) => (
                    <li key={name}>
                      <button className="list-item-link" onClick={() => openDashboard(p.slug, name)}>
                        {name}
                      </button>
                      <button className="list-item-delete" onClick={() => removeDashboard(p.slug, name)}>&times;</button>
                    </li>
                  ))}
                </ul>
              )}
              {showInput === p.slug && (
                <div className="inline-create-row">
                  <input
                    type="text"
                    value={newDashName[p.slug] ?? ''}
                    onChange={(e) => setNewDashName((prev) => ({ ...prev, [p.slug]: e.target.value }))}
                    placeholder="Dashboard name..."
                    autoFocus
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') submitCreate(p.slug);
                      if (e.key === 'Escape') setShowInput(null);
                    }}
                  />
                  <button className="inline-create-ok" onClick={() => submitCreate(p.slug)}>Create</button>
                  <button className="inline-create-cancel" onClick={() => setShowInput(null)}>&times;</button>
                </div>
              )}
            </div>
            <div className="list-card-footer">
              <button className="list-card-action" onClick={() => startCreate(p.slug)}>
                + New Dashboard
              </button>
            </div>
          </div>
        ))}
        {projects.length === 0 && (
          <div className="list-empty">Create a project first in the Pipelines tab.</div>
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
