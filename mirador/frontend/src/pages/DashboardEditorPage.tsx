import { useCallback, useEffect, useState } from 'react';
import useStore, { type DashboardWidget } from '../store/useStore';
import { fetchDashboard, saveDashboard as apiSaveDashboard, refreshDashboardData } from '../api/client';
import WidgetCard from '../components/dashboard/WidgetCard';
import WidgetConfigPanel from '../components/dashboard/WidgetConfigPanel';

let widgetIdCounter = 0;

type WidgetType = DashboardWidget['type'];

const widgetTypes: { type: WidgetType; label: string }[] = [
  { type: 'table', label: 'Table' },
  { type: 'bar_chart', label: 'Bar Chart' },
  { type: 'line_chart', label: 'Line Chart' },
  { type: 'pie_chart', label: 'Pie Chart' },
  { type: 'stat_card', label: 'Stat Card' },
];

export default function DashboardEditorPage() {
  const slug = useStore((s) => s.currentProjectSlug);
  const dashName = useStore((s) => s.currentDashboardName);
  const setView = useStore((s) => s.setView);
  const dashboardDef = useStore((s) => s.dashboardDef);
  const setDashboardDef = useStore((s) => s.setDashboardDef);
  const dashboardData = useStore((s) => s.dashboardData);
  const setDashboardData = useStore((s) => s.setDashboardData);
  const addWidget = useStore((s) => s.addWidget);
  const removeWidget = useStore((s) => s.removeWidget);
  const selectedWidgetId = useStore((s) => s.selectedWidgetId);
  const selectWidget = useStore((s) => s.selectWidget);
  const addDataSource = useStore((s) => s.addDataSource);
  const removeDataSource = useStore((s) => s.removeDataSource);

  const [showAddDs, setShowAddDs] = useState(false);
  const [dsForm, setDsForm] = useState({ workflow_name: '', node_id: '', alias: '' });
  const [showAddWidget, setShowAddWidget] = useState(false);

  // Load dashboard on mount
  useEffect(() => {
    if (!slug || !dashName) return;
    (async () => {
      const data = await fetchDashboard(slug, dashName);
      if (data) {
        setDashboardDef(data);
      } else {
        setDashboardDef({ name: dashName, data_sources: [], widgets: [], grid_cols: 12 });
      }
    })();
    return () => { setDashboardDef(null); };
  }, [slug, dashName, setDashboardDef]);

  const handleSave = useCallback(async () => {
    if (!slug || !dashName || !dashboardDef) return;
    await apiSaveDashboard(slug, dashName, dashboardDef);
  }, [slug, dashName, dashboardDef]);

  const handleRefresh = useCallback(async () => {
    if (!slug || !dashName) return;
    const data = await refreshDashboardData(slug, dashName);
    setDashboardData(data);
  }, [slug, dashName, setDashboardData]);

  const handleAddWidget = (type: WidgetType) => {
    const id = `widget_${++widgetIdCounter}`;
    const widget: DashboardWidget = {
      id,
      type,
      title: type.replace('_', ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
      layout: { x: 0, y: 0, w: 4, h: 3 },
      data_source: dashboardDef?.data_sources[0]?.alias ?? '',
      config: {},
    };
    addWidget(widget);
    setShowAddWidget(false);
    selectWidget(id);
  };

  const handleAddDs = () => {
    if (!dsForm.alias.trim()) return;
    addDataSource({
      workflow_name: dsForm.workflow_name,
      node_id: dsForm.node_id,
      alias: dsForm.alias.trim(),
    });
    setDsForm({ workflow_name: '', node_id: '', alias: '' });
    setShowAddDs(false);
  };

  if (!dashboardDef) return null;

  const selectedWidget = dashboardDef.widgets.find((w) => w.id === selectedWidgetId);

  return (
    <div className="dashboard-editor">
      <div className="dashboard-toolbar">
        <button className="back-btn" onClick={() => setView('dashboards')} title="Back to dashboards">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M9 2L4 7l5 5"/>
          </svg>
        </button>
        <span className="toolbar-workflow-name">{dashName}</span>
        <div className="toolbar-spacer" />
        <button onClick={() => setShowAddWidget(!showAddWidget)}>+ Add Widget</button>
        <button className="primary-btn" onClick={handleSave}>Save</button>
        <button onClick={handleRefresh}>Refresh Data</button>
      </div>

      <div className="dashboard-main">
        {/* Left: Data Sources */}
        <div className="dashboard-side-panel">
          <h4>Data Sources</h4>
          <ul className="ds-list">
            {dashboardDef.data_sources.map((ds) => (
              <li key={ds.alias}>
                <span>{ds.alias}</span>
                <button onClick={() => removeDataSource(ds.alias)}>&times;</button>
              </li>
            ))}
          </ul>
          <button className="list-card-action" onClick={() => setShowAddDs(!showAddDs)}>+ Add Source</button>
          {showAddDs && (
            <div className="add-ds-form">
              <input
                placeholder="Workflow name"
                value={dsForm.workflow_name}
                onChange={(e) => setDsForm({ ...dsForm, workflow_name: e.target.value })}
              />
              <input
                placeholder="Node ID"
                value={dsForm.node_id}
                onChange={(e) => setDsForm({ ...dsForm, node_id: e.target.value })}
              />
              <input
                placeholder="Alias"
                value={dsForm.alias}
                onChange={(e) => setDsForm({ ...dsForm, alias: e.target.value })}
              />
              <button onClick={handleAddDs}>Add</button>
            </div>
          )}

          {showAddWidget && (
            <>
              <h4 style={{ marginTop: 16 }}>Add Widget</h4>
              <div className="widget-type-grid">
                {widgetTypes.map((wt) => (
                  <button key={wt.type} className="widget-type-btn" onClick={() => handleAddWidget(wt.type)}>
                    {wt.label}
                  </button>
                ))}
              </div>
            </>
          )}
        </div>

        {/* Center: Grid */}
        <div className="dashboard-grid-container">
          {dashboardDef.widgets.length === 0 ? (
            <div className="list-empty">No widgets yet. Click "+ Add Widget" to begin.</div>
          ) : (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(12, 1fr)', gap: 12, gridAutoRows: 80 }}>
              {dashboardDef.widgets.map((w) => (
                <div
                  key={w.id}
                  style={{
                    gridColumn: `${w.layout.x + 1} / span ${w.layout.w}`,
                    gridRow: `${w.layout.y + 1} / span ${w.layout.h}`,
                  }}
                  onClick={() => selectWidget(w.id)}
                >
                  <WidgetCard
                    widget={w}
                    data={dashboardData[w.data_source]}
                    selected={selectedWidgetId === w.id}
                    onRemove={() => removeWidget(w.id)}
                  />
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Right: Widget Config */}
        {selectedWidget && (
          <WidgetConfigPanel widget={selectedWidget} />
        )}
      </div>
    </div>
  );
}
