import useStore, { type DashboardWidget } from '../../store/useStore';

interface Props {
  widget: DashboardWidget;
}

export default function WidgetConfigPanel({ widget }: Props) {
  const updateWidgetConfig = useStore((s) => s.updateWidgetConfig);
  const updateWidgetLayout = useStore((s) => s.updateWidgetLayout);
  const dashboardDef = useStore((s) => s.dashboardDef);

  const setConfig = (key: string, value: any) => {
    updateWidgetConfig(widget.id, { [key]: value });
  };

  const setLayout = (key: string, value: number) => {
    updateWidgetLayout(widget.id, { ...widget.layout, [key]: value });
  };

  const dataSources = dashboardDef?.data_sources ?? [];

  return (
    <div className="dashboard-config-panel">
      <h4>Widget Config</h4>

      <div className="field-group">
        <label>Title</label>
        <input
          type="text"
          value={widget.title}
          onChange={(e) => {
            // Update title directly on dashboardDef
            const def = useStore.getState().dashboardDef;
            if (!def) return;
            useStore.getState().setDashboardDef({
              ...def,
              widgets: def.widgets.map((w) =>
                w.id === widget.id ? { ...w, title: e.target.value } : w
              ),
            });
          }}
        />
      </div>

      <div className="field-group">
        <label>Data Source</label>
        <select
          value={widget.data_source}
          onChange={(e) => {
            const def = useStore.getState().dashboardDef;
            if (!def) return;
            useStore.getState().setDashboardDef({
              ...def,
              widgets: def.widgets.map((w) =>
                w.id === widget.id ? { ...w, data_source: e.target.value } : w
              ),
            });
          }}
        >
          <option value="">-- Select --</option>
          {dataSources.map((ds) => (
            <option key={ds.alias} value={ds.alias}>{ds.alias}</option>
          ))}
        </select>
      </div>

      <h4 style={{ marginTop: 16 }}>Layout</h4>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
        <div className="field-group">
          <label>X</label>
          <input type="number" value={widget.layout.x} onChange={(e) => setLayout('x', parseInt(e.target.value) || 0)} min={0} max={11} />
        </div>
        <div className="field-group">
          <label>Y</label>
          <input type="number" value={widget.layout.y} onChange={(e) => setLayout('y', parseInt(e.target.value) || 0)} min={0} />
        </div>
        <div className="field-group">
          <label>Width</label>
          <input type="number" value={widget.layout.w} onChange={(e) => setLayout('w', parseInt(e.target.value) || 1)} min={1} max={12} />
        </div>
        <div className="field-group">
          <label>Height</label>
          <input type="number" value={widget.layout.h} onChange={(e) => setLayout('h', parseInt(e.target.value) || 1)} min={1} max={12} />
        </div>
      </div>

      {/* Type-specific config */}
      {(widget.type === 'bar_chart' || widget.type === 'line_chart') && (
        <>
          <h4 style={{ marginTop: 16 }}>Chart Config</h4>
          <div className="field-group">
            <label>X Column</label>
            <input
              type="text"
              value={widget.config.x_column ?? ''}
              onChange={(e) => setConfig('x_column', e.target.value)}
              placeholder="Auto (first column)"
            />
          </div>
          <div className="field-group">
            <label>Y Column</label>
            <input
              type="text"
              value={widget.config.y_column ?? ''}
              onChange={(e) => setConfig('y_column', e.target.value)}
              placeholder="Auto (second column)"
            />
          </div>
          <div className="field-group">
            <label>Color</label>
            <input
              type="color"
              value={widget.config.color ?? '#4b6777'}
              onChange={(e) => setConfig('color', e.target.value)}
            />
          </div>
        </>
      )}

      {widget.type === 'pie_chart' && (
        <>
          <h4 style={{ marginTop: 16 }}>Pie Config</h4>
          <div className="field-group">
            <label>Label Column</label>
            <input
              type="text"
              value={widget.config.label_column ?? ''}
              onChange={(e) => setConfig('label_column', e.target.value)}
              placeholder="Auto (first column)"
            />
          </div>
          <div className="field-group">
            <label>Value Column</label>
            <input
              type="text"
              value={widget.config.value_column ?? ''}
              onChange={(e) => setConfig('value_column', e.target.value)}
              placeholder="Auto (second column)"
            />
          </div>
        </>
      )}

      {widget.type === 'stat_card' && (
        <>
          <h4 style={{ marginTop: 16 }}>Stat Card Config</h4>
          <div className="field-group">
            <label>Value Column</label>
            <input
              type="text"
              value={widget.config.value_column ?? ''}
              onChange={(e) => setConfig('value_column', e.target.value)}
              placeholder="Auto"
            />
          </div>
          <div className="field-group">
            <label>Aggregation</label>
            <select
              value={widget.config.aggregation ?? 'first'}
              onChange={(e) => setConfig('aggregation', e.target.value)}
            >
              <option value="first">First Value</option>
              <option value="sum">Sum</option>
              <option value="avg">Average</option>
              <option value="count">Count</option>
              <option value="min">Min</option>
              <option value="max">Max</option>
            </select>
          </div>
          <div className="field-group">
            <label>Label</label>
            <input
              type="text"
              value={widget.config.label ?? ''}
              onChange={(e) => setConfig('label', e.target.value)}
              placeholder="Auto"
            />
          </div>
        </>
      )}
    </div>
  );
}
