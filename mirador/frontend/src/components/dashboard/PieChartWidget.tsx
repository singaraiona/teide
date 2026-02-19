interface Props {
  data?: { rows: any[]; columns: string[] };
  config: Record<string, any>;
}

const COLORS = ['#4b6777', '#22c55e', '#f97316', '#8b5cf6', '#ef4444', '#06b6d4', '#eab308', '#ec4899'];

export default function PieChartWidget({ data, config }: Props) {
  if (!data || !data.rows.length) {
    return <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No data. Click Refresh.</div>;
  }

  const labelCol = config.label_column || data.columns[0];
  const valueCol = config.value_column || data.columns[1] || data.columns[0];
  const rows = data.rows.slice(0, 8);
  const values = rows.map((r) => Math.abs(Number(r[valueCol]) || 0));
  const total = values.reduce((a, b) => a + b, 0) || 1;

  // SVG pie chart
  const cx = 50, cy = 50, r = 40;
  let cumAngle = -Math.PI / 2;
  const slices = values.map((v, i) => {
    const angle = (v / total) * 2 * Math.PI;
    const x1 = cx + r * Math.cos(cumAngle);
    const y1 = cy + r * Math.sin(cumAngle);
    cumAngle += angle;
    const x2 = cx + r * Math.cos(cumAngle);
    const y2 = cy + r * Math.sin(cumAngle);
    const largeArc = angle > Math.PI ? 1 : 0;
    const d = `M${cx},${cy} L${x1},${y1} A${r},${r} 0 ${largeArc},1 ${x2},${y2} Z`;
    return (
      <path key={i} d={d} fill={COLORS[i % COLORS.length]} opacity={0.8}>
        <title>{`${rows[i][labelCol]}: ${v}`}</title>
      </path>
    );
  });

  return (
    <div style={{ display: 'flex', alignItems: 'center', height: '100%', gap: 8 }}>
      <svg viewBox="0 0 100 100" style={{ width: 80, height: 80, flexShrink: 0 }}>
        {slices}
      </svg>
      <div style={{ fontSize: 10, color: 'var(--text-secondary)', overflow: 'hidden' }}>
        {rows.map((row, i) => (
          <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 4, whiteSpace: 'nowrap' }}>
            <span style={{ width: 8, height: 8, borderRadius: 2, background: COLORS[i % COLORS.length], flexShrink: 0 }} />
            <span style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>{String(row[labelCol] ?? '')}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
