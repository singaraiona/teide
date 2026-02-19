interface Props {
  data?: { rows: any[]; columns: string[] };
  config: Record<string, any>;
}

export default function LineChartWidget({ data, config }: Props) {
  if (!data || !data.rows.length) {
    return <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No data. Click Refresh.</div>;
  }

  const yCol = config.y_column || data.columns[1] || data.columns[0];
  const rows = data.rows.slice(0, 50);
  const values = rows.map((r) => Number(r[yCol]) || 0);
  const minVal = Math.min(...values);
  const maxVal = Math.max(...values);
  const range = maxVal - minVal || 1;

  // SVG line chart
  const w = 300;
  const h = 100;
  const pad = 4;
  const points = values.map((v, i) => {
    const x = pad + (i / Math.max(values.length - 1, 1)) * (w - 2 * pad);
    const y = h - pad - ((v - minVal) / range) * (h - 2 * pad);
    return `${x},${y}`;
  });

  const color = config.color || 'var(--primary)';

  return (
    <svg viewBox={`0 0 ${w} ${h}`} style={{ width: '100%', height: '100%' }} preserveAspectRatio="none">
      <polyline
        points={points.join(' ')}
        fill="none"
        stroke={color}
        strokeWidth="2"
        strokeLinejoin="round"
      />
    </svg>
  );
}
