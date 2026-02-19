interface Props {
  data?: { rows: any[]; columns: string[] };
  config: Record<string, any>;
}

export default function StatCardWidget({ data, config }: Props) {
  if (!data || !data.rows.length) {
    return <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No data. Click Refresh.</div>;
  }

  const valueCol = config.value_column || data.columns[1] || data.columns[0];
  const agg = config.aggregation || 'first'; // first, sum, avg, count, min, max
  const label = config.label || valueCol;

  let value: number | string;
  const values = data.rows.map((r) => Number(r[valueCol])).filter((v) => !isNaN(v));

  switch (agg) {
    case 'sum':
      value = values.reduce((a, b) => a + b, 0);
      break;
    case 'avg':
      value = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
      break;
    case 'count':
      value = data.rows.length;
      break;
    case 'min':
      value = values.length ? Math.min(...values) : 0;
      break;
    case 'max':
      value = values.length ? Math.max(...values) : 0;
      break;
    default:
      value = data.rows[0]?.[valueCol] ?? '';
  }

  const formatted = typeof value === 'number'
    ? value >= 1_000_000
      ? `${(value / 1_000_000).toFixed(1)}M`
      : value >= 1_000
        ? `${(value / 1_000).toFixed(1)}K`
        : Number.isInteger(value)
          ? value.toString()
          : value.toFixed(2)
    : String(value);

  return (
    <div className="stat-card-widget">
      <div className="stat-card-value">{formatted}</div>
      <div className="stat-card-label">{label}</div>
    </div>
  );
}
