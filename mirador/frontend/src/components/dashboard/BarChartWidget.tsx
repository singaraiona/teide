interface Props {
  data?: { rows: any[]; columns: string[] };
  config: Record<string, any>;
}

export default function BarChartWidget({ data, config }: Props) {
  if (!data || !data.rows.length) {
    return <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No data. Click Refresh.</div>;
  }

  const xCol = config.x_column || data.columns[0];
  const yCol = config.y_column || data.columns[1] || data.columns[0];
  const rows = data.rows.slice(0, 20);

  // Find max for scaling
  const values = rows.map((r) => Number(r[yCol]) || 0);
  const maxVal = Math.max(...values, 1);

  return (
    <div style={{ display: 'flex', alignItems: 'flex-end', gap: 2, height: '100%', padding: '4px 0' }}>
      {rows.map((row, i) => {
        const val = Number(row[yCol]) || 0;
        const pct = (val / maxVal) * 100;
        return (
          <div key={i} style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', minWidth: 0 }}>
            <div
              style={{
                width: '80%',
                height: `${Math.max(pct, 2)}%`,
                background: config.color || 'var(--primary)',
                borderRadius: '2px 2px 0 0',
                opacity: 0.75,
              }}
              title={`${row[xCol]}: ${val}`}
            />
            <div style={{ fontSize: 8, color: 'var(--text-muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%', textAlign: 'center' }}>
              {String(row[xCol] ?? '').slice(0, 6)}
            </div>
          </div>
        );
      })}
    </div>
  );
}
