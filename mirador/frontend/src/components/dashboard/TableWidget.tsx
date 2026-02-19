interface Props {
  data?: { rows: any[]; columns: string[] };
  config: Record<string, any>;
}

export default function TableWidget({ data }: Props) {
  if (!data || !data.rows.length) {
    return <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No data. Click Refresh.</div>;
  }

  const { rows, columns } = data;
  const maxRows = 50;

  return (
    <div style={{ overflow: 'auto', height: '100%' }}>
      <table className="widget-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, maxRows).map((row, i) => (
            <tr key={i}>
              {columns.map((col) => (
                <td key={col}>{String(row[col] ?? '')}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > maxRows && (
        <div style={{ fontSize: 11, color: 'var(--text-muted)', padding: '4px 6px' }}>
          Showing {maxRows} of {rows.length} rows
        </div>
      )}
    </div>
  );
}
