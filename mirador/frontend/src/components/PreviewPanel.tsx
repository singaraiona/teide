import useStore from '../store/useStore';

export default function PreviewPanel() {
  const selectedNodeId = useStore((s) => s.selectedNodeId);
  const nodeResults = useStore((s) => s.nodeResults);

  // Show global error if present
  if (nodeResults._error) {
    return (
      <div className="preview-panel">
        <h3>Pipeline Error</h3>
        <div className="preview-error">{nodeResults._error.error}</div>
      </div>
    );
  }

  if (!selectedNodeId) {
    return (
      <div className="preview-panel">
        <h3>Preview</h3>
        <div className="preview-empty">Select a node to see its output</div>
      </div>
    );
  }

  const result = nodeResults[selectedNodeId];

  if (!result) {
    return (
      <div className="preview-panel">
        <h3>Preview</h3>
        <div className="preview-empty">Run the pipeline to see results</div>
      </div>
    );
  }

  if (result.error) {
    return (
      <div className="preview-panel">
        <h3>Preview</h3>
        <div className="preview-error">{result.error}</div>
      </div>
    );
  }

  const { rows, columns, total } = result;

  // Simple info for nodes without row data (e.g., csv_source returns rows as count)
  if (typeof rows === 'number') {
    return (
      <div className="preview-panel">
        <h3>Preview</h3>
        <div className="preview-meta">
          {rows} rows{columns && Array.isArray(columns) ? ` | ${columns.length} columns: ${columns.join(', ')}` : ''}
        </div>
      </div>
    );
  }

  // Table display for nodes with row arrays (e.g., grid)
  if (Array.isArray(rows) && rows.length > 0 && Array.isArray(columns)) {
    return (
      <div className="preview-panel">
        <h3>Preview</h3>
        <div className="preview-meta">
          Showing {rows.length} of {total ?? rows.length} rows | {columns.length} columns
        </div>
        <table>
          <thead>
            <tr>
              {columns.map((col: string) => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row: Record<string, any>, i: number) => (
              <tr key={i}>
                {columns.map((col: string) => (
                  <td key={col}>{String(row[col] ?? '')}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  // Fallback: show raw JSON
  return (
    <div className="preview-panel">
      <h3>Preview</h3>
      <pre style={{ fontSize: 11, overflow: 'auto', maxHeight: 160 }}>
        {JSON.stringify(result, null, 2)}
      </pre>
    </div>
  );
}
