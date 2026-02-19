import { useEffect, useState, type DragEvent } from 'react';
import { fetchNodeTypes, type NodeTypeMeta } from '../api/client';
import { NodeIcon } from './icons';

export default function NodePalette() {
  const [nodeTypes, setNodeTypes] = useState<NodeTypeMeta[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState('');

  useEffect(() => {
    fetchNodeTypes()
      .then((data) => {
        setNodeTypes(data);
        setLoading(false);
      })
      .catch((err) => {
        console.error('Failed to fetch node types:', err);
        setError('Could not load node types. Is the backend running?');
        setLoading(false);
      });
  }, []);

  const onDragStart = (event: DragEvent, meta: NodeTypeMeta) => {
    event.dataTransfer.setData(
      'application/reactflow',
      JSON.stringify({
        nodeType: meta.id,
        label: meta.label,
        category: meta.category,
      })
    );
    event.dataTransfer.effectAllowed = 'move';
  };

  // Filter by search query
  const filtered = search.trim()
    ? nodeTypes.filter((nt) => {
        const q = search.toLowerCase();
        return nt.label.toLowerCase().includes(q) ||
          (nt.description ?? '').toLowerCase().includes(q) ||
          nt.id.toLowerCase().includes(q);
      })
    : nodeTypes;

  // Group by category, maintain consistent order
  const categoryOrder = ['input', 'compute', 'generic', 'output'];
  const grouped: Record<string, NodeTypeMeta[]> = {};
  for (const nt of filtered) {
    if (!grouped[nt.category]) grouped[nt.category] = [];
    grouped[nt.category].push(nt);
  }

  if (loading) {
    return (
      <div className="node-palette">
        <p style={{ color: 'var(--text-muted)', fontSize: 13 }}>Loading nodes...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="node-palette">
        <p style={{ color: 'var(--red)', fontSize: 13 }}>{error}</p>
      </div>
    );
  }

  return (
    <div className="node-palette">
      <div className="palette-search-sticky">
        <div className="palette-search">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            <circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>
          </svg>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search nodes..."
          />
          {search && (
            <button className="palette-search-clear" onClick={() => setSearch('')}>&times;</button>
          )}
        </div>
      </div>
      <div className="palette-list">
        {categoryOrder.map(
          (cat) =>
            grouped[cat] && (
              <div key={cat}>
                <h3>{cat}</h3>
                {grouped[cat].map((nt) => (
                  <div
                    key={nt.id}
                    className="palette-item"
                    draggable
                    onDragStart={(e) => onDragStart(e, nt)}
                  >
                    <div className="item-icon">
                      <NodeIcon nodeType={nt.id} size={16} />
                    </div>
                    <div className="item-text">
                      <span className="item-name">{nt.label}</span>
                      {nt.description && <span className="item-desc">{nt.description}</span>}
                    </div>
                  </div>
                ))}
              </div>
            )
        )}
      </div>
    </div>
  );
}
