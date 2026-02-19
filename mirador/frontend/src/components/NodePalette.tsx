import { useEffect, useState, type DragEvent } from 'react';
import { fetchNodeTypes, type NodeTypeMeta } from '../api/client';

export default function NodePalette() {
  const [nodeTypes, setNodeTypes] = useState<NodeTypeMeta[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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

  // Group by category, maintain consistent order
  const categoryOrder = ['input', 'compute', 'generic', 'output'];
  const grouped: Record<string, NodeTypeMeta[]> = {};
  for (const nt of nodeTypes) {
    if (!grouped[nt.category]) grouped[nt.category] = [];
    grouped[nt.category].push(nt);
  }

  if (loading) {
    return (
      <div className="node-palette">
        <p style={{ color: '#999', fontSize: 13 }}>Loading nodes...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="node-palette">
        <p style={{ color: '#ef4444', fontSize: 13 }}>{error}</p>
      </div>
    );
  }

  return (
    <div className="node-palette">
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
                  <div className="item-label">{nt.label}</div>
                  {nt.description && (
                    <div className="item-desc">{nt.description}</div>
                  )}
                </div>
              ))}
            </div>
          )
      )}
    </div>
  );
}
