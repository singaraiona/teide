import { useCallback } from 'react';
import useStore from '../store/useStore';

export default function Inspector() {
  const nodes = useStore((s) => s.nodes);
  const selectedNodeId = useStore((s) => s.selectedNodeId);
  const updateNodeConfig = useStore((s) => s.updateNodeConfig);

  const node = nodes.find((n) => n.id === selectedNodeId);

  const setConfig = useCallback(
    (key: string, value: any) => {
      if (!selectedNodeId) return;
      updateNodeConfig(selectedNodeId, { [key]: value });
    },
    [selectedNodeId, updateNodeConfig]
  );

  if (!node) {
    return (
      <div className="inspector">
        <div className="no-selection">Select a node to configure</div>
      </div>
    );
  }

  const { nodeType, label, category, config } = node.data;

  return (
    <div className="inspector">
      <h3>{label}</h3>
      <div className="inspector-category">{category}</div>

      {/* file_path — text input */}
      {(nodeType === 'csv_source') && (
        <div className="field-group">
          <label>File Path</label>
          <input
            type="text"
            value={config.file_path ?? ''}
            onChange={(e) => setConfig('file_path', e.target.value)}
            placeholder="/path/to/data.csv"
          />
        </div>
      )}

      {/* Filter fields */}
      {nodeType === 'filter' && (
        <>
          <div className="field-group">
            <label>Column</label>
            <input
              type="text"
              value={config.column ?? ''}
              onChange={(e) => setConfig('column', e.target.value)}
              placeholder="column_name"
            />
          </div>
          <div className="field-group">
            <label>Operator</label>
            <select
              value={config.operator ?? 'eq'}
              onChange={(e) => setConfig('operator', e.target.value)}
            >
              <option value="eq">eq (=)</option>
              <option value="ne">ne (!=)</option>
              <option value="gt">gt (&gt;)</option>
              <option value="lt">lt (&lt;)</option>
              <option value="ge">ge (&gt;=)</option>
              <option value="le">le (&lt;=)</option>
            </select>
          </div>
          <div className="field-group">
            <label>Value</label>
            <input
              type="text"
              value={config.value ?? ''}
              onChange={(e) => setConfig('value', e.target.value)}
              placeholder="filter value"
            />
          </div>
        </>
      )}

      {/* GroupBy fields */}
      {nodeType === 'groupby' && (
        <>
          <div className="field-group">
            <label>Group Keys (comma-separated)</label>
            <input
              type="text"
              value={Array.isArray(config.keys) ? config.keys.join(', ') : (config.keys ?? '')}
              onChange={(e) => {
                const keys = e.target.value
                  .split(',')
                  .map((s: string) => s.trim())
                  .filter(Boolean);
                setConfig('keys', keys);
              }}
              placeholder="id1, id2"
            />
          </div>
          <div className="field-group">
            <label>Aggregations</label>
            <AggEditor
              aggs={Array.isArray(config.aggs) ? config.aggs : []}
              onChange={(aggs) => setConfig('aggs', aggs)}
            />
          </div>
        </>
      )}

      {/* Sort fields */}
      {nodeType === 'sort' && (
        <div className="field-group">
          <label>Sort Columns</label>
          <SortColumnsEditor
            columns={Array.isArray(config.columns) ? config.columns : []}
            onChange={(cols) => setConfig('columns', cols)}
          />
        </div>
      )}

      {/* Join fields */}
      {nodeType === 'join' && (
        <>
          <div className="field-group">
            <label>Right Table (CSV Path)</label>
            <input
              type="text"
              value={config.right_file ?? ''}
              onChange={(e) => setConfig('right_file', e.target.value)}
              placeholder="/path/to/right.csv"
            />
          </div>
          <div className="field-group">
            <label>Join Keys (comma-separated)</label>
            <input
              type="text"
              value={Array.isArray(config.keys) ? config.keys.join(', ') : (config.keys ?? '')}
              onChange={(e) => {
                const keys = e.target.value
                  .split(',')
                  .map((s: string) => s.trim())
                  .filter(Boolean);
                setConfig('keys', keys);
              }}
              placeholder="key1, key2"
            />
          </div>
          <div className="field-group">
            <label>Join Type</label>
            <select
              value={config.how ?? 'inner'}
              onChange={(e) => setConfig('how', e.target.value)}
            >
              <option value="inner">Inner</option>
              <option value="left">Left</option>
            </select>
          </div>
        </>
      )}

      {/* Grid (output) fields */}
      {nodeType === 'grid' && (
        <div className="field-group">
          <label>Page Size</label>
          <input
            type="number"
            value={config.page_size ?? 100}
            onChange={(e) => setConfig('page_size', parseInt(e.target.value) || 100)}
            min={1}
            max={10000}
          />
        </div>
      )}
    </div>
  );
}

/* ---------- Agg editor sub-component ---------- */

interface Agg {
  column: string;
  op: string;
}

function AggEditor({
  aggs,
  onChange,
}: {
  aggs: Agg[];
  onChange: (aggs: Agg[]) => void;
}) {
  const ops = ['sum', 'avg', 'min', 'max', 'count'];

  const update = (idx: number, field: keyof Agg, value: string) => {
    const next = [...aggs];
    next[idx] = { ...next[idx], [field]: value };
    onChange(next);
  };

  const add = () => onChange([...aggs, { column: '', op: 'sum' }]);

  const remove = (idx: number) => {
    const next = aggs.filter((_, i) => i !== idx);
    onChange(next);
  };

  return (
    <>
      {aggs.map((agg, i) => (
        <div className="array-item" key={i}>
          <input
            type="text"
            value={agg.column}
            onChange={(e) => update(i, 'column', e.target.value)}
            placeholder="column"
          />
          <select value={agg.op} onChange={(e) => update(i, 'op', e.target.value)}>
            {ops.map((op) => (
              <option key={op} value={op}>
                {op}
              </option>
            ))}
          </select>
          <button className="remove-btn" onClick={() => remove(i)}>
            &times;
          </button>
        </div>
      ))}
      <button className="add-btn" onClick={add}>
        + Add aggregation
      </button>
    </>
  );
}

/* ---------- Sort columns editor sub-component ---------- */

interface SortCol {
  name: string;
  descending?: boolean;
}

function SortColumnsEditor({
  columns,
  onChange,
}: {
  columns: SortCol[];
  onChange: (cols: SortCol[]) => void;
}) {
  const update = (idx: number, field: string, value: any) => {
    const next = [...columns];
    next[idx] = { ...next[idx], [field]: value };
    onChange(next);
  };

  const add = () => onChange([...columns, { name: '', descending: false }]);

  const remove = (idx: number) => {
    const next = columns.filter((_, i) => i !== idx);
    onChange(next);
  };

  return (
    <>
      {columns.map((col, i) => (
        <div className="array-item" key={i}>
          <input
            type="text"
            value={col.name}
            onChange={(e) => update(i, 'name', e.target.value)}
            placeholder="column name"
          />
          <div className="checkbox-row">
            <input
              type="checkbox"
              checked={col.descending ?? false}
              onChange={(e) => update(i, 'descending', e.target.checked)}
            />
            <span style={{ fontSize: 11 }}>DESC</span>
          </div>
          <button className="remove-btn" onClick={() => remove(i)}>
            &times;
          </button>
        </div>
      ))}
      <button className="add-btn" onClick={add}>
        + Add column
      </button>
    </>
  );
}
