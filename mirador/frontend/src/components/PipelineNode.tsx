import { memo, useCallback, useRef, useEffect, useState } from 'react';
import { Handle, Position, useReactFlow, type NodeProps } from '@xyflow/react';
import type { NodeData } from '../store/useStore';
import useStore from '../store/useStore';
import { NodeIcon } from './icons';

function PipelineNode({ id, data, selected }: NodeProps & { data: NodeData }) {
  const result = useStore((s) => s.nodeResults[id]);
  const isExecuting = useStore((s) => s.executingNodeId === id);
  const editing = useStore((s) => s.editingNodeId === id);
  const setEditingNodeId = useStore((s) => s.setEditingNodeId);
  const updateNodeLabel = useStore((s) => s.updateNodeLabel);
  const { deleteElements } = useReactFlow();

  const [editValue, setEditValue] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

  const hasResult = result && !result.error;
  const hasError = result && result.error;

  const onDelete = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      deleteElements({ nodes: [{ id }] });
    },
    [id, deleteElements]
  );

  useEffect(() => {
    if (editing) setEditValue(data.label);
  }, [editing, data.label]);

  useEffect(() => {
    if (editing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [editing]);

  const commitEdit = useCallback(() => {
    const trimmed = editValue.trim();
    if (trimmed && trimmed !== data.label) {
      updateNodeLabel(id, trimmed);
    }
    setEditingNodeId(null);
  }, [editValue, data.label, id, updateNodeLabel, setEditingNodeId]);

  const onKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      e.stopPropagation();
      if (e.key === 'Enter') commitEdit();
      else if (e.key === 'Escape') setEditingNodeId(null);
    },
    [commitEdit, setEditingNodeId]
  );

  const inputCount = data.category === 'input' ? 0 : 1;
  const outputCount = data.category === 'output' ? 0 : 1;

  return (
    <div className={`pipeline-node${selected ? ' selected' : ''}${isExecuting ? ' executing' : ''}`}>
      {/* Handles positioned at icon box center */}
      {Array.from({ length: inputCount }).map((_, i) => (
        <Handle key={`in-${i}`} type="target" position={Position.Left} id={`in-${i}`} style={{ top: 19 }} />
      ))}

      {/* The icon box IS the node */}
      <div className="node-icon-box">
        <NodeIcon nodeType={data.nodeType} size={20} />

        {/* Status dot */}
        <div className="node-status-dot">
          {isExecuting && <span className="status-pulse" />}
          {!isExecuting && hasResult && <span className="status-ok" />}
          {!isExecuting && hasError && <span className="status-err" />}
        </div>

        <button className="node-delete-btn" onClick={onDelete} title="Delete node">
          &times;
        </button>
      </div>

      {/* Name label below the icon box */}
      {editing ? (
        <input
          ref={inputRef}
          className="node-name-input"
          value={editValue}
          onChange={(e) => setEditValue(e.target.value)}
          onBlur={commitEdit}
          onKeyDown={onKeyDown}
          onClick={(e) => e.stopPropagation()}
          onMouseDown={(e) => e.stopPropagation()}
        />
      ) : (
        <span className="node-name">{data.label}</span>
      )}

      {Array.from({ length: outputCount }).map((_, i) => (
        <Handle key={`out-${i}`} type="source" position={Position.Right} id={`out-${i}`} style={{ top: 19 }} />
      ))}
    </div>
  );
}

export default memo(PipelineNode);
