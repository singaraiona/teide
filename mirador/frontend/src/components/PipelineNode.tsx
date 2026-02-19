import { memo } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { NodeData } from '../store/useStore';
import useStore from '../store/useStore';

function PipelineNode({ id, data, selected }: NodeProps & { data: NodeData }) {
  const nodeResults = useStore((s) => s.nodeResults);
  const result = nodeResults[id];

  const hasResult = result && !result.error;
  const hasError = result && result.error;

  return (
    <div className={`pipeline-node${selected ? ' selected' : ''}`}>
      <div className={`node-header ${data.category}`}>
        <span>{data.label}</span>
        {hasResult && <span className="node-status ok">&#10003;</span>}
        {hasError && <span className="node-status err">&#10007;</span>}
      </div>
      <div className="node-body">
        {data.nodeType}
        {hasResult && result.rows !== undefined && (
          <div>{typeof result.rows === 'number' ? `${result.rows} rows` : `${result.total ?? ''} rows`}</div>
        )}
      </div>
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
}

export default memo(PipelineNode);
