import NodePalette from './NodePalette';
import Inspector from './Inspector';

export default function Sidebar() {
  return (
    <div className="sidebar">
      <NodePalette />
      <Inspector />
    </div>
  );
}
