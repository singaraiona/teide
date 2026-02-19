import { useState, useCallback, useRef, useEffect } from 'react';
import NodePalette from './NodePalette';

export default function Sidebar() {
  const [width, setWidth] = useState(220);
  const dragging = useRef(false);

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
  }, []);

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!dragging.current) return;
      setWidth(Math.min(380, Math.max(140, e.clientX)));
    };
    const onMouseUp = () => {
      if (!dragging.current) return;
      dragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };
  }, []);

  return (
    <div className="sidebar" style={{ width }}>
      <NodePalette />
      <div className="resize-handle-right" onMouseDown={onMouseDown} />
    </div>
  );
}
