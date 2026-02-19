import { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import useStore from '../store/useStore';

function formatTime(ts: number) {
  const d = new Date(ts);
  return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function LevelIcon({ level }: { level: string }) {
  switch (level) {
    case 'info':
      return (
        <svg className="console-level-icon" width="12" height="12" viewBox="0 0 12 12">
          <circle cx="6" cy="6" r="5" fill="none" stroke="currentColor" strokeWidth="1.2"/>
          <path d="M6 5.2v3.3" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round"/>
          <circle cx="6" cy="3.5" r="0.8" fill="currentColor"/>
        </svg>
      );
    case 'warn':
      return (
        <svg className="console-level-icon" width="12" height="12" viewBox="0 0 12 12">
          <path d="M5.5 1.5a.58.58 0 011 0l4.3 7.9a.56.56 0 01-.5.85H1.7a.56.56 0 01-.5-.85Z" fill="none" stroke="currentColor" strokeWidth="1.1"/>
          <path d="M6 4.8v2.2" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round"/>
          <circle cx="6" cy="8.5" r="0.7" fill="currentColor"/>
        </svg>
      );
    case 'error':
      return (
        <svg className="console-level-icon" width="12" height="12" viewBox="0 0 12 12">
          <circle cx="6" cy="6" r="5" fill="none" stroke="currentColor" strokeWidth="1.2"/>
          <path d="M4.2 4.2l3.6 3.6M7.8 4.2l-3.6 3.6" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round"/>
        </svg>
      );
    default:
      return null;
  }
}

export default function PreviewPanel() {
  const selectedNodeId = useStore((s) => s.selectedNodeId);
  const nodeResults = useStore((s) => s.nodeResults);
  const consoleMessages = useStore((s) => s.consoleMessages);
  const bottomTab = useStore((s) => s.bottomTab);
  const setBottomTab = useStore((s) => s.setBottomTab);
  const clearConsole = useStore((s) => s.clearConsole);

  const [height, setHeight] = useState(220);
  const [consoleFilter, setConsoleFilter] = useState<'all' | 'error' | 'warn' | 'info'>('all');
  const dragging = useRef(false);
  const startY = useRef(0);
  const startH = useRef(220);
  const consoleEndRef = useRef<HTMLDivElement>(null);

  const counts = useMemo(() => {
    const c = { error: 0, warn: 0, info: 0 };
    for (const m of consoleMessages) {
      if (m.level in c) c[m.level as keyof typeof c]++;
    }
    return c;
  }, [consoleMessages]);

  const filteredMessages = useMemo(() => {
    if (consoleFilter === 'all') return consoleMessages;
    return consoleMessages.filter((m) => m.level === consoleFilter);
  }, [consoleMessages, consoleFilter]);

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    startY.current = e.clientY;
    startH.current = height;
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
  }, [height]);

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!dragging.current) return;
      const delta = startY.current - e.clientY;
      setHeight(Math.min(window.innerHeight * 0.5, Math.max(80, startH.current + delta)));
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

  // Auto-scroll console to bottom on new messages
  useEffect(() => {
    if (bottomTab === 'console') {
      consoleEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [consoleMessages.length, bottomTab]);

  // Build preview content
  let previewContent: React.ReactNode;
  const globalError = nodeResults._error;

  if (globalError) {
    previewContent = <div className="preview-error">{globalError.error}</div>;
  } else if (!selectedNodeId) {
    previewContent = <div className="preview-empty">Select a node to see its output</div>;
  } else {
    const result = nodeResults[selectedNodeId];
    if (!result) {
      previewContent = <div className="preview-empty">Run the pipeline to see results</div>;
    } else if (result.error) {
      previewContent = <div className="preview-error">{result.error}</div>;
    } else if (typeof result.rows === 'number') {
      previewContent = (
        <div className="preview-meta">
          {result.rows} rows{result.columns && Array.isArray(result.columns) ? ` | ${result.columns.length} columns: ${result.columns.join(', ')}` : ''}
        </div>
      );
    } else if (Array.isArray(result.rows) && result.rows.length > 0 && Array.isArray(result.columns)) {
      previewContent = (
        <>
          <div className="preview-meta">
            Showing {result.rows.length} of {result.total ?? result.rows.length} rows | {result.columns.length} columns
          </div>
          <table>
            <thead>
              <tr>
                {result.columns.map((col: string) => <th key={col}>{col}</th>)}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row: Record<string, any>, i: number) => (
                <tr key={i}>
                  {result.columns.map((col: string) => (
                    <td key={col}>{String(row[col] ?? '')}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </>
      );
    } else {
      previewContent = (
        <pre style={{ fontSize: 11, overflow: 'auto', maxHeight: 160 }}>
          {JSON.stringify(result, null, 2)}
        </pre>
      );
    }
  }

  return (
    <div className="preview-panel" style={{ height }}>
      <div className="resize-handle-top" onMouseDown={onMouseDown} />
      <div className="bottom-tabs">
        <button
          className={`bottom-tab${bottomTab === 'preview' ? ' active' : ''}`}
          onClick={() => setBottomTab('preview')}
        >
          Preview
        </button>
        <button
          className={`bottom-tab${bottomTab === 'console' ? ' active' : ''}`}
          onClick={() => setBottomTab('console')}
        >
          Console
          {consoleMessages.length > 0 && (
            <span className="console-badge">{consoleMessages.length}</span>
          )}
        </button>
        <div className="bottom-tabs-spacer" />
      </div>
      <div className={`bottom-content${bottomTab === 'console' ? ' console-mode' : ''}`}>
        {bottomTab === 'preview' ? (
          previewContent
        ) : (
          <div className="console-log">
            <div className="console-filter-bar">
              <button
                className={`console-filter-btn${consoleFilter === 'all' ? ' active' : ''}`}
                onClick={() => setConsoleFilter('all')}
              >
                All
              </button>
              <button
                className={`console-filter-btn level-error${consoleFilter === 'error' ? ' active' : ''}`}
                onClick={() => setConsoleFilter('error')}
              >
                Errors{counts.error > 0 && <span className="filter-count">{counts.error}</span>}
              </button>
              <button
                className={`console-filter-btn level-warn${consoleFilter === 'warn' ? ' active' : ''}`}
                onClick={() => setConsoleFilter('warn')}
              >
                Warnings{counts.warn > 0 && <span className="filter-count">{counts.warn}</span>}
              </button>
              <button
                className={`console-filter-btn level-info${consoleFilter === 'info' ? ' active' : ''}`}
                onClick={() => setConsoleFilter('info')}
              >
                Info{counts.info > 0 && <span className="filter-count">{counts.info}</span>}
              </button>
              <div style={{ flex: 1 }} />
              {consoleMessages.length > 0 && (
                <button className="console-clear-inline" onClick={clearConsole}>
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
                    <circle cx="6" cy="6" r="5"/><path d="M4 8l4-4"/>
                  </svg>
                </button>
              )}
            </div>
            <div className="console-lines">
              {filteredMessages.length === 0 && (
                <div className="console-empty">
                  {consoleMessages.length === 0
                    ? 'Run a pipeline to see execution logs'
                    : 'No matching messages'}
                </div>
              )}
              {filteredMessages.map((msg, i) => (
                <div key={i} className={`console-line level-${msg.level}`}>
                  <LevelIcon level={msg.level} />
                  <span className="console-text">{msg.text}</span>
                  <span className="console-time">{formatTime(msg.timestamp)}</span>
                </div>
              ))}
              <div ref={consoleEndRef} />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
