import { useCallback, useEffect, useState } from 'react';
import { browseFiles, type FileEntry } from '../api/client';

interface FileBrowserProps {
  onSelect: (path: string) => void;
  onClose: () => void;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export default function FileBrowser({ onSelect, onClose }: FileBrowserProps) {
  const [currentPath, setCurrentPath] = useState('~');
  const [entries, setEntries] = useState<FileEntry[]>([]);
  const [parentPath, setParentPath] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [resolvedPath, setResolvedPath] = useState('');

  const loadDir = useCallback(async (path: string) => {
    setLoading(true);
    setError(null);
    try {
      const result = await browseFiles(path);
      setResolvedPath(result.path);
      setParentPath(result.parent);
      if (result.error) {
        setError(result.error);
        setEntries([]);
      } else {
        setEntries(result.entries);
      }
    } catch {
      setError('Failed to connect to server');
      setEntries([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadDir(currentPath);
  }, [currentPath, loadDir]);

  const navigateTo = (path: string) => setCurrentPath(path);

  return (
    <div className="fb-overlay" onClick={onClose}>
      <div className="fb-dialog" onClick={(e) => e.stopPropagation()}>
        <div className="fb-header">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className="fb-icon">
            <path d="M1 3.5C1 2.67 1.67 2 2.5 2H6l1.5 1.5H13.5C14.33 3.5 15 4.17 15 5V12.5C15 13.33 14.33 14 13.5 14H2.5C1.67 14 1 13.33 1 12.5V3.5Z" fill="var(--primary)" opacity="0.8"/>
          </svg>
          <span className="fb-title">Select File</span>
          <button className="fb-close" onClick={onClose}>&times;</button>
        </div>

        <div className="fb-path-bar">
          {parentPath && (
            <button className="fb-up-btn" onClick={() => navigateTo(parentPath)} title="Go up">
              <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 2L2 8h4v6h4V8h4L8 2z"/>
              </svg>
            </button>
          )}
          <span className="fb-path-text" title={resolvedPath}>{resolvedPath}</span>
        </div>

        <div className="fb-entries">
          {loading && <div className="fb-message">Loading...</div>}
          {error && <div className="fb-message fb-error">{error}</div>}
          {!loading && !error && entries.length === 0 && (
            <div className="fb-message">Empty directory</div>
          )}
          {entries.map((entry) => (
            <div
              key={entry.path}
              className={`fb-entry fb-entry-${entry.type}`}
              onClick={() =>
                entry.type === 'dir' ? navigateTo(entry.path) : onSelect(entry.path)
              }
            >
              {entry.type === 'dir' ? (
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className="fb-entry-icon">
                  <path d="M1 3.5C1 2.67 1.67 2 2.5 2H6l1.5 1.5H13.5C14.33 3.5 15 4.17 15 5V12.5C15 13.33 14.33 14 13.5 14H2.5C1.67 14 1 13.33 1 12.5V3.5Z" fill="#f59e0b" opacity="0.85"/>
                </svg>
              ) : (
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className="fb-entry-icon">
                  <rect x="3" y="1" width="10" height="14" rx="1.5" fill="var(--primary)" opacity="0.2" stroke="var(--primary)" strokeWidth="0.8"/>
                  <path d="M5 5h6M5 7.5h6M5 10h4" stroke="var(--primary)" strokeWidth="0.8" strokeLinecap="round"/>
                </svg>
              )}
              <span className="fb-entry-name">{entry.name}</span>
              {entry.size !== undefined && (
                <span className="fb-entry-size">{formatSize(entry.size)}</span>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
