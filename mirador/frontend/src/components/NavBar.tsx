import useStore, { type AppView } from '../store/useStore';
import { IconTeide } from './icons';

const tabs: { view: AppView; label: string }[] = [
  { view: 'workflows', label: 'Pipelines' },
  { view: 'dashboards', label: 'Dashboards' },
];

export default function NavBar() {
  const currentView = useStore((s) => s.currentView);
  const setView = useStore((s) => s.setView);

  const isEditing = currentView === 'workflow-editor' || currentView === 'dashboard-editor';

  return (
    <nav className="navbar">
      <div className="navbar-brand">
        <IconTeide />
        <span className="navbar-title">Mirador</span>
      </div>
      {!isEditing && (
        <div className="navbar-tabs">
          {tabs.map((t) => (
            <button
              key={t.view}
              className={`navbar-tab${currentView === t.view ? ' active' : ''}`}
              onClick={() => setView(t.view)}
            >
              {t.label}
            </button>
          ))}
        </div>
      )}
      <div className="navbar-spacer" />
    </nav>
  );
}
