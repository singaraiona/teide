import './App.css';
import NavBar from './components/NavBar';
import useStore from './store/useStore';
import WorkflowListPage from './pages/WorkflowListPage';
import DashboardListPage from './pages/DashboardListPage';
import WorkflowEditorPage from './pages/WorkflowEditorPage';
import DashboardEditorPage from './pages/DashboardEditorPage';

function App() {
  const view = useStore((s) => s.currentView);

  return (
    <div className="app-layout">
      <NavBar />
      {view === 'workflows' && <WorkflowListPage />}
      {view === 'dashboards' && <DashboardListPage />}
      {view === 'workflow-editor' && <WorkflowEditorPage />}
      {view === 'dashboard-editor' && <DashboardEditorPage />}
    </div>
  );
}

export default App;
