import './App.css';
import Toolbar from './components/Toolbar';
import Sidebar from './components/Sidebar';
import Canvas from './components/Canvas';
import PreviewPanel from './components/PreviewPanel';

function App() {
  return (
    <div className="app-layout">
      <Toolbar />
      <div className="app-main">
        <Sidebar />
        <Canvas />
      </div>
      <PreviewPanel />
    </div>
  );
}

export default App;
