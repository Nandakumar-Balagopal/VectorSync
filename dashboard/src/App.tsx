import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Header, HeaderName, HeaderGlobalBar, HeaderGlobalAction, Content } from '@carbon/react';
import { Notification, UserAvatar } from '@carbon/icons-react';
import { Navigation } from './components/Navigation';
import { Overview } from './pages/Overview';
import { TableDetails } from './pages/TableDetails';
import { SemanticSearch } from './pages/SemanticSearch';
import { DebugPanel } from './pages/DebugPanel';
import { Configuration } from './pages/Configuration';
import { GlobalPipeline } from './pages/GlobalPipeline';
import { JobQueue } from './pages/JobQueue';
import { Alerts } from './pages/Alerts';
import './App.scss';

function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <Header aria-label="VectorSync Dashboard">
          <HeaderName href="/" prefix="">
            VectorSync
          </HeaderName>
          <HeaderGlobalBar>
            <HeaderGlobalAction aria-label="Notifications">
              <Notification size={20} />
            </HeaderGlobalAction>
            <HeaderGlobalAction aria-label="User Profile">
              <UserAvatar size={20} />
            </HeaderGlobalAction>
          </HeaderGlobalBar>
        </Header>

        <Navigation />

        <Content className="main-content">
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/tables/:tableId" element={<TableDetails />} />
            <Route path="/pipeline" element={<GlobalPipeline />} />
            <Route path="/jobs" element={<JobQueue />} />
            <Route path="/search" element={<SemanticSearch />} />
            <Route path="/debug" element={<DebugPanel />} />
            <Route path="/config" element={<Configuration />} />
            <Route path="/alerts" element={<Alerts />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Content>
      </div>
    </BrowserRouter>
  );
}

export default App;