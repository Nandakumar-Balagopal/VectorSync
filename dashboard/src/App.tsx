import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Header, HeaderName, Content } from '@carbon/react';
import { Navigation } from './components/Navigation';
import { Overview } from './pages/Overview';
import { TableDetails } from './pages/TableDetails';
import { Lifecycle } from './pages/Lifecycle';
import { SemanticSearch } from './pages/SemanticSearch';
import { Configuration } from './pages/Configuration';
import './App.scss';

function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <Header aria-label="VectorSync Dashboard">
          <HeaderName href="/" prefix="">
            VectorSync
          </HeaderName>
        </Header>

        <Navigation />

        <Content className="main-content">
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/tables/:tableId" element={<TableDetails />} />
            <Route path="/lifecycle" element={<Lifecycle />} />
            <Route path="/search" element={<SemanticSearch />} />
            <Route path="/config" element={<Configuration />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Content>
      </div>
    </BrowserRouter>
  );
}

export default App;
