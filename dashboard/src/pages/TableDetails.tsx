import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Breadcrumb,
  BreadcrumbItem,
  Button,
  Loading,
  Tabs,
  TabList,
  Tab,
  TabPanels,
  TabPanel,
  Tag,
  ProgressBar,
  InlineNotification,
} from '@carbon/react';
import { ArrowLeft, Renew, Settings } from '@carbon/icons-react';
import { vectorSyncApi } from '../services/api';
import type { TableDetails as TableDetailsType, TableConfig } from '../types';
import './TableDetails.scss';

interface TimelineEvent {
  timestamp: string;
  type: 'insert' | 'update' | 'delete';
  count: number;
  processed: boolean;
}

export function TableDetails() {
  const { tableId } = useParams<{ tableId: string }>();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [details, setDetails] = useState<TableDetailsType | null>(null);
  const [config, setConfig] = useState<TableConfig | null>(null);

  const loadData = async () => {
    if (!tableId) return;
    try {
      const [detailsData, configData] = await Promise.all([
        vectorSyncApi.getTableDetails(tableId),
        vectorSyncApi.getTable(tableId),
      ]);
      setDetails(detailsData);
      setConfig(configData);
    } catch (error) {
      console.error('Error loading table details:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadData();
    const interval = setInterval(() => void loadData(), 30000);
    return () => clearInterval(interval);
  }, [tableId]);

  const handleSync = async () => {
    if (!tableId) return;
    try {
      await vectorSyncApi.triggerSync(tableId);
      setTimeout(loadData, 2000);
    } catch (error) {
      console.error('Error triggering sync:', error);
    }
  };

  if (loading) {
    return <Loading description="Loading table details..." withOverlay={false} />;
  }

  if (!details || !config) {
    return (
      <div className="table-details">
        <InlineNotification
          kind="error"
          title="Table not found"
          subtitle={`Table ${tableId} could not be loaded`}
        />
      </div>
    );
  }

  const coveragePercent = details.embeddingStats.totalRows > 0
    ? (details.embeddingStats.embeddedRows / details.embeddingStats.totalRows) * 100
    : 0;

  return (
    <div className="table-details">
      <Breadcrumb>
        <BreadcrumbItem href="/">Overview</BreadcrumbItem>
        <BreadcrumbItem isCurrentPage>{config.tableName}</BreadcrumbItem>
      </Breadcrumb>

      <div className="page-header">
        <div className="header-content">
          <Button
            kind="ghost"
            size="sm"
            renderIcon={ArrowLeft}
            onClick={() => navigate('/')}
          >
            Back
          </Button>
          <div className="title-section">
            <h1>{config.catalog}.{config.tableName}</h1>
            <div className="header-tags">
              <Tag type={details.health.health === 'healthy' ? 'green' : 'red'} size="sm">
                {details.health.health}
              </Tag>
              <Tag type="blue" size="sm">
                {config.vectorColumn}
              </Tag>
            </div>
          </div>
        </div>
        <div className="header-actions">
          <Button
            kind="tertiary"
            size="sm"
            renderIcon={Settings}
            onClick={() => navigate(`/config?table=${tableId}`)}
          >
            Configure
          </Button>
          <Button
            kind="primary"
            size="sm"
            renderIcon={Renew}
            onClick={handleSync}
          >
            Trigger Sync
          </Button>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label">Lag</div>
          <div className="stat-value">{details.health.lag}s</div>
          <div className="stat-description">
            {details.health.lag > 600 ? 'Behind schedule' : 'On track'}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Pending Rows</div>
          <div className="stat-value">{details.health.pendingRows.toLocaleString()}</div>
          <div className="stat-description">
            {details.health.pendingRows > 1000 ? 'High backlog' : 'Normal'}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Index Status</div>
          <div className="stat-value">{details.indexStats.status}</div>
          <div className="stat-description">
            {details.indexStats.vectorCount.toLocaleString()} vectors
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Coverage</div>
          <div className="stat-value">{coveragePercent.toFixed(1)}%</div>
          <div className="stat-description">
            {details.embeddingStats.embeddedRows.toLocaleString()} / {details.embeddingStats.totalRows.toLocaleString()} rows
          </div>
        </div>
      </div>

      <Tabs>
        <TabList aria-label="Table details tabs">
          <Tab>CDC Timeline</Tab>
          <Tab>Embedding Pipeline</Tab>
          <Tab>Index Statistics</Tab>
          <Tab>Configuration</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            <div className="timeline-panel">
              <h3>Change Data Capture Timeline</h3>
              <p className="panel-description">
                Recent CDC events from the source table
              </p>
              <div className="timeline">
                {details.cdcTimeline.map((event, idx) => (
                  <div key={idx} className={`timeline-event ${event.processed ? 'processed' : 'pending'}`}>
                    <div className="event-marker" />
                    <div className="event-content">
                      <div className="event-header">
                        <span className="event-type">{event.type.toUpperCase()}</span>
                        <span className="event-count">{event.count} rows</span>
                      </div>
                      <div className="event-time">
                        {new Date(event.timestamp).toLocaleString()}
                      </div>
                      {event.processed ? (
                        <Tag type="green" size="sm">Processed</Tag>
                      ) : (
                        <Tag type="warm-gray" size="sm">Pending</Tag>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </TabPanel>

          <TabPanel>
            <div className="embedding-panel">
              <h3>Embedding Pipeline Status</h3>
              <p className="panel-description">
                Vector embedding generation progress
              </p>
              <div className="embedding-stats">
                <div className="progress-section">
                  <div className="progress-header">
                    <span>Embedding Coverage</span>
                    <span>{coveragePercent.toFixed(1)}%</span>
                  </div>
                  <ProgressBar
                    value={coveragePercent}
                    max={100}
                    label="Coverage"
                    hideLabel
                  />
                  <div className="progress-details">
                    <span>{details.embeddingStats.embeddedRows.toLocaleString()} embedded</span>
                    <span>{details.embeddingStats.pendingRows.toLocaleString()} pending</span>
                    <span>{details.embeddingStats.failedRows.toLocaleString()} failed</span>
                  </div>
                </div>

                <div className="embedding-metrics">
                  <div className="metric">
                    <div className="metric-label">Total Rows</div>
                    <div className="metric-value">{details.embeddingStats.totalRows.toLocaleString()}</div>
                  </div>
                  <div className="metric">
                    <div className="metric-label">Embedded</div>
                    <div className="metric-value success">{details.embeddingStats.embeddedRows.toLocaleString()}</div>
                  </div>
                  <div className="metric">
                    <div className="metric-label">Pending</div>
                    <div className="metric-value warning">{details.embeddingStats.pendingRows.toLocaleString()}</div>
                  </div>
                  <div className="metric">
                    <div className="metric-label">Failed</div>
                    <div className="metric-value error">{details.embeddingStats.failedRows.toLocaleString()}</div>
                  </div>
                </div>

                {details.embeddingStats.lastEmbeddingTime && (
                  <div className="last-embedding">
                    Last embedding: {new Date(details.embeddingStats.lastEmbeddingTime).toLocaleString()}
                  </div>
                )}
              </div>
            </div>
          </TabPanel>

          <TabPanel>
            <div className="index-panel">
              <h3>HNSW Index Statistics</h3>
              <p className="panel-description">
                Vector index performance and configuration
              </p>
              <div className="index-stats">
                <div className="stat-row">
                  <span className="stat-label">Status</span>
                  <Tag type={details.indexStats.status === 'fresh' ? 'green' : 'warm-gray'} size="sm">
                    {details.indexStats.status}
                  </Tag>
                </div>
                <div className="stat-row">
                  <span className="stat-label">Vector Count</span>
                  <span className="stat-value">{details.indexStats.vectorCount.toLocaleString()}</span>
                </div>
                <div className="stat-row">
                  <span className="stat-label">Dimension</span>
                  <span className="stat-value">{details.indexStats.dimension}</span>
                </div>
                <div className="stat-row">
                  <span className="stat-label">M Parameter</span>
                  <span className="stat-value">{details.indexStats.m}</span>
                </div>
                <div className="stat-row">
                  <span className="stat-label">EF Construction</span>
                  <span className="stat-value">{details.indexStats.efConstruction}</span>
                </div>
                {details.indexStats.lastBuildTime && (
                  <div className="stat-row">
                    <span className="stat-label">Last Build</span>
                    <span className="stat-value">
                      {new Date(details.indexStats.lastBuildTime).toLocaleString()}
                    </span>
                  </div>
                )}
              </div>
            </div>
          </TabPanel>

          <TabPanel>
            <div className="config-panel">
              <h3>Table Configuration</h3>
              <p className="panel-description">
                Current vectorization settings
              </p>
              <div className="config-details">
                <div className="config-row">
                  <span className="config-label">Table ID</span>
                  <code className="config-value">{config.tableId}</code>
                </div>
                <div className="config-row">
                  <span className="config-label">Catalog</span>
                  <code className="config-value">{config.catalog}</code>
                </div>
                <div className="config-row">
                  <span className="config-label">Table Name</span>
                  <code className="config-value">{config.tableName}</code>
                </div>
                <div className="config-row">
                  <span className="config-label">Vector Column</span>
                  <code className="config-value">{config.vectorColumn}</code>
                </div>
                <div className="config-row">
                  <span className="config-label">Text Column</span>
                  <code className="config-value">{config.textColumn}</code>
                </div>
                <div className="config-row">
                  <span className="config-label">Sync Enabled</span>
                  <Tag type={config.syncEnabled ? 'green' : 'gray'} size="sm">
                    {config.syncEnabled ? 'Enabled' : 'Disabled'}
                  </Tag>
                </div>
              </div>
            </div>
          </TabPanel>
        </TabPanels>
      </Tabs>
    </div>
  );
}
