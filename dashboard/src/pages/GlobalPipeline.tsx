import React, { useState, useEffect } from 'react';
import {
  Tile,
  DataTable,
  TableContainer,
  Table,
  TableHead,
  TableRow,
  TableHeader,
  TableBody,
  TableCell,
  Tag,
  ProgressBar,
  Loading,
  Grid,
  Column,
} from '@carbon/react';
import { Chip, Activity, Dashboard, ServerProxy } from '@carbon/icons-react';
import { vectorSyncApi } from '../services/api';
import type { GlobalPipelineStats, WorkerInfo, PerformanceMetrics } from '../types';
import './GlobalPipeline.scss';

export const GlobalPipeline: React.FC = () => {
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState<GlobalPipelineStats | null>(null);
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [metrics, setMetrics] = useState<PerformanceMetrics | null>(null);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 5000);
    return () => clearInterval(interval);
  }, []);

  const loadData = async () => {
    try {
      const [pipelineStats, workerData, performanceData] = await Promise.all([
        vectorSyncApi.getPipelineStats(),
        vectorSyncApi.getWorkers(),
        vectorSyncApi.getPerformanceMetrics(),
      ]);
      
      setStats(pipelineStats);
      setWorkers(Array.isArray(workerData) ? workerData : []);
      setMetrics(performanceData);
    } catch (err) {
      console.error('Failed to load pipeline data:', err);
    } finally {
      setLoading(false);
    }
  };

  const workerHeaders = [
    { key: 'workerId', header: 'Worker ID' },
    { key: 'status', header: 'Status' },
    { key: 'cpuUsage', header: 'CPU' },
    { key: 'memoryUsage', header: 'Memory' },
    { key: 'currentJobs', header: 'Current Jobs' },
    { key: 'uptime', header: 'Uptime' },
  ];

  const workerRows = workers.map(worker => ({
    id: worker.workerId,
    workerId: worker.workerId,
    status: worker.status,
    cpuUsage: worker.cpuUsage,
    memoryUsage: worker.memoryUsage,
    currentJobs: worker.currentJobs,
    uptime: worker.uptime,
  }));

  const formatUptime = (seconds: number): string => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${hours}h ${minutes}m`;
  };

  const getStatusColor = (status: string): 'green' | 'blue' | 'red' => {
    switch (status) {
      case 'active': return 'green';
      case 'idle': return 'blue';
      case 'offline': return 'red';
      default: return 'blue';
    }
  };

  if (loading) {
    return <Loading description="Loading pipeline data..." withOverlay={false} />;
  }

  return (
    <div className="global-pipeline">
      <div className="global-pipeline__header">
        <h1>Global Pipeline</h1>
        <p>System-wide monitoring and operational visibility</p>
      </div>

      {/* Summary Cards */}
      <Grid className="global-pipeline__summary" narrow>
        <Column sm={4} md={4} lg={4}>
          <Tile className="global-pipeline__card">
            <div className="global-pipeline__card-icon">
              <Activity size={32} />
            </div>
            <div className="global-pipeline__card-content">
              <h3>{stats?.activeWorkers ?? 0}</h3>
              <p>Active Workers</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={4}>
          <Tile className="global-pipeline__card">
            <div className="global-pipeline__card-icon">
              <Dashboard size={32} />
            </div>
            <div className="global-pipeline__card-content">
              <h3>{stats?.totalJobsQueued ?? 0}</h3>
              <p>Jobs Queued</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={4}>
          <Tile className="global-pipeline__card">
            <div className="global-pipeline__card-icon">
              <ServerProxy size={32} />
            </div>
            <div className="global-pipeline__card-content">
              <h3>{stats?.jobsInProgress ?? 0}</h3>
              <p>Jobs In Progress</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={4}>
          <Tile className="global-pipeline__card">
            <div className="global-pipeline__card-icon">
              <Chip size={32} />
            </div>
            <div className="global-pipeline__card-content">
              <h3>{stats?.throughput?.toFixed(1) ?? '0.0'}</h3>
              <p>Jobs/Min</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={4}>
          <Tile className="global-pipeline__card">
            <div className="global-pipeline__card-content">
              <h3>{stats?.successRate?.toFixed(1) ?? '0.0'}%</h3>
              <p>Success Rate</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={4}>
          <Tile className="global-pipeline__card">
            <div className="global-pipeline__card-content">
              <h3>{stats?.backlogSize ?? 0}</h3>
              <p>Backlog Size</p>
            </div>
          </Tile>
        </Column>
      </Grid>

      {/* Resource Utilization */}
      <div className="global-pipeline__section">
        <h2>Resource Utilization</h2>
        <Grid narrow>
          <Column sm={4} md={8} lg={8}>
            <Tile>
              <h3>System CPU Usage</h3>
              <div className="global-pipeline__progress">
                <ProgressBar
                  label="CPU"
                  value={stats?.cpuUsage ?? 0}
                  max={100}
                  helperText={`${stats?.cpuUsage ?? 0}% utilized`}
                />
              </div>
            </Tile>
          </Column>

          <Column sm={4} md={8} lg={8}>
            <Tile>
              <h3>System Memory Usage</h3>
              <div className="global-pipeline__progress">
                <ProgressBar
                  label="Memory"
                  value={stats?.memoryUsage ?? 0}
                  max={100}
                  helperText={`${stats?.memoryUsage ?? 0}% utilized`}
                />
              </div>
            </Tile>
          </Column>
        </Grid>
      </div>

      {/* Worker Health Dashboard */}
      <div className="global-pipeline__section">
        <h2>Worker Health Dashboard</h2>
        <DataTable rows={workerRows} headers={workerHeaders}>
          {({ rows, headers, getTableProps, getHeaderProps, getRowProps }) => (
            <TableContainer>
              <Table {...getTableProps()}>
                <TableHead>
                  <TableRow>
                    {headers.map((header) => (
                      <TableHeader {...getHeaderProps({ header })} key={header.key}>
                        {header.header}
                      </TableHeader>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {rows.map((row) => {
                    const originalRow = workerRows.find(r => r.id === row.id);
                    if (!originalRow) return null;
                    
                    return (
                      <TableRow {...getRowProps({ row })} key={row.id}>
                        <TableCell>{originalRow.workerId}</TableCell>
                        <TableCell>
                          <Tag type={getStatusColor(originalRow.status)}>
                            {originalRow.status}
                          </Tag>
                        </TableCell>
                        <TableCell>
                          <div className="global-pipeline__metric">
                            <span>{originalRow.cpuUsage}%</span>
                            <ProgressBar
                              label="CPU"
                              value={originalRow.cpuUsage}
                              max={100}
                              hideLabel
                            />
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="global-pipeline__metric">
                            <span>{originalRow.memoryUsage}%</span>
                            <ProgressBar
                              label="Memory"
                              value={originalRow.memoryUsage}
                              max={100}
                              hideLabel
                            />
                          </div>
                        </TableCell>
                        <TableCell>
                          {originalRow.currentJobs} / {workers.find(w => w.workerId === originalRow.workerId)?.maxCapacity ?? 0}
                        </TableCell>
                        <TableCell>{formatUptime(originalRow.uptime)}</TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </DataTable>
      </div>

      {/* Performance Metrics */}
      {metrics && metrics.cdcLatency && metrics.embeddingLatency && metrics.errorRates && (
        <div className="global-pipeline__section">
          <h2>Performance Metrics</h2>
          <Grid narrow>
            <Column sm={4} md={8} lg={5}>
              <Tile>
                <h3>CDC Latency</h3>
                <div className="global-pipeline__latency">
                  <div className="global-pipeline__latency-item">
                    <span className="label">P50:</span>
                    <span className="value">{metrics.cdcLatency.p50}ms</span>
                  </div>
                  <div className="global-pipeline__latency-item">
                    <span className="label">P95:</span>
                    <span className="value">{metrics.cdcLatency.p95}ms</span>
                  </div>
                  <div className="global-pipeline__latency-item">
                    <span className="label">P99:</span>
                    <span className="value">{metrics.cdcLatency.p99}ms</span>
                  </div>
                  <div className="global-pipeline__latency-item">
                    <span className="label">Avg:</span>
                    <span className="value">{metrics.cdcLatency.avg}ms</span>
                  </div>
                </div>
              </Tile>
            </Column>

            <Column sm={4} md={8} lg={5}>
              <Tile>
                <h3>Embedding Latency</h3>
                <div className="global-pipeline__latency">
                  <div className="global-pipeline__latency-item">
                    <span className="label">P50:</span>
                    <span className="value">{metrics.embeddingLatency.p50}ms</span>
                  </div>
                  <div className="global-pipeline__latency-item">
                    <span className="label">P95:</span>
                    <span className="value">{metrics.embeddingLatency.p95}ms</span>
                  </div>
                  <div className="global-pipeline__latency-item">
                    <span className="label">P99:</span>
                    <span className="value">{metrics.embeddingLatency.p99}ms</span>
                  </div>
                  <div className="global-pipeline__latency-item">
                    <span className="label">Avg:</span>
                    <span className="value">{metrics.embeddingLatency.avg}ms</span>
                  </div>
                </div>
              </Tile>
            </Column>

            <Column sm={4} md={8} lg={6}>
              <Tile>
                <h3>Error Rates</h3>
                <div className="global-pipeline__errors">
                  <div className="global-pipeline__error-item">
                    <span className="label">CDC Errors:</span>
                    <Tag type="red">{metrics.errorRates.cdcErrors}</Tag>
                  </div>
                  <div className="global-pipeline__error-item">
                    <span className="label">Embedding Errors:</span>
                    <Tag type="red">{metrics.errorRates.embeddingErrors}</Tag>
                  </div>
                  <div className="global-pipeline__error-item">
                    <span className="label">Index Errors:</span>
                    <Tag type="red">{metrics.errorRates.indexErrors}</Tag>
                  </div>
                  <div className="global-pipeline__error-item">
                    <span className="label">Error Rate:</span>
                    <Tag type={metrics.errorRates.errorRate > 5 ? 'red' : 'green'}>
                      {metrics.errorRates.errorRate.toFixed(2)}%
                    </Tag>
                  </div>
                </div>
              </Tile>
            </Column>
          </Grid>
        </div>
      )}
    </div>
  );
};
