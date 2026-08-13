import React, { useState, useEffect } from 'react';
import {
  DataTable,
  TableContainer,
  Table,
  TableHead,
  TableRow,
  TableHeader,
  TableBody,
  TableCell,
  TableToolbar,
  TableToolbarContent,
  TableToolbarSearch,
  Button,
  Tag,
  OverflowMenu,
  OverflowMenuItem,
  Loading,
  Tabs,
  TabList,
  Tab,
  TabPanels,
  TabPanel,
  Tile,
  Grid,
  Column,
} from '@carbon/react';
import { Renew, TrashCan, View, Checkmark, Error, WarningAlt, Time } from '@carbon/icons-react';
import { vectorSyncApi } from '../services/api';
import type { Job } from '../types';
import './JobQueue.scss';

export const JobQueue: React.FC = () => {
  const [loading, setLoading] = useState(true);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [searchValue, setSearchValue] = useState('');
  const [selectedTab, setSelectedTab] = useState(0);

  useEffect(() => {
    loadJobs();
    const interval = setInterval(loadJobs, 5000); // Refresh every 5 seconds
    return () => clearInterval(interval);
  }, []);

  const loadJobs = async () => {
    try {
      const jobsData = await vectorSyncApi.getJobs();
      setJobs(Array.isArray(jobsData) ? jobsData : []);
    } catch (err) {
      console.error('Failed to load jobs:', err);
      setJobs([]);
    } finally {
      setLoading(false);
    }
  };

  const handleRetry = async (jobId: string) => {
    try {
      await vectorSyncApi.retryJob(jobId);
      await loadJobs();
    } catch (err) {
      console.error('Failed to retry job:', err);
    }
  };

  const handleCancel = async (jobId: string) => {
    if (!confirm('Are you sure you want to cancel this job?')) return;
    try {
      await vectorSyncApi.cancelJob(jobId);
      await loadJobs();
    } catch (err) {
      console.error('Failed to cancel job:', err);
    }
  };

  const handleViewDetails = (jobId: string) => {
    // Navigate to job details or show modal
    console.log('View details for job:', jobId);
  };

  const headers = [
    { key: 'jobId', header: 'Job ID' },
    { key: 'tableName', header: 'Table' },
    { key: 'type', header: 'Type' },
    { key: 'status', header: 'Status' },
    { key: 'retries', header: 'Retries' },
    { key: 'createdAt', header: 'Created' },
    { key: 'actions', header: 'Actions' },
  ];

  const getStatusTag = (status: Job['status']) => {
    const types: Record<Job['status'], 'green' | 'blue' | 'red' | 'gray'> = {
      completed: 'green',
      running: 'blue',
      failed: 'red',
      queued: 'gray',
    };
    return <Tag type={types[status]} size="sm">{status}</Tag>;
  };

  const getTypeTag = (type: Job['type']) => {
    const types: Record<Job['type'], 'purple' | 'cyan' | 'teal'> = {
      embedding: 'purple',
      cdc: 'cyan',
      index: 'teal',
    };
    return <Tag type={types[type]} size="sm">{type}</Tag>;
  };

  const formatTimestamp = (timestamp: string): string => {
    const date = new Date(timestamp);
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const minutes = Math.floor(diff / 60000);
    if (minutes < 1) return 'Just now';
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    return date.toLocaleDateString();
  };

  // Filter jobs based on tab
  const filterByStatus = (status?: Job['status']) => {
    if (!status) return jobs;
    return jobs.filter(job => job.status === status);
  };

  const getFilteredJobs = () => {
    let filtered = jobs;
    
    // Filter by tab
    switch (selectedTab) {
      case 0: // All
        break;
      case 1: // Queued
        filtered = filterByStatus('queued');
        break;
      case 2: // Running
        filtered = filterByStatus('running');
        break;
      case 3: // Completed
        filtered = filterByStatus('completed');
        break;
      case 4: // Failed
        filtered = filterByStatus('failed');
        break;
    }

    // Filter by search
    if (searchValue) {
      filtered = filtered.filter(job =>
        job.jobId.toLowerCase().includes(searchValue.toLowerCase()) ||
        job.tableName.toLowerCase().includes(searchValue.toLowerCase())
      );
    }

    return filtered;
  };

  const filteredJobs = getFilteredJobs();

  const rows = filteredJobs.map(job => ({
    id: job.jobId,
    jobId: job.jobId,
    tableName: job.tableName,
    type: job.type,
    status: job.status,
    retries: job.retries,
    createdAt: job.createdAt,
    job,
  }));

  // Calculate statistics
  const stats = {
    total: jobs.length,
    queued: jobs.filter(j => j.status === 'queued').length,
    running: jobs.filter(j => j.status === 'running').length,
    completed: jobs.filter(j => j.status === 'completed').length,
    failed: jobs.filter(j => j.status === 'failed').length,
    successRate: jobs.length > 0
      ? ((jobs.filter(j => j.status === 'completed').length / jobs.length) * 100).toFixed(1)
      : '0.0',
  };

  if (loading) {
    return <Loading description="Loading jobs..." withOverlay={false} />;
  }

  return (
    <div className="job-queue">
      <div className="job-queue__header">
        <h1>Job Queue</h1>
        <p>Monitor and manage background jobs across all tables</p>
      </div>

      {/* Statistics Cards */}
      <Grid className="job-queue__stats" narrow>
        <Column sm={4} md={4} lg={3}>
          <Tile className="job-queue__stat-card">
            <div className="stat-icon">
              <Time size={24} />
            </div>
            <div className="stat-content">
              <h3>{stats.total}</h3>
              <p>Total Jobs</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={3}>
          <Tile className="job-queue__stat-card queued">
            <div className="stat-content">
              <h3>{stats.queued}</h3>
              <p>Queued</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={3}>
          <Tile className="job-queue__stat-card running">
            <div className="stat-content">
              <h3>{stats.running}</h3>
              <p>Running</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={3}>
          <Tile className="job-queue__stat-card completed">
            <div className="stat-content">
              <h3>{stats.completed}</h3>
              <p>Completed</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={3}>
          <Tile className="job-queue__stat-card failed">
            <div className="stat-content">
              <h3>{stats.failed}</h3>
              <p>Failed</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={3}>
          <Tile className="job-queue__stat-card success">
            <div className="stat-content">
              <h3>{stats.successRate}%</h3>
              <p>Success Rate</p>
            </div>
          </Tile>
        </Column>
      </Grid>

      {/* Tabs for filtering */}
      <Tabs selectedIndex={selectedTab} onChange={({ selectedIndex }) => setSelectedTab(selectedIndex)}>
        <TabList aria-label="Job status tabs">
          <Tab>All ({stats.total})</Tab>
          <Tab>Queued ({stats.queued})</Tab>
          <Tab>Running ({stats.running})</Tab>
          <Tab>Completed ({stats.completed})</Tab>
          <Tab>Failed ({stats.failed})</Tab>
        </TabList>
        <TabPanels>
          {[0, 1, 2, 3, 4].map(tabIndex => (
            <TabPanel key={tabIndex}>
              <DataTable rows={rows} headers={headers}>
                {({ rows, headers, getTableProps, getHeaderProps, getRowProps }) => (
                  <TableContainer title="Jobs">
                    <TableToolbar>
                      <TableToolbarContent>
                        <TableToolbarSearch
                          value={searchValue}
                          onChange={(e: any) => setSearchValue(e.target.value)}
                          placeholder="Search jobs..."
                        />
                      </TableToolbarContent>
                    </TableToolbar>
                    <Table {...getTableProps()}>
                      <TableHead>
                        <TableRow>
                          {headers.map(header => (
                            <TableHeader {...getHeaderProps({ header })} key={header.key}>
                              {header.header}
                            </TableHeader>
                          ))}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {rows.length === 0 ? (
                          <TableRow>
                            <TableCell colSpan={headers.length}>
                              <div className="empty-state">
                                <p>No jobs found</p>
                              </div>
                            </TableCell>
                          </TableRow>
                        ) : (
                          rows.map(row => {
                            const originalRow = filteredJobs.find(j => j.jobId === row.id);
                            if (!originalRow) return null;

                            return (
                              <TableRow {...getRowProps({ row })} key={row.id}>
                                <TableCell>
                                  <code className="job-id">{originalRow.jobId}</code>
                                </TableCell>
                                <TableCell>{originalRow.tableName}</TableCell>
                                <TableCell>{getTypeTag(originalRow.type)}</TableCell>
                                <TableCell>{getStatusTag(originalRow.status)}</TableCell>
                                <TableCell>
                                  {originalRow.retries > 0 ? (
                                    <span className="retry-count">
                                      <WarningAlt size={16} />
                                      {originalRow.retries}
                                    </span>
                                  ) : (
                                    <span>-</span>
                                  )}
                                </TableCell>
                                <TableCell>{formatTimestamp(originalRow.createdAt)}</TableCell>
                                <TableCell>
                                  <OverflowMenu size="sm" flipped>
                                    <OverflowMenuItem
                                      itemText="View Details"
                                      onClick={() => handleViewDetails(originalRow.jobId)}
                                    />
                                    {originalRow.status === 'failed' && (
                                      <OverflowMenuItem
                                        itemText="Retry"
                                        onClick={() => handleRetry(originalRow.jobId)}
                                      />
                                    )}
                                    {(originalRow.status === 'queued' || originalRow.status === 'running') && (
                                      <OverflowMenuItem
                                        itemText="Cancel"
                                        isDelete
                                        onClick={() => handleCancel(originalRow.jobId)}
                                      />
                                    )}
                                  </OverflowMenu>
                                </TableCell>
                              </TableRow>
                            );
                          })
                        )}
                      </TableBody>
                    </Table>
                  </TableContainer>
                )}
              </DataTable>
            </TabPanel>
          ))}
        </TabPanels>
      </Tabs>
    </div>
  );
};
