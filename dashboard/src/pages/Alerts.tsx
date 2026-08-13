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
  InlineNotification,
} from '@carbon/react';
import { 
  Checkmark, 
  Close, 
  View, 
  WarningAlt, 
  ErrorFilled, 
  InformationFilled,
  CheckmarkFilled 
} from '@carbon/icons-react';
import { vectorSyncApi } from '../services/api';
import type { Alert } from '../types';
import './Alerts.scss';

export const Alerts: React.FC = () => {
  const [loading, setLoading] = useState(true);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [selectedTab, setSelectedTab] = useState(0);
  const [notification, setNotification] = useState<{
    kind: 'success' | 'error' | 'info';
    title: string;
    subtitle: string;
  } | null>(null);

  useEffect(() => {
    loadAlerts();
    const interval = setInterval(loadAlerts, 10000); // Refresh every 10 seconds
    return () => clearInterval(interval);
  }, []);

  const loadAlerts = async () => {
    try {
      const alertsData = await vectorSyncApi.getAlerts();
      setAlerts(Array.isArray(alertsData) ? alertsData : []);
    } catch (err) {
      console.error('Failed to load alerts:', err);
      setAlerts([]);
    } finally {
      setLoading(false);
    }
  };

  const handleAcknowledge = async (alertId: string) => {
    try {
      await vectorSyncApi.acknowledgeAlert(alertId);
      setAlerts(prev => prev.map(alert =>
        alert.id === alertId ? { ...alert, acknowledged: true } : alert
      ));
      showNotification('success', 'Alert Acknowledged', 'The alert has been marked as acknowledged.');
    } catch (err) {
      console.error('Failed to acknowledge alert:', err);
      showNotification('error', 'Error', 'Failed to acknowledge alert.');
    }
  };

  const handleDismiss = async (alertId: string) => {
    try {
      // In a real app, this would call an API to dismiss/delete the alert
      setAlerts(prev => prev.filter(alert => alert.id !== alertId));
      showNotification('success', 'Alert Dismissed', 'The alert has been removed.');
    } catch (err) {
      console.error('Failed to dismiss alert:', err);
      showNotification('error', 'Error', 'Failed to dismiss alert.');
    }
  };

  const handleViewRelated = (alertId: string) => {
    const alert = alerts.find(a => a.id === alertId);
    if (alert?.tableId) {
      window.location.href = `/tables/${alert.tableId}`;
    }
  };

  const showNotification = (kind: 'success' | 'error' | 'info', title: string, subtitle: string) => {
    setNotification({ kind, title, subtitle });
    setTimeout(() => setNotification(null), 5000);
  };

  const headers = [
    { key: 'severity', header: 'Severity' },
    { key: 'message', header: 'Message' },
    { key: 'table', header: 'Table' },
    { key: 'timestamp', header: 'Time' },
    { key: 'status', header: 'Status' },
    { key: 'actions', header: 'Actions' },
  ];

  const getSeverityIcon = (severity: Alert['severity']) => {
    switch (severity) {
      case 'critical':
        return <ErrorFilled size={20} className="severity-icon critical" />;
      case 'warning':
        return <WarningAlt size={20} className="severity-icon warning" />;
      case 'info':
        return <InformationFilled size={20} className="severity-icon info" />;
    }
  };

  const getSeverityTag = (severity: Alert['severity']) => {
    const types: Record<Alert['severity'], 'red' | 'warm-gray' | 'blue'> = {
      critical: 'red',
      warning: 'warm-gray',
      info: 'blue',
    };
    return <Tag type={types[severity]} size="sm">{severity}</Tag>;
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
    const days = Math.floor(hours / 24);
    if (days < 7) return `${days}d ago`;
    return date.toLocaleDateString();
  };

  // Filter alerts based on tab
  const getFilteredAlerts = () => {
    let filtered = alerts;
    
    switch (selectedTab) {
      case 0: // All
        break;
      case 1: // Unacknowledged
        filtered = alerts.filter(alert => !alert.acknowledged);
        break;
      case 2: // Critical
        filtered = alerts.filter(alert => alert.severity === 'critical');
        break;
      case 3: // Warning
        filtered = alerts.filter(alert => alert.severity === 'warning');
        break;
      case 4: // Info
        filtered = alerts.filter(alert => alert.severity === 'info');
        break;
    }

    return filtered;
  };

  const filteredAlerts = getFilteredAlerts();

  const rows = filteredAlerts.map(alert => ({
    id: alert.id,
    severity: alert.severity,
    message: alert.message,
    table: alert.tableId || '-',
    timestamp: alert.timestamp,
    status: alert.acknowledged,
    alert,
  }));

  // Calculate statistics
  const stats = {
    total: alerts.length,
    unacknowledged: alerts.filter(a => !a.acknowledged).length,
    critical: alerts.filter(a => a.severity === 'critical').length,
    warning: alerts.filter(a => a.severity === 'warning').length,
    info: alerts.filter(a => a.severity === 'info').length,
  };

  if (loading) {
    return <Loading description="Loading alerts..." withOverlay={false} />;
  }

  return (
    <div className="alerts">
      {notification && (
        <div className="alerts__notification">
          <InlineNotification
            kind={notification.kind}
            title={notification.title}
            subtitle={notification.subtitle}
            onClose={() => setNotification(null)}
            hideCloseButton={false}
          />
        </div>
      )}

      <div className="alerts__header">
        <h1>Alerts</h1>
        <p>Monitor system alerts and notifications</p>
      </div>

      {/* Statistics Cards */}
      <Grid className="alerts__stats" narrow>
        <Column sm={4} md={4} lg={3}>
          <Tile className="alerts__stat-card">
            <div className="stat-content">
              <h3>{stats.total}</h3>
              <p>Total Alerts</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={3}>
          <Tile className="alerts__stat-card unacknowledged">
            <div className="stat-icon">
              <WarningAlt size={24} />
            </div>
            <div className="stat-content">
              <h3>{stats.unacknowledged}</h3>
              <p>Unacknowledged</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={2}>
          <Tile className="alerts__stat-card critical">
            <div className="stat-content">
              <h3>{stats.critical}</h3>
              <p>Critical</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={2}>
          <Tile className="alerts__stat-card warning">
            <div className="stat-content">
              <h3>{stats.warning}</h3>
              <p>Warning</p>
            </div>
          </Tile>
        </Column>

        <Column sm={4} md={4} lg={2}>
          <Tile className="alerts__stat-card info">
            <div className="stat-content">
              <h3>{stats.info}</h3>
              <p>Info</p>
            </div>
          </Tile>
        </Column>
      </Grid>

      {/* Tabs for filtering */}
      <Tabs selectedIndex={selectedTab} onChange={({ selectedIndex }) => setSelectedTab(selectedIndex)}>
        <TabList aria-label="Alert filter tabs">
          <Tab>All ({stats.total})</Tab>
          <Tab>Unacknowledged ({stats.unacknowledged})</Tab>
          <Tab>Critical ({stats.critical})</Tab>
          <Tab>Warning ({stats.warning})</Tab>
          <Tab>Info ({stats.info})</Tab>
        </TabList>
        <TabPanels>
          {[0, 1, 2, 3, 4].map(tabIndex => (
            <TabPanel key={tabIndex}>
              <DataTable rows={rows} headers={headers}>
                {({ rows, headers, getTableProps, getHeaderProps, getRowProps }) => (
                  <TableContainer title="Alerts">
                    <TableToolbar>
                      <TableToolbarContent>
                        {stats.unacknowledged > 0 && (
                          <Button
                            kind="primary"
                            size="sm"
                            renderIcon={CheckmarkFilled}
                            onClick={() => {
                              // Acknowledge all unacknowledged alerts
                              alerts
                                .filter(a => !a.acknowledged)
                                .forEach(a => handleAcknowledge(a.id));
                            }}
                          >
                            Acknowledge All
                          </Button>
                        )}
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
                                <CheckmarkFilled size={48} className="empty-icon" />
                                <p>No alerts to display</p>
                                <span>All systems are operating normally</span>
                              </div>
                            </TableCell>
                          </TableRow>
                        ) : (
                          rows.map(row => {
                            const originalAlert = filteredAlerts.find(a => a.id === row.id);
                            if (!originalAlert) return null;

                            return (
                              <TableRow 
                                {...getRowProps({ row })} 
                                key={row.id}
                                className={originalAlert.acknowledged ? 'acknowledged' : 'unacknowledged'}
                              >
                                <TableCell>
                                  <div className="severity-cell">
                                    {getSeverityIcon(originalAlert.severity)}
                                    {getSeverityTag(originalAlert.severity)}
                                  </div>
                                </TableCell>
                                <TableCell>
                                  <div className="message-cell">
                                    {originalAlert.message}
                                  </div>
                                </TableCell>
                                <TableCell>
                                  {originalAlert.tableId ? (
                                    <Button
                                      kind="ghost"
                                      size="sm"
                                      onClick={() => handleViewRelated(originalAlert.id)}
                                    >
                                      {originalAlert.tableId}
                                    </Button>
                                  ) : (
                                    <span>-</span>
                                  )}
                                </TableCell>
                                <TableCell>{formatTimestamp(originalAlert.timestamp)}</TableCell>
                                <TableCell>
                                  {originalAlert.acknowledged ? (
                                    <Tag type="green" size="sm" renderIcon={Checkmark}>
                                      Acknowledged
                                    </Tag>
                                  ) : (
                                    <Tag type="gray" size="sm">
                                      New
                                    </Tag>
                                  )}
                                </TableCell>
                                <TableCell>
                                  <OverflowMenu size="sm" flipped>
                                    {!originalAlert.acknowledged && (
                                      <OverflowMenuItem
                                        itemText="Acknowledge"
                                        onClick={() => handleAcknowledge(originalAlert.id)}
                                      />
                                    )}
                                    {originalAlert.tableId && (
                                      <OverflowMenuItem
                                        itemText="View Related Table"
                                        onClick={() => handleViewRelated(originalAlert.id)}
                                      />
                                    )}
                                    <OverflowMenuItem
                                      itemText="Dismiss"
                                      isDelete
                                      onClick={() => handleDismiss(originalAlert.id)}
                                    />
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
