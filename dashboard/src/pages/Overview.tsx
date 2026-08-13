import { useState, useEffect } from 'react';
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
  Modal,
  TextInput,
  Dropdown,
  MultiSelect,
  Toggle,
  ComboBox,
} from '@carbon/react';
import { Renew, TrashCan, View, WarningAlt, Checkmark, Error, Add } from '@carbon/icons-react';
import { vectorSyncApi } from '../services/api';
import type { TableConfig, TableHealth, HealthStatus } from '../types';
import './Overview.scss';

interface TableRow {
  id: string;
  table: string;
  status: HealthStatus;
  lag: string;
  pending: number;
  indexStatus: string;
  lastSync: string;
  config: TableConfig;
  health: TableHealth;
}

const headers = [
  { key: 'table', header: 'Table' },
  { key: 'status', header: 'Status' },
  { key: 'lag', header: 'Lag' },
  { key: 'pending', header: 'Pending Rows' },
  { key: 'indexStatus', header: 'Index' },
  { key: 'lastSync', header: 'Last Sync' },
  { key: 'actions', header: 'Actions' },
];

const getHealthIcon = (health: HealthStatus) => {
  switch (health) {
    case 'healthy':
      return <Checkmark size={16} className="health-icon healthy" />;
    case 'lagging':
      return <WarningAlt size={16} className="health-icon lagging" />;
    case 'broken':
      return <Error size={16} className="health-icon broken" />;
    default:
      return null;
  }
};

const getHealthTag = (health: HealthStatus) => {
  const types: Record<HealthStatus, 'green' | 'warm-gray' | 'red' | 'gray'> = {
    healthy: 'green',
    lagging: 'warm-gray',
    broken: 'red',
    unknown: 'gray',
  };
  return <Tag type={types[health]} size="sm">{health}</Tag>;
};

const formatLag = (seconds: number): string => {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h`;
};

const formatTimestamp = (timestamp: string | null): string => {
  if (!timestamp) return 'Never';
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

export function Overview() {
  const [tables, setTables] = useState<TableConfig[]>([]);
  const [healthData, setHealthData] = useState<TableHealth[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchValue, setSearchValue] = useState('');
  const [filterStatus, setFilterStatus] = useState<HealthStatus | 'all'>('all');
  const [showRegisterModal, setShowRegisterModal] = useState(false);
  const [registerForm, setRegisterForm] = useState({
    catalog: 'iceberg',
    tableName: '',
    embeddingColumns: [] as string[],
    vectorColumn: '',
    textColumn: '',
    primaryKey: '',
    modelName: 'sentence-transformers/all-MiniLM-L6-v2',
    enabled: true,
  });
  const [registering, setRegistering] = useState(false);
  const [discoveredTables, setDiscoveredTables] = useState<any[]>([]);
  const [selectedTableUuid, setSelectedTableUuid] = useState<string>('');
  const [availableColumns, setAvailableColumns] = useState<Array<{ id: string; label: string }>>([]);
  const [loadingTables, setLoadingTables] = useState(false);

  const loadData = async () => {
    try {
      const [tablesData, healthDataResponse] = await Promise.all([
        vectorSyncApi.getTables(),
        vectorSyncApi.getAllTableHealth(),
      ]);
      // Ensure we always have arrays
      setTables(Array.isArray(tablesData) ? tablesData : []);
      setHealthData(Array.isArray(healthDataResponse) ? healthDataResponse : []);
    } catch (error) {
      console.error('Error loading fleet data:', error);
      // Set empty arrays on error
      setTables([]);
      setHealthData([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadData();
    const interval = setInterval(() => void loadData(), 30000);
    return () => clearInterval(interval);
  }, []);

  // Load discovered tables when modal opens
  useEffect(() => {
    if (showRegisterModal) {
      loadDiscoveredTables();
    }
  }, [showRegisterModal]);

  const loadDiscoveredTables = async () => {
    setLoadingTables(true);
    try {
      console.log('Loading discovered tables...');
      const tables = await vectorSyncApi.getDiscoveredTables(false);
      console.log('Discovered tables loaded:', tables);
      
      if (!Array.isArray(tables)) {
        console.error('Discovered tables is not an array:', tables);
        setDiscoveredTables([]);
      } else {
        setDiscoveredTables(tables);
      }
    } catch (error) {
      console.error('Error loading discovered tables:', error);
      setDiscoveredTables([]);
    } finally {
      setLoadingTables(false);
    }
  };

  const handleTableSelection = async (selectedItem: any) => {
    console.log('Table selected:', selectedItem);
    
    if (!selectedItem) {
      setRegisterForm(prev => ({ ...prev, tableName: '' }));
      setSelectedTableUuid('');
      setAvailableColumns([]);
      return;
    }

    setRegisterForm(prev => ({ ...prev, tableName: selectedItem.tableName }));
    setSelectedTableUuid(selectedItem.uuid);

    // Fetch table details to get schema
    try {
      console.log('Fetching table details for UUID:', selectedItem.uuid);
      const tableDetails = await vectorSyncApi.getDiscoveredTable(selectedItem.uuid);
      console.log('Table details:', tableDetails);
      
      // Parse schema JSON to extract column names
      if (tableDetails.schemaJson) {
        const schema = JSON.parse(tableDetails.schemaJson);
        console.log('Parsed schema:', schema);
        const columns = schema.fields?.map((field: any) => ({
          id: field.name,
          label: `${field.name} (${field.type})`,
        })) || [];
        console.log('Available columns:', columns);
        setAvailableColumns(columns);
      } else {
        console.warn('No schemaJson found in table details');
        setAvailableColumns([]);
      }
    } catch (error) {
      console.error('Error fetching table schema:', error);
      setAvailableColumns([]);
    }
  };

  const handleSync = async (tableId: string) => {
    try {
      await vectorSyncApi.triggerSync(tableId);
      setTimeout(loadData, 2000);
    } catch (error) {
      console.error('Error triggering sync:', error);
    }
  };

  const handleDelete = async (tableId: string) => {
    if (!confirm('Are you sure you want to delete this table?')) return;
    try {
      await vectorSyncApi.deleteTable(tableId);
      setTables(prev => prev.filter(t => t.tableId !== tableId));
    } catch (error) {
      console.error('Error deleting table:', error);
    }
  };

  const handleViewDetails = (tableId: string) => {
    window.location.href = `/tables/${tableId}`;
  };

  const handleRegisterTable = async () => {
    if (!registerForm.catalog || !registerForm.tableName || registerForm.embeddingColumns.length === 0 || !registerForm.primaryKey) {
      alert('Please fill in all required fields (catalog, table name, embedding columns, and primary key)');
      return;
    }

    setRegistering(true);
    try {
      await vectorSyncApi.registerTable({
        catalog: registerForm.catalog,
        tableName: registerForm.tableName,
        embeddingColumns: registerForm.embeddingColumns,
        vectorColumn: registerForm.vectorColumn || `${registerForm.embeddingColumns[0]}_vector`,
        textColumn: registerForm.textColumn || registerForm.embeddingColumns[0],
        primaryKey: registerForm.primaryKey,
        modelName: registerForm.modelName,
        enabled: registerForm.enabled,
        syncEnabled: true,
      });
      
      setShowRegisterModal(false);
      setRegisterForm({
        catalog: 'iceberg',
        tableName: '',
        embeddingColumns: [],
        vectorColumn: '',
        textColumn: '',
        primaryKey: '',
        modelName: 'sentence-transformers/all-MiniLM-L6-v2',
        enabled: true,
      });
      setSelectedTableUuid('');
      setAvailableColumns([]);
      
      // Reload tables
      await loadData();
    } catch (error) {
      console.error('Error registering table:', error);
      alert('Failed to register table. Please try again.');
    } finally {
      setRegistering(false);
    }
  };

  const modelOptions = [
    { id: 'sentence-transformers/all-MiniLM-L6-v2', label: 'all-MiniLM-L6-v2 (384d)' },
    { id: 'sentence-transformers/all-mpnet-base-v2', label: 'all-mpnet-base-v2 (768d)' },
    { id: 'text-embedding-ada-002', label: 'OpenAI Ada-002 (1536d)' },
    { id: 'text-embedding-3-small', label: 'OpenAI Embedding-3-Small (1536d)' },
  ];

  // Format discovered tables for ComboBox
  const tableComboBoxItems = (Array.isArray(discoveredTables) ? discoveredTables : []).map(table => ({
    id: table.uuid,
    label: `${table.schemaName}.${table.tableName}`,
    tableName: table.tableName,
    uuid: table.uuid,
  }));

  // Combine tables and health data - with safety check
  const rows: TableRow[] = (Array.isArray(tables) ? tables : []).map(table => {
    const health = healthData.find(h => h.tableId === table.tableId) || {
      tableId: table.tableId,
      health: 'unknown' as HealthStatus,
      lag: 0,
      pendingRows: 0,
      indexStatus: 'unknown' as const,
      lastSyncTime: null,
    };

    return {
      id: table.tableId,
      table: `${table.catalog}.${table.tableName}`,
      status: health.health,
      lag: formatLag(health.lag),
      pending: health.pendingRows,
      indexStatus: health.indexStatus,
      lastSync: formatTimestamp(health.lastSyncTime),
      config: table,
      health,
    };
  });

  // Filter rows
  const filteredRows = rows.filter(row => {
    const matchesSearch = row.table.toLowerCase().includes(searchValue.toLowerCase());
    const matchesFilter = filterStatus === 'all' || row.status === filterStatus;
    return matchesSearch && matchesFilter;
  });

  // Sort by health (broken first, then lagging, then healthy)
  const sortedRows = [...filteredRows].sort((a, b) => {
    const healthOrder = { broken: 0, lagging: 1, healthy: 2, unknown: 3 };
    return healthOrder[a.status] - healthOrder[b.status];
  });

  if (loading) {
    return <Loading description="Loading fleet data..." withOverlay={false} />;
  }

  return (
    <div className="overview">
      <div className="page-header">
        <h1>Overview</h1>
        <p>Monitor all vectorized tables across your data lakehouse</p>
      </div>

      <div className="fleet-stats">
        <div className="stat-card">
          <div className="stat-value">{tables.length}</div>
          <div className="stat-label">Total Tables</div>
        </div>
        <div className="stat-card healthy">
          <div className="stat-value">
            {healthData.filter(h => h.health === 'healthy').length}
          </div>
          <div className="stat-label">Healthy</div>
        </div>
        <div className="stat-card lagging">
          <div className="stat-value">
            {healthData.filter(h => h.health === 'lagging').length}
          </div>
          <div className="stat-label">Lagging</div>
        </div>
        <div className="stat-card broken">
          <div className="stat-value">
            {healthData.filter(h => h.health === 'broken').length}
          </div>
          <div className="stat-label">Broken</div>
        </div>
      </div>

      <DataTable rows={sortedRows} headers={headers}>
        {({ rows, headers, getTableProps, getHeaderProps, getRowProps }) => (
          <TableContainer title="Vectorized Tables">
            <TableToolbar>
              <TableToolbarContent>
                <TableToolbarSearch
                  value={searchValue}
                  onChange={(e: any) => setSearchValue(e.target.value)}
                  placeholder="Search tables..."
                />
                <Button
                  kind="primary"
                  renderIcon={Add}
                  onClick={() => setShowRegisterModal(true)}
                >
                  Register Table
                </Button>
                <div className="filter-buttons">
                  <Button
                    kind={filterStatus === 'all' ? 'primary' : 'ghost'}
                    size="sm"
                    onClick={() => setFilterStatus('all')}
                  >
                    All
                  </Button>
                  <Button
                    kind={filterStatus === 'broken' ? 'danger' : 'ghost'}
                    size="sm"
                    onClick={() => setFilterStatus('broken')}
                  >
                    Broken
                  </Button>
                  <Button
                    kind={filterStatus === 'lagging' ? 'tertiary' : 'ghost'}
                    size="sm"
                    onClick={() => setFilterStatus('lagging')}
                  >
                    Lagging
                  </Button>
                </div>
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
                {rows.map(row => {
                  const originalRow = sortedRows.find(r => r.id === row.id)!;
                  return (
                    <TableRow {...getRowProps({ row })} key={row.id}>
                      <TableCell>
                        <div className="table-cell-with-icon">
                          {getHealthIcon(originalRow.status)}
                          <span className="table-name">{originalRow.table}</span>
                        </div>
                      </TableCell>
                      <TableCell>{getHealthTag(originalRow.status)}</TableCell>
                      <TableCell>
                        <span className={originalRow.health.lag > 600 ? 'lag-warning' : ''}>
                          {originalRow.lag}
                        </span>
                      </TableCell>
                      <TableCell>
                        <span className={originalRow.pending > 1000 ? 'pending-warning' : ''}>
                          {originalRow.pending.toLocaleString()}
                        </span>
                      </TableCell>
                      <TableCell>
                        <Tag
                          type={originalRow.indexStatus === 'fresh' ? 'green' : 'warm-gray'}
                          size="sm"
                        >
                          {originalRow.indexStatus}
                        </Tag>
                      </TableCell>
                      <TableCell>{originalRow.lastSync}</TableCell>
                      <TableCell>
                        <OverflowMenu size="sm" flipped>
                          <OverflowMenuItem
                            itemText="View Details"
                            onClick={() => handleViewDetails(originalRow.id)}
                          />
                          <OverflowMenuItem
                            itemText="Trigger Sync"
                            onClick={() => handleSync(originalRow.id)}
                          />
                          <OverflowMenuItem
                            itemText="Delete"
                            isDelete
                            onClick={() => handleDelete(originalRow.id)}
                          />
                        </OverflowMenu>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </DataTable>

      <Modal
        open={showRegisterModal}
        onRequestClose={() => setShowRegisterModal(false)}
        onRequestSubmit={handleRegisterTable}
        modalHeading="Register Table for Vectorization"
        primaryButtonText={registering ? 'Registering...' : 'Register'}
        secondaryButtonText="Cancel"
        primaryButtonDisabled={registering}
        size="md"
      >
        <div className="register-form">
          <TextInput
            id="catalog-input"
            labelText="Catalog *"
            value="iceberg"
            disabled={true}
            helperText="Catalog is locked to Iceberg"
          />

          <ComboBox
            id="table-name-combo"
            titleText="Table Name *"
            placeholder={loadingTables ? "Loading tables..." : "Type to filter tables..."}
            items={tableComboBoxItems}
            selectedItem={tableComboBoxItems.find(t => t.uuid === selectedTableUuid)}
            onChange={({ selectedItem }) => handleTableSelection(selectedItem)}
            disabled={registering || loadingTables}
            helperText="Select from discovered tables - type to filter"
            shouldFilterItem={({ item, inputValue }) => {
              if (!inputValue) return true;
              return item.label.toLowerCase().includes(inputValue.toLowerCase());
            }}
            itemToString={(item) => (item ? item.label : '')}
            size="lg"
          />

          <MultiSelect
            id="embedding-columns"
            titleText="Columns to Embed *"
            label="Select columns"
            items={availableColumns}
            selectedItems={availableColumns.filter(c => registerForm.embeddingColumns.includes(c.id))}
            onChange={({ selectedItems }) =>
              setRegisterForm(prev => ({
                ...prev,
                embeddingColumns: (selectedItems ?? []).map(item => item.id),
              }))
            }
            disabled={registering || availableColumns.length === 0}
            helperText={availableColumns.length === 0 ? "Select a table first to see available columns" : ""}
          />

          <Dropdown
            id="primary-key"
            titleText="Primary Key Column *"
            label="Select primary key"
            items={availableColumns}
            selectedItem={availableColumns.find(c => c.id === registerForm.primaryKey)}
            onChange={({ selectedItem }) =>
              setRegisterForm(prev => ({ ...prev, primaryKey: selectedItem?.id || '' }))
            }
            disabled={registering || availableColumns.length === 0}
            helperText={availableColumns.length === 0 ? "Select a table first to see available columns" : "Column used as unique identifier"}
          />

          <TextInput
            id="vector-column"
            labelText="Vector Column Name (optional)"
            placeholder="Auto-generated if empty"
            value={registerForm.vectorColumn}
            onChange={(e) => setRegisterForm(prev => ({ ...prev, vectorColumn: e.target.value }))}
            disabled={registering}
            helperText="Column where vectors will be stored"
          />

          <TextInput
            id="text-column"
            labelText="Primary Text Column (optional)"
            placeholder="First embedding column if empty"
            value={registerForm.textColumn}
            onChange={(e) => setRegisterForm(prev => ({ ...prev, textColumn: e.target.value }))}
            disabled={registering}
            helperText="Main text column for search display"
          />

          <Dropdown
            id="model-dropdown"
            titleText="Embedding Model"
            label="Select model"
            items={modelOptions}
            selectedItem={modelOptions.find(m => m.id === registerForm.modelName)}
            onChange={({ selectedItem }) =>
              setRegisterForm(prev => ({ ...prev, modelName: selectedItem?.id || '' }))
            }
            disabled={registering}
          />

          <Toggle
            id="enabled-toggle"
            labelText="Enable Vectorization"
            labelA="Disabled"
            labelB="Enabled"
            toggled={registerForm.enabled}
            onToggle={(checked) => setRegisterForm(prev => ({ ...prev, enabled: checked }))}
            disabled={registering}
          />
        </div>
      </Modal>
    </div>
  );
}
