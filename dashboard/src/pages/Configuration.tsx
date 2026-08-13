import React, { useState, useEffect } from 'react';
import {
  Tabs,
  TabList,
  Tab,
  TabPanels,
  TabPanel,
  TextInput,
  NumberInput,
  Button,
  Dropdown,
  Toggle,
  DataTable,
  TableContainer,
  Table,
  TableHead,
  TableRow,
  TableHeader,
  TableBody,
  TableCell,
  Tag,
  Loading,
  InlineNotification,
  Accordion,
  AccordionItem,
  PasswordInput,
  Modal,
  TextArea,
  ProgressBar,
  Tile,
  Section,
} from '@carbon/react';
import { Save, Reset, Add, Edit, Renew, Catalog } from '@carbon/icons-react';
import { vectorSyncApi } from '../services/api';
import type { 
  SystemConfig, 
  TableConfig, 
  ModelInfo, 
  StorageConfig, 
  IndexConfig 
} from '../types';
import './Configuration.scss';

export const Configuration: React.FC = () => {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  
  // Table Sync State
  const [isSyncing, setIsSyncing] = useState(false);
  const [syncJobId, setSyncJobId] = useState<string | null>(null);
  const [syncProgress, setSyncProgress] = useState<{
    status: string;
    tablesDiscovered: number;
    tablesRegistered: number;
    message?: string;
  } | null>(null);
  
  // Add Custom Model Modal
  const [isModelModalOpen, setIsModelModalOpen] = useState(false);
  const [newModel, setNewModel] = useState({
    id: '',
    name: '',
    dimension: 384,
    maxTokens: 512,
    provider: 'custom' as 'openai' | 'cohere' | 'local' | 'custom',
    endpoint: '',
    apiKey: '',
  });

  const providerOptions = [
    { id: 'openai', label: 'OpenAI' },
    { id: 'cohere', label: 'Cohere' },
    { id: 'custom', label: 'Custom API' },
    { id: 'local', label: 'Local Model' },
  ];

  const localModelOptions = [
    {
      id: 'sentence-transformers/all-MiniLM-L6-v2',
      label: 'all-MiniLM-L6-v2 (384d)',
      name: 'all-MiniLM-L6-v2',
      dimension: 384,
      maxTokens: 256,
    },
    {
      id: 'sentence-transformers/all-mpnet-base-v2',
      label: 'all-mpnet-base-v2 (768d)',
      name: 'all-mpnet-base-v2',
      dimension: 768,
      maxTokens: 384,
    },
  ];

  const selectedLocalModel = localModelOptions.find((model) => model.id === newModel.id);

  // System Config
  const [systemConfig, setSystemConfig] = useState<SystemConfig>({
    embeddingService: {
      endpoint: 'http://localhost:8000',
      timeout: 30000,
      retryAttempts: 3,
    },
    defaultModel: 'sentence-transformers/all-MiniLM-L6-v2',
    workerPool: {
      size: 4,
      maxConcurrent: 10,
    },
    syncFrequency: 300,
    retryPolicy: {
      maxRetries: 3,
      backoffMultiplier: 2,
      initialDelay: 1000,
    },
  });

  // Tables
  const [tables, setTables] = useState<TableConfig[]>([]);

  // Models
  const [models, setModels] = useState<ModelInfo[]>([
    {
      id: 'sentence-transformers/all-MiniLM-L6-v2',
      name: 'all-MiniLM-L6-v2',
      dimension: 384,
      maxTokens: 256,
      provider: 'local',
      performance: { avgLatency: 50, throughput: 1000 },
    },
    {
      id: 'sentence-transformers/all-mpnet-base-v2',
      name: 'all-mpnet-base-v2',
      dimension: 768,
      maxTokens: 384,
      provider: 'local',
      performance: { avgLatency: 80, throughput: 800 },
    },
    {
      id: 'text-embedding-ada-002',
      name: 'OpenAI Ada-002',
      dimension: 1536,
      maxTokens: 8191,
      provider: 'openai',
      performance: { avgLatency: 200, throughput: 500 },
    },
  ]);

  // Storage Config
  const [storageConfig, setStorageConfig] = useState<StorageConfig>({
    iceberg: {
      catalogType: 'rest',
      uri: 'http://localhost:8181',
      warehouse: 's3://warehouse',
    },
    s3: {
      endpoint: 'http://localhost:9000',
      accessKey: '',
      secretKey: '',
      bucket: 'vectorsync',
      region: 'us-east-1',
    },
    database: {
      host: 'localhost',
      port: 5432,
      database: 'vectorsync',
      username: 'postgres',
    },
  });

  // Index Config
  const [indexConfig, setIndexConfig] = useState<IndexConfig>({
    hnsw: {
      m: 16,
      efConstruction: 200,
      efSearch: 50,
    },
    cacheSize: 1024,
    rebuildThreshold: 10,
  });

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      const tablesData = await vectorSyncApi.getTables();
      setTables(Array.isArray(tablesData) ? tablesData : []);
    } catch (err) {
      console.error('Failed to load data:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleSaveSystemConfig = async () => {
    setSaving(true);
    setError(null);
    setSuccess(null);

    try {
      // Mock API call
      await new Promise(resolve => setTimeout(resolve, 1000));
      setSuccess('System configuration saved successfully');
    } catch (err: any) {
      setError(err.message || 'Failed to save configuration');
    } finally {
      setSaving(false);
    }
  };

  const handleSaveStorageConfig = async () => {
    setSaving(true);
    setError(null);
    setSuccess(null);

    try {
      await new Promise(resolve => setTimeout(resolve, 1000));
      setSuccess('Storage configuration saved successfully');
    } catch (err: any) {
      setError(err.message || 'Failed to save configuration');
    } finally {
      setSaving(false);
    }
  };

  const handleSaveIndexConfig = async () => {
    setSaving(true);
    setError(null);
    setSuccess(null);

    try {
      await new Promise(resolve => setTimeout(resolve, 1000));
      setSuccess('Index configuration saved successfully');
    } catch (err: any) {
      setError(err.message || 'Failed to save configuration');
    } finally {
      setSaving(false);
    }
  };

  const handleToggleTableSync = async (tableId: string, enabled: boolean) => {
    try {
      // Mock API call
      await new Promise(resolve => setTimeout(resolve, 500));
      setTables(prev =>
        prev.map(t => (t.tableId === tableId ? { ...t, syncEnabled: enabled } : t))
      );
      setSuccess(`Sync ${enabled ? 'enabled' : 'disabled'} for table`);
    } catch (err: any) {
      setError(err.message || 'Failed to update table');
    }
  };

  const handleAddModel = async () => {
    setSaving(true);
    setError(null);
    
    try {
      // Validation
      if (newModel.provider === 'local') {
        if (!selectedLocalModel) {
          throw new Error('Select a local model');
        }
      } else {
        if (!newModel.name || !newModel.id) {
          throw new Error('Model name and ID are required');
        }
        if (!newModel.apiKey) {
          throw new Error('API key is required for external providers');
        }
        if (!newModel.endpoint) {
          throw new Error('Endpoint is required for external providers');
        }
      }

      await new Promise(resolve => setTimeout(resolve, 1000));
      
      const modelToAdd: ModelInfo = {
        id: newModel.provider === 'local' ? selectedLocalModel!.id : newModel.id,
        name: newModel.provider === 'local' ? selectedLocalModel!.name : newModel.name,
        dimension: newModel.provider === 'local' ? selectedLocalModel!.dimension : newModel.dimension,
        maxTokens: newModel.provider === 'local' ? selectedLocalModel!.maxTokens : newModel.maxTokens,
        provider: newModel.provider,
        endpoint: newModel.provider === 'local' ? undefined : newModel.endpoint || undefined,
        performance: { avgLatency: 0, throughput: 0 },
      };
      
      setModels(prev => [...prev, modelToAdd]);
      setSuccess(`Model "${modelToAdd.name}" added successfully`);
      setIsModelModalOpen(false);
      
      setNewModel({
        id: '',
        name: '',
        dimension: 384,
        maxTokens: 512,
        provider: 'custom',
        endpoint: '',
        apiKey: '',
      });
    } catch (err: any) {
      setError(err.message || 'Failed to add model');
    } finally {
      setSaving(false);
    }
  };

  const handleSyncTables = async () => {
    setIsSyncing(true);
    setError(null);
    setSuccess(null);
    setSyncProgress(null);
    
    try {
      // Convert s3:// to s3a:// for Hadoop compatibility
      let s3Path = storageConfig.iceberg.warehouse;
      if (s3Path.startsWith('s3://')) {
        s3Path = s3Path.replace('s3://', 's3a://');
      }
      
      const response = await fetch('http://localhost:8080/api/tables/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          catalogName: 'iceberg_catalog',
          s3Path: s3Path,
          syncExistingTables: true,
          registerNewTables: true,
          createdBy: 'admin',
          awsAccessKey: storageConfig.s3.accessKey,
          awsSecretKey: storageConfig.s3.secretKey,
          awsEndpoint: storageConfig.s3.endpoint,
        }),
      });
      
      const result = await response.json();
      
      if (result.success) {
        setSyncJobId(result.jobId);
        setSuccess('Table sync started successfully');
        
        // Start polling for status
        pollSyncStatus(result.jobId);
      } else {
        setError(result.message || 'Failed to start table sync');
        setIsSyncing(false);
      }
    } catch (err: any) {
      setError(err.message || 'Failed to start table sync');
      setIsSyncing(false);
    }
  };

  const pollSyncStatus = async (jobId: string) => {
    const pollInterval = setInterval(async () => {
      try {
        const response = await fetch(`http://localhost:8080/api/tables/sync/status/${jobId}`);
        const status = await response.json();
        
        setSyncProgress({
          status: status.status,
          tablesDiscovered: status.tablesDiscovered || 0,
          tablesRegistered: status.tablesRegistered || 0,
          message: status.errorMessage,
        });
        
        if (status.status === 'COMPLETED') {
          clearInterval(pollInterval);
          setIsSyncing(false);
          setSuccess(`Table sync completed! Discovered ${status.tablesDiscovered} tables, registered ${status.tablesRegistered} tables.`);
          loadData(); // Reload tables
        } else if (status.status === 'FAILED') {
          clearInterval(pollInterval);
          setIsSyncing(false);
          setError(`Table sync failed: ${status.errorMessage}`);
        }
      } catch (err) {
        console.error('Failed to poll sync status:', err);
      }
    }, 2000); // Poll every 2 seconds
    
    // Stop polling after 5 minutes
    setTimeout(() => {
      clearInterval(pollInterval);
      if (isSyncing) {
        setIsSyncing(false);
        setError('Sync operation timed out');
      }
    }, 300000);
  };

  const catalogTypeOptions = [
    { id: 'rest', label: 'REST Catalog' },
    { id: 'hive', label: 'Hive Metastore' },
    { id: 'glue', label: 'AWS Glue' },
  ];

  const tableHeaders = [
    { key: 'table', header: 'Table' },
    { key: 'model', header: 'Model' },
    { key: 'syncEnabled', header: 'Sync Enabled' },
    { key: 'syncFrequency', header: 'Sync Frequency' },
    { key: 'actions', header: 'Actions' },
  ];

  const tableRows = tables.map(table => ({
    id: table.tableId,
    table: `${table.catalog}.${table.tableName}`,
    model: table.modelName,
    syncEnabled: table.syncEnabled,
    syncFrequency: '5 min',
    tableData: table,
  }));

  const modelHeaders = [
    { key: 'name', header: 'Model' },
    { key: 'dimension', header: 'Dimension' },
    { key: 'provider', header: 'Provider' },
    { key: 'latency', header: 'Avg Latency' },
    { key: 'throughput', header: 'Throughput' },
  ];

  const modelRows = models.map(model => ({
    id: model.id,
    name: model.name,
    dimension: model.dimension,
    provider: model.provider,
    latency: `${model.performance.avgLatency}ms`,
    throughput: `${model.performance.throughput} tok/s`,
  }));

  if (loading) {
    return <Loading description="Loading configuration..." withOverlay={false} />;
  }

  return (
    <div className="configuration">
      <div className="configuration__header">
        <h1>Configuration</h1>
        <p>System-wide and per-table settings management</p>
      </div>

      {error && (
        <InlineNotification
          kind="error"
          title="Error"
          subtitle={error}
          onClose={() => setError(null)}
          className="configuration__notification"
        />
      )}

      {success && (
        <InlineNotification
          kind="success"
          title="Success"
          subtitle={success}
          onClose={() => setSuccess(null)}
          className="configuration__notification"
        />
      )}

      <Tabs>
        <TabList aria-label="Configuration tabs">
          <Tab>System Settings</Tab>
          <Tab>Table Configuration</Tab>
          <Tab>Model Management</Tab>
          <Tab>Storage & Infrastructure</Tab>
          <Tab>Index Settings</Tab>
        </TabList>
        <TabPanels>
          {/* System Settings Tab */}
          <TabPanel>
            <div className="configuration__tab-content">
              <h2>System Settings</h2>
              
              <Accordion>
                <AccordionItem title="Embedding Service">
                  <div className="configuration__form">
                    <TextInput
                      id="embedding-endpoint"
                      labelText="Service Endpoint"
                      value={systemConfig.embeddingService.endpoint}
                      onChange={(e) =>
                        setSystemConfig(prev => ({
                          ...prev,
                          embeddingService: { ...prev.embeddingService, endpoint: e.target.value },
                        }))
                      }
                      helperText="URL of the embedding service"
                    />
                    <NumberInput
                      id="embedding-timeout"
                      label="Timeout (ms)"
                      value={systemConfig.embeddingService.timeout}
                      onChange={(e, { value }) =>
                        setSystemConfig(prev => ({
                          ...prev,
                          embeddingService: { ...prev.embeddingService, timeout: Number(value) || 30000 },
                        }))
                      }
                      min={1000}
                      max={120000}
                      step={1000}
                    />
                    <NumberInput
                      id="embedding-retries"
                      label="Retry Attempts"
                      value={systemConfig.embeddingService.retryAttempts}
                      onChange={(e, { value }) =>
                        setSystemConfig(prev => ({
                          ...prev,
                          embeddingService: { ...prev.embeddingService, retryAttempts: Number(value) || 3 },
                        }))
                      }
                      min={0}
                      max={10}
                    />
                  </div>
                </AccordionItem>

                <AccordionItem title="Worker Pool">
                  <div className="configuration__form">
                    <NumberInput
                      id="worker-pool-size"
                      label="Pool Size"
                      value={systemConfig.workerPool.size}
                      onChange={(e, { value }) =>
                        setSystemConfig(prev => ({
                          ...prev,
                          workerPool: { ...prev.workerPool, size: Number(value) || 4 },
                        }))
                      }
                      min={1}
                      max={32}
                      helperText="Number of worker threads"
                    />
                    <NumberInput
                      id="max-concurrent"
                      label="Max Concurrent Jobs"
                      value={systemConfig.workerPool.maxConcurrent}
                      onChange={(e, { value }) =>
                        setSystemConfig(prev => ({
                          ...prev,
                          workerPool: { ...prev.workerPool, maxConcurrent: Number(value) || 10 },
                        }))
                      }
                      min={1}
                      max={100}
                    />
                  </div>
                </AccordionItem>

                <AccordionItem title="Sync & Retry Policy">
                  <div className="configuration__form">
                    <NumberInput
                      id="sync-frequency"
                      label="Sync Frequency (seconds)"
                      value={systemConfig.syncFrequency}
                      onChange={(e, { value }) =>
                        setSystemConfig(prev => ({ ...prev, syncFrequency: Number(value) || 300 }))
                      }
                      min={10}
                      max={3600}
                      step={10}
                    />
                    <NumberInput
                      id="max-retries"
                      label="Max Retries"
                      value={systemConfig.retryPolicy.maxRetries}
                      onChange={(e, { value }) =>
                        setSystemConfig(prev => ({
                          ...prev,
                          retryPolicy: { ...prev.retryPolicy, maxRetries: Number(value) || 3 },
                        }))
                      }
                      min={0}
                      max={10}
                    />
                    <NumberInput
                      id="backoff-multiplier"
                      label="Backoff Multiplier"
                      value={systemConfig.retryPolicy.backoffMultiplier}
                      onChange={(e, { value }) =>
                        setSystemConfig(prev => ({
                          ...prev,
                          retryPolicy: { ...prev.retryPolicy, backoffMultiplier: Number(value) || 2 },
                        }))
                      }
                      min={1}
                      max={10}
                      step={0.5}
                    />
                  </div>
                </AccordionItem>
              </Accordion>

              <div className="configuration__actions">
                <Button kind="primary" renderIcon={Save} onClick={handleSaveSystemConfig} disabled={saving}>
                  {saving ? 'Saving...' : 'Save Changes'}
                </Button>
                <Button kind="secondary" renderIcon={Reset} onClick={loadData}>
                  Reset
                </Button>
              </div>
            </div>
          </TabPanel>

          {/* Table Configuration Tab */}
          <TabPanel>
            <div className="configuration__tab-content">
              <h2>Table Configuration</h2>
              <p className="configuration__tab-description">
                Manage per-table settings and sync preferences
              </p>

              <DataTable rows={tableRows} headers={tableHeaders}>
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
                          const originalRow = tableRows.find(r => r.id === row.id)!;
                          return (
                            <TableRow {...getRowProps({ row })} key={row.id}>
                              <TableCell>{originalRow.table}</TableCell>
                              <TableCell>
                                <Tag type="blue" size="sm">
                                  {originalRow.model}
                                </Tag>
                              </TableCell>
                              <TableCell>
                                <Toggle
                                  id={`sync-${originalRow.id}`}
                                  size="sm"
                                  toggled={originalRow.syncEnabled}
                                  onToggle={(checked) =>
                                    handleToggleTableSync(originalRow.id, checked)
                                  }
                                  labelA="Off"
                                  labelB="On"
                                />
                              </TableCell>
                              <TableCell>{originalRow.syncFrequency}</TableCell>
                              <TableCell>
                                <Button kind="ghost" size="sm" renderIcon={Edit}>
                                  Edit
                                </Button>
                              </TableCell>
                            </TableRow>
                          );
                        })}
                      </TableBody>
                    </Table>
                  </TableContainer>
                )}
              </DataTable>
            </div>
          </TabPanel>

          {/* Model Management Tab */}
          <TabPanel>
            <div className="configuration__tab-content">
              <h2>Model Management</h2>
              <p className="configuration__tab-description">
                Available embedding models and their specifications
              </p>

              <DataTable rows={modelRows} headers={modelHeaders}>
                {({ rows, headers, getTableProps, getHeaderProps, getRowProps }) => (
                  <TableContainer
                    title="Available Models"
                    description="Embedding models configured for use"
                  >
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
                        {rows.map((row) => (
                          <TableRow {...getRowProps({ row })} key={row.id}>
                            {row.cells.map((cell) => (
                              <TableCell key={cell.id}>
                                {cell.info.header === 'provider' ? (
                                  <Tag
                                    type={
                                      cell.value === 'openai'
                                        ? 'purple'
                                        : cell.value === 'local'
                                        ? 'green'
                                        : 'blue'
                                    }
                                    size="sm"
                                  >
                                    {cell.value}
                                  </Tag>
                                ) : (
                                  cell.value
                                )}
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                )}
              </DataTable>

              <div className="configuration__actions">
                <Button
                  kind="primary"
                  renderIcon={Add}
                  onClick={() => setIsModelModalOpen(true)}
                >
                  Add Custom Model
                </Button>
              </div>
            </div>
          </TabPanel>

          {/* Storage & Infrastructure Tab */}
          <TabPanel>
            <div className="configuration__tab-content">
              <h2>Storage & Infrastructure</h2>

              <Accordion>
                <AccordionItem title="Iceberg Catalog">
                  <div className="configuration__form">
                    <Dropdown
                      id="catalog-type"
                      titleText="Catalog Type"
                      label="Select type"
                      items={catalogTypeOptions}
                      selectedItem={catalogTypeOptions.find(
                        o => o.id === storageConfig.iceberg.catalogType
                      )}
                      onChange={({ selectedItem }) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          iceberg: { ...prev.iceberg, catalogType: selectedItem?.id as any },
                        }))
                      }
                    />
                    <TextInput
                      id="catalog-uri"
                      labelText="Catalog URI"
                      value={storageConfig.iceberg.uri}
                      onChange={(e) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          iceberg: { ...prev.iceberg, uri: e.target.value },
                        }))
                      }
                    />
                    <TextInput
                      id="warehouse"
                      labelText="Warehouse Location"
                      value={storageConfig.iceberg.warehouse}
                      onChange={(e) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          iceberg: { ...prev.iceberg, warehouse: e.target.value },
                        }))
                      }
                    />
                  </div>
                </AccordionItem>

                <AccordionItem title="S3 / MinIO Storage">
                  <div className="configuration__form">
                    <TextInput
                      id="s3-endpoint"
                      labelText="Endpoint"
                      value={storageConfig.s3.endpoint}
                      onChange={(e) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          s3: { ...prev.s3, endpoint: e.target.value },
                        }))
                      }
                    />
                    <TextInput
                      id="s3-bucket"
                      labelText="Bucket Name"
                      value={storageConfig.s3.bucket}
                      onChange={(e) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          s3: { ...prev.s3, bucket: e.target.value },
                        }))
                      }
                    />
                    <TextInput
                      id="s3-access-key"
                      labelText="Access Key"
                      value={storageConfig.s3.accessKey}
                      onChange={(e) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          s3: { ...prev.s3, accessKey: e.target.value },
                        }))
                      }
                      type="password"
                    />
                    <PasswordInput
                      id="s3-secret-key"
                      labelText="Secret Key"
                      value={storageConfig.s3.secretKey}
                      onChange={(e) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          s3: { ...prev.s3, secretKey: e.target.value },
                        }))
                      }
                    />
                    
                    <Tile className="configuration__sync-tile" style={{ marginTop: '2rem' }}>
                      <div className="configuration__sync-header">
                        <Catalog size={24} style={{ marginRight: '0.5rem' }} />
                        <h4>Iceberg Table Discovery</h4>
                      </div>
                      <p className="configuration__sync-description">
                        Scan your S3 storage to discover and sync all Iceberg tables. This will automatically register tables with their metadata, schemas, and warehouse locations.
                      </p>
                      
                      {syncProgress && (
                        <InlineNotification
                          kind={syncProgress.status === 'RUNNING' ? 'info' : syncProgress.status === 'COMPLETED' ? 'success' : 'error'}
                          title={`Sync Status: ${syncProgress.status}`}
                          subtitle={`Discovered: ${syncProgress.tablesDiscovered} tables | Registered: ${syncProgress.tablesRegistered} tables`}
                          lowContrast
                          style={{ marginTop: '1rem', marginBottom: '1rem' }}
                        />
                      )}
                      
                      <div className="configuration__sync-actions">
                        <Button
                          kind="primary"
                          renderIcon={Renew}
                          onClick={handleSyncTables}
                          disabled={isSyncing || !storageConfig.s3.accessKey || !storageConfig.s3.secretKey}
                        >
                          {isSyncing ? 'Syncing Tables...' : 'Sync Tables from S3'}
                        </Button>
                        
                        {isSyncing && (
                          <Loading description="Scanning S3 for Iceberg tables..." withOverlay={false} small />
                        )}
                      </div>
                    </Tile>
                  </div>
                </AccordionItem>

                <AccordionItem title="Database">
                  <div className="configuration__form">
                    <TextInput
                      id="db-host"
                      labelText="Host"
                      value={storageConfig.database.host}
                      onChange={(e) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          database: { ...prev.database, host: e.target.value },
                        }))
                      }
                    />
                    <NumberInput
                      id="db-port"
                      label="Port"
                      value={storageConfig.database.port}
                      onChange={(e, { value }) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          database: { ...prev.database, port: Number(value) || 5432 },
                        }))
                      }
                      min={1}
                      max={65535}
                    />
                    <TextInput
                      id="db-name"
                      labelText="Database Name"
                      value={storageConfig.database.database}
                      onChange={(e) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          database: { ...prev.database, database: e.target.value },
                        }))
                      }
                    />
                    <TextInput
                      id="db-username"
                      labelText="Username"
                      value={storageConfig.database.username}
                      onChange={(e) =>
                        setStorageConfig(prev => ({
                          ...prev,
                          database: { ...prev.database, username: e.target.value },
                        }))
                      }
                    />
                  </div>
                </AccordionItem>
              </Accordion>

              <div className="configuration__actions">
                <Button kind="primary" renderIcon={Save} onClick={handleSaveStorageConfig} disabled={saving}>
                  {saving ? 'Saving...' : 'Save Changes'}
                </Button>
                <Button kind="secondary" renderIcon={Reset}>
                  Reset
                </Button>
              </div>
            </div>
          </TabPanel>

          {/* Index Settings Tab */}
          <TabPanel>
            <div className="configuration__tab-content">
              <h2>Index Settings</h2>

              <Accordion>
                <AccordionItem title="HNSW Parameters">
                  <div className="configuration__form">
                    <NumberInput
                      id="hnsw-m"
                      label="M (connections per layer)"
                      value={indexConfig.hnsw.m}
                      onChange={(e, { value }) =>
                        setIndexConfig(prev => ({
                          ...prev,
                          hnsw: { ...prev.hnsw, m: Number(value) || 16 },
                        }))
                      }
                      min={4}
                      max={64}
                      helperText="Higher = better recall, more memory"
                    />
                    <NumberInput
                      id="hnsw-ef-construction"
                      label="ef_construction"
                      value={indexConfig.hnsw.efConstruction}
                      onChange={(e, { value }) =>
                        setIndexConfig(prev => ({
                          ...prev,
                          hnsw: { ...prev.hnsw, efConstruction: Number(value) || 200 },
                        }))
                      }
                      min={16}
                      max={1000}
                      helperText="Higher = better index quality, slower build"
                    />
                    <NumberInput
                      id="hnsw-ef-search"
                      label="ef_search"
                      value={indexConfig.hnsw.efSearch}
                      onChange={(e, { value }) =>
                        setIndexConfig(prev => ({
                          ...prev,
                          hnsw: { ...prev.hnsw, efSearch: Number(value) || 50 },
                        }))
                      }
                      min={10}
                      max={500}
                      helperText="Higher = better recall, slower search"
                    />
                  </div>
                </AccordionItem>

                <AccordionItem title="Cache & Performance">
                  <div className="configuration__form">
                    <NumberInput
                      id="cache-size"
                      label="Cache Size (MB)"
                      value={indexConfig.cacheSize}
                      onChange={(e, { value }) =>
                        setIndexConfig(prev => ({ ...prev, cacheSize: Number(value) || 1024 }))
                      }
                      min={128}
                      max={8192}
                      step={128}
                    />
                    <NumberInput
                      id="rebuild-threshold"
                      label="Rebuild Threshold (%)"
                      value={indexConfig.rebuildThreshold}
                      onChange={(e, { value }) =>
                        setIndexConfig(prev => ({ ...prev, rebuildThreshold: Number(value) || 10 }))
                      }
                      min={1}
                      max={50}
                      helperText="Rebuild index when staleness exceeds this %"
                    />
                  </div>
                </AccordionItem>
              </Accordion>

              <div className="configuration__actions">
                <Button kind="primary" renderIcon={Save} onClick={handleSaveIndexConfig} disabled={saving}>
                  {saving ? 'Saving...' : 'Save Changes'}
                </Button>
                <Button kind="secondary" renderIcon={Reset}>
                  Reset
                </Button>
              </div>
            </div>
          </TabPanel>
        </TabPanels>
      </Tabs>

      {/* Add Custom Model Modal */}
      <Modal
        open={isModelModalOpen}
        onRequestClose={() => setIsModelModalOpen(false)}
        onRequestSubmit={handleAddModel}
        modalHeading="Add Custom Embedding Model"
        modalLabel="Model Management"
        primaryButtonText={saving ? 'Adding...' : 'Add Model'}
        secondaryButtonText="Cancel"
        primaryButtonDisabled={saving}
      >
        <div className="configuration__modal-content">
          <Dropdown
            id="model-provider"
            titleText="Provider"
            label="Select provider"
            items={providerOptions}
            selectedItem={providerOptions.find((option) => option.id === newModel.provider)}
            onChange={({ selectedItem }) =>
              setNewModel(prev => ({
                ...prev,
                provider: (selectedItem?.id as 'openai' | 'cohere' | 'local' | 'custom') || prev.provider,
                id: selectedItem?.id === 'local' ? '' : prev.id,
                name: selectedItem?.id === 'local' ? '' : prev.name,
              }))
            }
          />

          {newModel.provider === 'local' ? (
            <>
              <Dropdown
                id="local-model"
                titleText="Local Model"
                label="Select local model"
                items={localModelOptions}
                selectedItem={selectedLocalModel}
                onChange={({ selectedItem }) =>
                  setNewModel(prev => ({
                    ...prev,
                    id: selectedItem?.id || '',
                    name: selectedItem?.name || '',
                    dimension: selectedItem?.dimension || 384,
                    maxTokens: selectedItem?.maxTokens || 512,
                  }))
                }
                helperText="Choose from models available in the embedding service"
              />

              <TextInput
                id="model-dimension"
                labelText="Embedding Dimension"
                value={selectedLocalModel ? String(selectedLocalModel.dimension) : ''}
                readOnly
                helperText="Derived automatically from the selected local model"
              />

              <TextInput
                id="model-max-tokens"
                labelText="Max Tokens"
                value={selectedLocalModel ? String(selectedLocalModel.maxTokens) : ''}
                readOnly
                helperText="Derived automatically from the selected local model"
              />
            </>
          ) : (
            <>
              <TextInput
                id="model-id"
                labelText="Model ID"
                placeholder="e.g., text-embedding-3-small"
                value={newModel.id}
                onChange={(e) => setNewModel(prev => ({ ...prev, id: e.target.value }))}
                helperText="Unique identifier for the model"
              />

              <TextInput
                id="model-name"
                labelText="Display Name"
                placeholder="e.g., OpenAI Embedding v3 Small"
                value={newModel.name}
                onChange={(e) => setNewModel(prev => ({ ...prev, name: e.target.value }))}
                helperText="Human-readable name"
              />

              <TextInput
                id="model-endpoint"
                labelText="API Endpoint"
                placeholder="e.g., https://api.openai.com/v1/embeddings"
                value={newModel.endpoint}
                onChange={(e) => setNewModel(prev => ({ ...prev, endpoint: e.target.value }))}
                helperText="Full URL to the embedding API endpoint"
              />

              <PasswordInput
                id="model-api-key"
                labelText="API Key"
                placeholder="Enter your API key"
                value={newModel.apiKey}
                onChange={(e) => setNewModel(prev => ({ ...prev, apiKey: e.target.value }))}
                helperText="API key for authentication (stored securely)"
              />

              <NumberInput
                id="model-dimension"
                label="Embedding Dimension"
                value={newModel.dimension}
                onChange={(e, { value }) =>
                  setNewModel(prev => ({ ...prev, dimension: Number(value) || 384 }))
                }
                min={128}
                max={4096}
                step={64}
                helperText="Use manual dimension only when metadata cannot be fetched from the provider"
              />

              <NumberInput
                id="model-max-tokens"
                label="Max Tokens"
                value={newModel.maxTokens}
                onChange={(e, { value }) =>
                  setNewModel(prev => ({ ...prev, maxTokens: Number(value) || 512 }))
                }
                min={128}
                max={8192}
                step={128}
                helperText="Maximum input tokens supported"
              />
            </>
          )}

          <div className="configuration__modal-examples">
            <h4>Example Configurations:</h4>
            <ul>
              <li><strong>OpenAI:</strong> text-embedding-3-small (1536d), text-embedding-ada-002 (1536d)</li>
              <li><strong>Cohere:</strong> embed-english-v3.0 (1024d), embed-multilingual-v3.0 (1024d)</li>
              <li><strong>Google:</strong> textembedding-gecko@003 (768d)</li>
              <li><strong>Custom:</strong> Any OpenAI-compatible embedding API</li>
            </ul>
          </div>
        </div>
      </Modal>
    </div>
  );
};

// Made with Bob
