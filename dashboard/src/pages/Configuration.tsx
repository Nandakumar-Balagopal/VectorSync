import { useCallback, useEffect, useState } from 'react';
import {
  Button,
  Checkbox,
  InlineNotification,
  Loading,
  StructuredListBody,
  StructuredListCell,
  StructuredListHead,
  StructuredListRow,
  StructuredListWrapper,
  Tag,
  TextInput,
} from '@carbon/react';
import { vectorSyncApi } from '../services/api';
import type { DiscoveredTable, TableConfig } from '../types';
import { describeError } from '../utils/format';
import './Configuration.scss';

/**
 * Registration and discovery -- the two configuration operations with real backends.
 *
 * The previous version rendered forms for system settings, model registries, storage config and
 * index tuning, none of which had endpoints; edits went nowhere.
 */
export function Configuration() {
  const [tables, setTables] = useState<TableConfig[] | null>(null);
  const [discovered, setDiscovered] = useState<DiscoveredTable[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  // register form
  const [tableName, setTableName] = useState('');
  const [catalog, setCatalog] = useState('default');
  const [columns, setColumns] = useState('name,description');
  const [modelName, setModelName] = useState('all-MiniLM-L6-v2');
  const [embeddingVersion, setEmbeddingVersion] = useState('v1');
  const [enabled, setEnabled] = useState(true);

  // discovery form
  const [s3Path, setS3Path] = useState('');
  const [catalogName, setCatalogName] = useState('');

  const load = useCallback(async () => {
    try {
      setTables(await vectorSyncApi.getTables());
      setDiscovered(await vectorSyncApi.getDiscoveredTables().catch(() => []));
    } catch (err) {
      setError(describeError(err, 'Could not load configuration'));
      setTables([]);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Derived rather than stored: no setState runs synchronously from the effect.
  const loading = tables === null;

  const register = async () => {
    if (!tableName.trim()) return;
    setBusy('register');
    setError(null);
    setNotice(null);
    try {
      const created = await vectorSyncApi.registerTable({
        catalog: catalog.trim() || 'default',
        tableName: tableName.trim(),
        embeddingColumns: columns.split(',').map(part => part.trim()).filter(Boolean),
        modelName: modelName.trim(),
        embeddingVersion: embeddingVersion.trim() || 'v1',
        enabled,
      });
      setNotice(`Registered ${created.tableName}. Run a sync to materialize embeddings.`);
      setTableName('');
      await load();
    } catch (err) {
      setError(describeError(err, 'Registration failed'));
    } finally {
      setBusy(null);
    }
  };

  const discover = async () => {
    if (!s3Path.trim() || !catalogName.trim()) return;
    setBusy('discover');
    setError(null);
    setNotice(null);
    try {
      const { jobId } = await vectorSyncApi.startTableDiscovery({
        catalogName: catalogName.trim(),
        s3Path: s3Path.trim(),
        syncExistingTables: false,
        registerNewTables: false,
        createdBy: 'dashboard',
      });
      setNotice(`Discovery job ${jobId} started. Reload to see results.`);
    } catch (err) {
      setError(describeError(err, 'Discovery failed'));
    } finally {
      setBusy(null);
    }
  };

  if (loading) {
    return <Loading description="Loading configuration" withOverlay={false} />;
  }

  return (
    <div className="configuration">
      <h1>Configuration</h1>

      {error && (
        <InlineNotification kind="error" title="Error" subtitle={error}
          onCloseButtonClick={() => setError(null)} />
      )}
      {notice && (
        <InlineNotification kind="success" title="Done" subtitle={notice}
          onCloseButtonClick={() => setNotice(null)} />
      )}

      <h2>Register a table</h2>
      <p className="configuration__hint">
        The table must already exist in the Iceberg catalog and its rows must have an
        <code> id </code> column, which is what vector identity is derived from.
      </p>
      <div className="configuration__form">
        <TextInput id="table-name" labelText="Table name" placeholder="default.products"
          value={tableName} onChange={event => setTableName(event.target.value)} />
        <TextInput id="catalog" labelText="Catalog" value={catalog}
          onChange={event => setCatalog(event.target.value)} />
        <TextInput id="columns" labelText="Embedded columns (comma separated)" value={columns}
          onChange={event => setColumns(event.target.value)} />
        <TextInput id="model" labelText="Embedding model" value={modelName}
          onChange={event => setModelName(event.target.value)} />
        <TextInput id="version" labelText="Embedding version" value={embeddingVersion}
          onChange={event => setEmbeddingVersion(event.target.value)} />
        <Checkbox id="enabled" labelText="Enabled for sync" checked={enabled}
          onChange={(_, { checked }) => setEnabled(checked)} />
        <Button disabled={busy !== null || !tableName.trim()} onClick={register}>
          {busy === 'register' ? 'Registering…' : 'Register'}
        </Button>
      </div>

      <h2>Registered tables</h2>
      {(tables ?? []).length === 0 ? (
        <p className="configuration__hint">None yet.</p>
      ) : (
        <StructuredListWrapper>
          <StructuredListHead>
            <StructuredListRow head>
              <StructuredListCell head>Table</StructuredListCell>
              <StructuredListCell head>Columns</StructuredListCell>
              <StructuredListCell head>Model</StructuredListCell>
              <StructuredListCell head>Version</StructuredListCell>
              <StructuredListCell head>Enabled</StructuredListCell>
            </StructuredListRow>
          </StructuredListHead>
          <StructuredListBody>
            {(tables ?? []).map(table => (
              <StructuredListRow key={table.tableId}>
                <StructuredListCell>{table.tableName}</StructuredListCell>
                <StructuredListCell>{table.embeddingColumns.join(', ')}</StructuredListCell>
                <StructuredListCell>{table.modelName}</StructuredListCell>
                <StructuredListCell>{table.embeddingVersion ?? 'v1'}</StructuredListCell>
                <StructuredListCell>
                  <Tag type={table.enabled ? 'green' : 'gray'} size="sm">
                    {table.enabled ? 'yes' : 'no'}
                  </Tag>
                </StructuredListCell>
              </StructuredListRow>
            ))}
          </StructuredListBody>
        </StructuredListWrapper>
      )}

      <h2>Discover Iceberg tables in object storage</h2>
      <p className="configuration__hint">
        Crawls a prefix for Iceberg metadata files and records what it finds. Discovery only —
        nothing is registered or synced as a result.
      </p>
      <div className="configuration__form">
        <TextInput id="catalog-name" labelText="Catalog name" placeholder="iceberg_data"
          value={catalogName} onChange={event => setCatalogName(event.target.value)} />
        <TextInput id="s3-path" labelText="Path" placeholder="s3a://bucket/warehouse"
          value={s3Path} onChange={event => setS3Path(event.target.value)} />
        <Button disabled={busy !== null || !s3Path.trim() || !catalogName.trim()} onClick={discover}>
          {busy === 'discover' ? 'Starting…' : 'Start discovery'}
        </Button>
      </div>

      {discovered.length > 0 && (
        <StructuredListWrapper>
          <StructuredListHead>
            <StructuredListRow head>
              <StructuredListCell head>Table</StructuredListCell>
              <StructuredListCell head>Schema</StructuredListCell>
              <StructuredListCell head>Rows</StructuredListCell>
              <StructuredListCell head>Files</StructuredListCell>
              <StructuredListCell head>Registered</StructuredListCell>
            </StructuredListRow>
          </StructuredListHead>
          <StructuredListBody>
            {discovered.map(table => (
              <StructuredListRow key={table.uuid}>
                <StructuredListCell>{table.tableName}</StructuredListCell>
                <StructuredListCell>{table.schemaName}</StructuredListCell>
                <StructuredListCell>{table.totalRecords ?? '—'}</StructuredListCell>
                <StructuredListCell>{table.totalFiles ?? '—'}</StructuredListCell>
                <StructuredListCell>
                  <Tag type={table.registered ? 'green' : 'gray'} size="sm">
                    {table.registered ? 'yes' : 'no'}
                  </Tag>
                </StructuredListCell>
              </StructuredListRow>
            ))}
          </StructuredListBody>
        </StructuredListWrapper>
      )}
    </div>
  );
}
