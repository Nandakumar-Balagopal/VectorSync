import { useCallback, useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import {
  Button,
  InlineNotification,
  Loading,
  StructuredListBody,
  StructuredListCell,
  StructuredListHead,
  StructuredListRow,
  StructuredListWrapper,
  Tag,
  TextInput,
  Tile,
} from '@carbon/react';
import { vectorSyncApi } from '../services/api';
import type { IndexManifestEntry, ModelVersions, SyncStatus, TableConfig } from '../types';
import { describeError } from '../utils/format';
import './TableDetails.scss';

/**
 * One table's real configuration, sync state, materialized embedding versions, and indexes.
 *
 * The previous version rendered a fabricated CDC timeline, embedding throughput, and index
 * freshness score against endpoints that do not exist.
 */
export function TableDetails() {
  const { tableId } = useParams<{ tableId: string }>();
  const navigate = useNavigate();

  const [config, setConfig] = useState<TableConfig | null>(null);
  const [status, setStatus] = useState<SyncStatus | null>(null);
  const [versions, setVersions] = useState<ModelVersions | null>(null);
  const [indexes, setIndexes] = useState<IndexManifestEntry[]>([]);
  const [newVersion, setNewVersion] = useState('');
  const [newModel, setNewModel] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!tableId) return;
    try {
      const configData = await vectorSyncApi.getTable(tableId);
      // See Overview: cleared after the fetch resolves so no setState runs synchronously in the
      // effect, and so a real error survives until a reload actually succeeds.
      setError(null);
      setConfig(configData);
      setStatus(await vectorSyncApi.getSyncStatus(tableId).catch(() => null));

      const [versionData, indexData] = await Promise.all([
        vectorSyncApi.getModelVersions(configData.tableName).catch(() => null),
        vectorSyncApi.getIndexes(configData.tableName).catch(() => []),
      ]);
      setVersions(versionData);
      setIndexes(indexData);
    } catch (err) {
      setError(describeError(err, 'Could not load table'));
    }
  }, [tableId]);

  useEffect(() => {
    void load();
  }, [load]);

  // Derived rather than stored: no setState runs synchronously from the effect.
  const loading = config === null && error === null;

  const migrate = async () => {
    if (!tableId || (!newVersion.trim() && !newModel.trim())) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const updated = await vectorSyncApi.setEmbedding(
        tableId, newModel.trim() || undefined, newVersion.trim() || undefined);
      setNotice(
        `Now targeting ${updated.modelName}:${updated.embeddingVersion}. The sync watermark was `
        + 'reset, so the next sync materializes this combination alongside the existing ones — '
        + 'nothing is overwritten.');
      setNewVersion('');
      setNewModel('');
      await load();
    } catch (err) {
      setError(describeError(err, 'Could not change the embedding model or version'));
    } finally {
      setBusy(false);
    }
  };

  if (loading) {
    return <Loading description="Loading table" withOverlay={false} />;
  }

  if (!config) {
    return (
      <div className="table-details">
        <InlineNotification kind="error" title="Not found" subtitle={error ?? 'Unknown table'} />
        <Button kind="ghost" onClick={() => navigate('/')}>Back to overview</Button>
      </div>
    );
  }

  return (
    <div className="table-details">
      <div className="table-details__header">
        <div>
          <h1>{config.tableName}</h1>
          <p className="table-details__subtitle">{config.tableId}</p>
        </div>
        <Button kind="ghost" onClick={() => navigate('/')}>Back</Button>
      </div>

      {error && (
        <InlineNotification kind="error" title="Error" subtitle={error}
          onCloseButtonClick={() => setError(null)} />
      )}
      {notice && (
        <InlineNotification kind="success" title="Migration started" subtitle={notice}
          onCloseButtonClick={() => setNotice(null)} />
      )}

      <div className="table-details__tiles">
        <Tile>
          <span className="table-details__label">Model</span>
          <span className="table-details__value">{config.modelName}</span>
        </Tile>
        <Tile>
          <span className="table-details__label">Current version</span>
          <span className="table-details__value">{config.embeddingVersion ?? 'v1'}</span>
        </Tile>
        <Tile>
          <span className="table-details__label">Embedded columns</span>
          <span className="table-details__value">{config.embeddingColumns.join(', ')}</span>
        </Tile>
        <Tile>
          <span className="table-details__label">Last synced snapshot</span>
          <span className="table-details__value">{status?.lastSnapshotId ?? 'never'}</span>
        </Tile>
      </div>

      <h2>Materialized versions</h2>
      <p className="table-details__hint">
        {versions?.modelVersions.length
          ? versions.modelVersions.join(', ')
          : 'None yet — run a sync to materialize embeddings.'}
      </p>

      <h2>Start a migration</h2>
      <p className="table-details__hint">
        Change the model, the version, or both. The new combination is materialized alongside the
        existing ones so it can be indexed and evaluated before it serves traffic — nothing is
        overwritten. Any sentence-transformers model name works; it is downloaded on first use.
      </p>
      <div className="table-details__migrate">
        <TextInput
          id="new-model"
          labelText="New embedding model (optional)"
          placeholder="all-mpnet-base-v2"
          value={newModel}
          onChange={event => setNewModel(event.target.value)}
        />
        <TextInput
          id="new-version"
          labelText="New embedding version (optional)"
          placeholder="v2"
          value={newVersion}
          onChange={event => setNewVersion(event.target.value)}
        />
        <Button
          disabled={busy || (!newVersion.trim() && !newModel.trim())}
          onClick={migrate}
        >
          {busy ? 'Applying…' : 'Start migration'}
        </Button>
      </div>

      <h2>Indexes</h2>
      {indexes.length === 0 ? (
        <p className="table-details__hint">No indexes built for this table.</p>
      ) : (
        <StructuredListWrapper>
          <StructuredListHead>
            <StructuredListRow head>
              <StructuredListCell head>Index</StructuredListCell>
              <StructuredListCell head>Version</StructuredListCell>
              <StructuredListCell head>Snapshot</StructuredListCell>
              <StructuredListCell head>Vectors</StructuredListCell>
              <StructuredListCell head>Status</StructuredListCell>
            </StructuredListRow>
          </StructuredListHead>
          <StructuredListBody>
            {indexes.map(entry => (
              <StructuredListRow key={entry.indexId}>
                <StructuredListCell><code>{entry.indexId.slice(0, 12)}</code></StructuredListCell>
                <StructuredListCell>{entry.embeddingVersion}</StructuredListCell>
                <StructuredListCell>{entry.sourceSnapshotId}</StructuredListCell>
                <StructuredListCell>{entry.vectorCount}</StructuredListCell>
                <StructuredListCell>
                  <Tag type={entry.status === 'READY' ? 'green' : 'gray'} size="sm">
                    {entry.status}
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
