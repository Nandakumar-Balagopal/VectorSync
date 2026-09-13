import { useCallback, useEffect, useState } from 'react';
import {
  Button,
  Dropdown,
  InlineNotification,
  Loading,
  StructuredListBody,
  StructuredListCell,
  StructuredListHead,
  StructuredListRow,
  StructuredListWrapper,
  Tag,
  Tile,
} from '@carbon/react';
import { vectorSyncApi } from '../services/api';
import type {
  IndexAliasEntry,
  IndexManifestEntry,
  ModelVersions,
  TableConfig,
} from '../types';
import { describeError, formatTime } from '../utils/format';
import './Lifecycle.scss';

const STATUS_TAG: Record<string, 'green' | 'blue' | 'red' | 'gray'> = {
  READY: 'green',
  BUILDING: 'blue',
  FAILED: 'red',
  ARCHIVED: 'gray',
};

/**
 * The embedding lifecycle: which versions exist, which index artifacts were built over them,
 * which one serves production, and the full promotion history.
 */
export function Lifecycle() {
  const [tables, setTables] = useState<TableConfig[]>([]);
  const [sourceTable, setSourceTable] = useState<string>('');
  const [versions, setVersions] = useState<ModelVersions | null>(null);
  const [indexes, setIndexes] = useState<IndexManifestEntry[] | null>(null);
  const [promotedId, setPromotedId] = useState<string | null>(null);
  const [history, setHistory] = useState<IndexAliasEntry[]>([]);
  const [selectedVersion, setSelectedVersion] = useState<string>('');
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    vectorSyncApi.getTables()
      .then(data => {
        setTables(data);
        if (data.length > 0) {
          setSourceTable(data[0].tableName);
        }
      })
      .catch(err => setError(describeError(err, 'Could not load tables')));
  }, []);

  const load = useCallback(async (table: string) => {
    setError(null);
    try {
      const [versionData, indexData, promoted, historyData] = await Promise.all([
        vectorSyncApi.getModelVersions(table),
        vectorSyncApi.getIndexes(table),
        vectorSyncApi.getPromotedIndex(table),
        vectorSyncApi.getPromotionHistory(table),
      ]);

      setVersions(versionData);
      setIndexes(indexData);
      setHistory(historyData);
      setPromotedId('indexId' in promoted ? (promoted as IndexManifestEntry).indexId : null);
      setSelectedVersion(versionData.modelVersions[0] ?? '');
    } catch (err) {
      setError(describeError(err, 'Could not load lifecycle state'));
      setIndexes([]);
    }
  }, []);

  useEffect(() => {
    if (!sourceTable) {
      return;
    }
    void load(sourceTable);
  }, [sourceTable, load]);

  // Derived rather than stored: no setState runs synchronously from the effect.
  const loading = indexes === null;

  const act = async (label: string, action: () => Promise<unknown>) => {
    setBusy(label);
    setError(null);
    try {
      await action();
      await load(sourceTable);
    } catch (err) {
      setError(describeError(err, `${label} failed`));
    } finally {
      setBusy(null);
    }
  };

  if (!tables.length && !error) {
    return <Loading description="Loading tables" withOverlay={false} />;
  }

  return (
    <div className="lifecycle">
      <h1>Embedding lifecycle</h1>

      {error && (
        <InlineNotification
          kind="error"
          title="Error"
          subtitle={error}
          onCloseButtonClick={() => setError(null)}
        />
      )}

      <div className="lifecycle__controls">
        <Dropdown
          id="source-table"
          titleText="Source table"
          label="Select a table"
          items={tables.map(table => table.tableName)}
          selectedItem={sourceTable || null}
          onChange={({ selectedItem }) => setSourceTable(selectedItem ?? '')}
        />
        <Dropdown
          id="model-version"
          titleText="Embedding version"
          label="Select a version"
          items={versions?.modelVersions ?? []}
          selectedItem={selectedVersion || null}
          onChange={({ selectedItem }) => setSelectedVersion(selectedItem ?? '')}
        />
        <Button
          disabled={!selectedVersion || busy !== null}
          onClick={() => act('Build index', () =>
            vectorSyncApi.buildIndex(sourceTable, selectedVersion))}
        >
          {busy === 'Build index' ? 'Building…' : 'Build index'}
        </Button>
        <Button
          kind="tertiary"
          disabled={history.length < 2 || busy !== null}
          onClick={() => act('Rollback', () => vectorSyncApi.rollbackIndex(sourceTable))}
        >
          Roll back
        </Button>
      </div>

      {versions && (
        <Tile className="lifecycle__summary">
          <div>
            <span className="lifecycle__label">Materialized versions</span>
            <span className="lifecycle__value">
              {versions.modelVersions.length ? versions.modelVersions.join(', ') : 'none'}
            </span>
          </div>
          <div>
            <span className="lifecycle__label">Latest source snapshot</span>
            <span className="lifecycle__value">{versions.latestSourceSnapshotId || '—'}</span>
          </div>
          <div>
            <span className="lifecycle__label">Serving</span>
            <span className="lifecycle__value">
              {promotedId ? <code>{promotedId.slice(0, 12)}</code> : 'exact search (nothing promoted)'}
            </span>
          </div>
        </Tile>
      )}

      <h2>Index artifacts</h2>
      {loading ? (
        <Loading description="Loading indexes" withOverlay={false} />
      ) : (indexes ?? []).length === 0 ? (
        <p className="lifecycle__empty">
          No indexes built for this table yet. Build one to serve searches from an ANN index
          instead of an exhaustive scan.
        </p>
      ) : (
        <StructuredListWrapper>
          <StructuredListHead>
            <StructuredListRow head>
              <StructuredListCell head>Index</StructuredListCell>
              <StructuredListCell head>Version</StructuredListCell>
              <StructuredListCell head>Snapshot</StructuredListCell>
              <StructuredListCell head>Vectors</StructuredListCell>
              <StructuredListCell head>Status</StructuredListCell>
              <StructuredListCell head>Metrics</StructuredListCell>
              <StructuredListCell head />
            </StructuredListRow>
          </StructuredListHead>
          <StructuredListBody>
            {(indexes ?? []).map(entry => (
              <StructuredListRow key={entry.indexId}>
                <StructuredListCell>
                  <code>{entry.indexId.slice(0, 12)}</code>
                  {entry.indexId === promotedId && (
                    <Tag type="green" size="sm" className="lifecycle__serving">serving</Tag>
                  )}
                </StructuredListCell>
                <StructuredListCell>{entry.embeddingVersion}</StructuredListCell>
                <StructuredListCell>{entry.sourceSnapshotId}</StructuredListCell>
                <StructuredListCell>{entry.vectorCount}</StructuredListCell>
                <StructuredListCell>
                  <Tag type={STATUS_TAG[entry.status] ?? 'gray'} size="sm">{entry.status}</Tag>
                </StructuredListCell>
                <StructuredListCell>
                  {Object.keys(entry.evalMetrics ?? {}).length === 0
                    ? <span className="lifecycle__muted">not evaluated</span>
                    : Object.entries(entry.evalMetrics)
                        .map(([key, value]) => `${key}=${value}`)
                        .join('  ')}
                </StructuredListCell>
                <StructuredListCell>
                  <Button
                    size="sm"
                    kind="ghost"
                    disabled={entry.status !== 'READY' || entry.indexId === promotedId || busy !== null}
                    onClick={() => act('Promote', () =>
                      vectorSyncApi.promoteIndex(sourceTable, entry.indexId, 'promoted from dashboard'))}
                  >
                    Promote
                  </Button>
                </StructuredListCell>
              </StructuredListRow>
            ))}
          </StructuredListBody>
        </StructuredListWrapper>
      )}

      <h2>Promotion history</h2>
      {history.length === 0 ? (
        <p className="lifecycle__empty">Nothing has been promoted for this table.</p>
      ) : (
        <StructuredListWrapper>
          <StructuredListHead>
            <StructuredListRow head>
              <StructuredListCell head>When</StructuredListCell>
              <StructuredListCell head>Index</StructuredListCell>
              <StructuredListCell head>By</StructuredListCell>
              <StructuredListCell head>Note</StructuredListCell>
            </StructuredListRow>
          </StructuredListHead>
          <StructuredListBody>
            {[...history].reverse().map((entry, position) => (
              <StructuredListRow key={`${entry.updatedAt}-${position}`}>
                <StructuredListCell>{formatTime(entry.updatedAt)}</StructuredListCell>
                <StructuredListCell><code>{entry.indexId.slice(0, 12)}</code></StructuredListCell>
                <StructuredListCell>{entry.updatedBy ?? '—'}</StructuredListCell>
                <StructuredListCell>{entry.note ?? '—'}</StructuredListCell>
              </StructuredListRow>
            ))}
          </StructuredListBody>
        </StructuredListWrapper>
      )}
    </div>
  );
}
