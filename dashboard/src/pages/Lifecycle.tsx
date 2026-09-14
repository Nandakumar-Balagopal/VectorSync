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
  TextArea,
  Tile,
} from '@carbon/react';
import { vectorSyncApi } from '../services/api';
import type {
  EvaluationReport,
  IndexAliasEntry,
  IndexStatusRow,
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
  const [indexes, setIndexes] = useState<IndexStatusRow[] | null>(null);
  const [evalQueries, setEvalQueries] = useState(
    'wireless earbuds with noise cancelling | p-001,p-002,p-003\n'
    + 'waterproof boots for hiking | p-011,p-014');
  const [evalReports, setEvalReports] = useState<Record<string, EvaluationReport>>({});
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
      const [versionData, indexData, historyData] = await Promise.all([
        vectorSyncApi.getModelVersions(table),
        vectorSyncApi.getIndexStatus(table),
        vectorSyncApi.getPromotionHistory(table),
      ]);

      setVersions(versionData);
      setIndexes(indexData);
      setHistory(historyData);
      setPromotedId(indexData.find(entry => entry.serving)?.indexId ?? null);
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

  /**
   * Each line is "query | relevantRowId,relevantRowId". Leave the ids off to measure index
   * recall only, which needs no labels.
   */
  const parseQueries = () => evalQueries
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      const [query, ids] = line.split('|');
      return {
        query: (query ?? '').trim(),
        relevantSourceRowIds: (ids ?? '').split(',').map(id => id.trim()).filter(Boolean),
      };
    })
    .filter(entry => entry.query.length > 0);

  const evaluate = async (indexId: string) => {
    const queries = parseQueries();
    if (queries.length === 0) {
      setError('Add at least one query, one per line.');
      return;
    }
    setBusy(`Evaluate ${indexId}`);
    setError(null);
    try {
      const report = await vectorSyncApi.evaluateIndex(indexId, 10, queries);
      setEvalReports(current => ({ ...current, [indexId]: report }));
      await load(sourceTable);
    } catch (err) {
      setError(describeError(err, 'Evaluation failed'));
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
              <StructuredListCell head>Model</StructuredListCell>
              <StructuredListCell head>Dim</StructuredListCell>
              <StructuredListCell head>Vectors</StructuredListCell>
              <StructuredListCell head>Coverage</StructuredListCell>
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
                <StructuredListCell>
                  {entry.embeddingModel}
                  <span className="lifecycle__muted"> :{entry.embeddingVersion}</span>
                </StructuredListCell>
                <StructuredListCell>{entry.dimension}</StructuredListCell>
                <StructuredListCell>{entry.vectorCount}</StructuredListCell>
                <StructuredListCell>
                  {entry.current
                    ? <Tag type="blue" size="sm">current</Tag>
                    : <Tag type="red" size="sm" title="Built over a snapshot that has since been superseded">stale</Tag>}
                </StructuredListCell>
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
                    disabled={entry.status !== 'READY' || busy !== null}
                    onClick={() => evaluate(entry.indexId)}
                  >
                    {busy === `Evaluate ${entry.indexId}` ? 'Scoring…' : 'Evaluate'}
                  </Button>
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

      <h2>Evaluation</h2>
      <p className="lifecycle__empty">
        One query per line, as <code>query | relevantRowId,relevantRowId</code>. Omit the ids to
        measure index recall only. Results are written onto the index&apos;s manifest entry, so the
        numbers a promotion was based on stay attached to the artifact.
      </p>
      <TextArea
        id="eval-queries"
        labelText="Queries and relevance labels"
        rows={4}
        value={evalQueries}
        onChange={event => setEvalQueries(event.target.value)}
      />
      <p className="lifecycle__empty lifecycle__eval-note">
        <strong>Index recall</strong> compares the index against an exhaustive scan — it measures
        the <em>index</em> and needs no labels. <strong>Precision</strong> compares against your
        labels — it measures the <em>model</em>. A tiny index can score perfect recall and useless
        precision, so promote on precision and treat low recall as a build-parameter problem.
      </p>

      {Object.keys(evalReports).length > 0 && (
        <StructuredListWrapper>
          <StructuredListHead>
            <StructuredListRow head>
              <StructuredListCell head>Index</StructuredListCell>
              <StructuredListCell head>Queries</StructuredListCell>
              <StructuredListCell head>Index recall@k</StructuredListCell>
              <StructuredListCell head>Precision@k</StructuredListCell>
            </StructuredListRow>
          </StructuredListHead>
          <StructuredListBody>
            {Object.values(evalReports).map(report => (
              <StructuredListRow key={report.indexId}>
                <StructuredListCell><code>{report.indexId.slice(0, 12)}</code></StructuredListCell>
                <StructuredListCell>{report.queryCount}</StructuredListCell>
                <StructuredListCell>{report.indexRecallAtK.toFixed(3)}</StructuredListCell>
                <StructuredListCell>
                  {report.precisionAtK === null
                    ? <span className="lifecycle__muted">no labels</span>
                    : report.precisionAtK.toFixed(3)}
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
