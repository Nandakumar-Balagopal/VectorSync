import { useCallback, useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
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
  Tile,
} from '@carbon/react';
import { vectorSyncApi } from '../services/api';
import type { SyncStatus, TableConfig } from '../types';
import { describeError, formatTime } from '../utils/format';
import './Overview.scss';

interface TableRow {
  config: TableConfig;
  status: SyncStatus | null;
}

/**
 * Registered tables and their real sync state.
 *
 * Shows only measured values. The previous version displayed health, lag, pending-row counts and
 * index freshness that were generated with Math.random() against endpoints that did not exist.
 */
export function Overview() {
  const navigate = useNavigate();
  const [rows, setRows] = useState<TableRow[] | null>(null);
  const [vectorCount, setVectorCount] = useState<number | null>(null);
  const [vectorCountError, setVectorCountError] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const configs = await vectorSyncApi.getTables();

      const withStatus = await Promise.all(configs.map(async config => ({
        config,
        status: await vectorSyncApi.getSyncStatus(config.tableId).catch(() => null),
      })));
      setRows(withStatus);

      try {
        setVectorCount(await vectorSyncApi.getVectorCount());
        setVectorCountError(false);
      } catch {
        // The vector table may not exist until the first sync; say so rather than showing 0.
        setVectorCount(null);
        setVectorCountError(true);
      }
    } catch (err) {
      setError(describeError(err, 'Could not load tables'));
      setRows([]);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Derived rather than stored: no setState runs synchronously from the effect.
  const loading = rows === null;

  const sync = async () => {
    setSyncing(true);
    setError(null);
    try {
      await vectorSyncApi.triggerSync();
      await load();
    } catch (err) {
      setError(describeError(err, 'Sync failed'));
    } finally {
      setSyncing(false);
    }
  };

  const remove = async (tableId: string) => {
    setError(null);
    try {
      await vectorSyncApi.deleteTable(tableId);
      await load();
    } catch (err) {
      setError(describeError(err, 'Delete failed'));
    }
  };

  if (loading) {
    return <Loading description="Loading tables" withOverlay={false} />;
  }

  return (
    <div className="overview">
      <div className="overview__header">
        <h1>Overview</h1>
        <Button onClick={sync} disabled={syncing || (rows ?? []).length === 0}>
          {syncing ? 'Syncing…' : 'Sync now'}
        </Button>
      </div>

      {error && (
        <InlineNotification
          kind="error"
          title="Error"
          subtitle={error}
          onCloseButtonClick={() => setError(null)}
        />
      )}

      <div className="overview__tiles">
        <Tile>
          <span className="overview__label">Registered tables</span>
          <span className="overview__metric">{(rows ?? []).length}</span>
        </Tile>
        <Tile>
          <span className="overview__label">Live vectors</span>
          <span className="overview__metric">
            {vectorCountError ? <span className="overview__unavailable">unavailable</span> : vectorCount}
          </span>
        </Tile>
        <Tile>
          <span className="overview__label">Enabled for sync</span>
          <span className="overview__metric">{(rows ?? []).filter(row => row.config.enabled).length}</span>
        </Tile>
      </div>

      {(rows ?? []).length === 0 ? (
        <p className="overview__empty">
          No tables registered. Register one from Configuration, then sync to materialize
          embeddings.
        </p>
      ) : (
        <StructuredListWrapper>
          <StructuredListHead>
            <StructuredListRow head>
              <StructuredListCell head>Table</StructuredListCell>
              <StructuredListCell head>Model</StructuredListCell>
              <StructuredListCell head>Version</StructuredListCell>
              <StructuredListCell head>Last synced snapshot</StructuredListCell>
              <StructuredListCell head>Last sync</StructuredListCell>
              <StructuredListCell head>Enabled</StructuredListCell>
              <StructuredListCell head />
            </StructuredListRow>
          </StructuredListHead>
          <StructuredListBody>
            {(rows ?? []).map(({ config, status }) => (
              <StructuredListRow key={config.tableId}>
                <StructuredListCell>{config.tableName}</StructuredListCell>
                <StructuredListCell>{config.modelName}</StructuredListCell>
                <StructuredListCell>{config.embeddingVersion ?? 'v1'}</StructuredListCell>
                <StructuredListCell>
                  {status?.lastSnapshotId ?? <span className="overview__unavailable">never</span>}
                </StructuredListCell>
                <StructuredListCell>{formatTime(status?.lastSyncAt)}</StructuredListCell>
                <StructuredListCell>
                  <Tag type={config.enabled ? 'green' : 'gray'} size="sm">
                    {config.enabled ? 'yes' : 'no'}
                  </Tag>
                </StructuredListCell>
                <StructuredListCell>
                  <Button size="sm" kind="ghost" onClick={() => navigate(`/tables/${config.tableId}`)}>
                    Details
                  </Button>
                  <Button size="sm" kind="danger--ghost" onClick={() => remove(config.tableId)}>
                    Delete
                  </Button>
                </StructuredListCell>
              </StructuredListRow>
            ))}
          </StructuredListBody>
        </StructuredListWrapper>
      )}
    </div>
  );
}
