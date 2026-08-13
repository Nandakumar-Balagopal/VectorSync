import React, { useState, useEffect } from 'react';
import {
  TextInput,
  Button,
  Dropdown,
  Tabs,
  TabList,
  Tab,
  TabPanels,
  TabPanel,
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
  CodeSnippet,
} from '@carbon/react';
import { Search, Renew, Debug, ChevronRight } from '@carbon/icons-react';
import { vectorSyncApi } from '../services/api';
import type { TableConfig, RowDebugInfo, CDCEvent, NearestNeighbor } from '../types';
import './DebugPanel.scss';

export const DebugPanel: React.FC = () => {
  const [tables, setTables] = useState<TableConfig[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [rowId, setRowId] = useState('');
  const [loading, setLoading] = useState(false);
  const [debugInfo, setDebugInfo] = useState<RowDebugInfo | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [reembedding, setReembedding] = useState(false);

  useEffect(() => {
    loadTables();
  }, []);

  const loadTables = async () => {
    try {
      const data = await vectorSyncApi.getTables();
      setTables(Array.isArray(data) ? data : []);
      if (data.length > 0 && !selectedTable) {
        setSelectedTable(data[0].tableId);
      }
    } catch (err) {
      console.error('Failed to load tables:', err);
    }
  };

  const handleLookup = async () => {
    if (!selectedTable || !rowId.trim()) {
      setError('Please select a table and enter a row ID');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const data = await vectorSyncApi.getRowDebugInfo(selectedTable, rowId);
      setDebugInfo(data);
    } catch (err: any) {
      console.error('Lookup failed:', err);
      setError(err.message || 'Failed to fetch row data');
    } finally {
      setLoading(false);
    }
  };

  const handleReembed = async () => {
    if (!debugInfo) return;

    setReembedding(true);
    try {
      await vectorSyncApi.reembedRow(debugInfo.tableId, debugInfo.rowId);
      
      // Reload debug info
      await handleLookup();
    } catch (err) {
      console.error('Re-embed failed:', err);
      setError('Failed to re-embed row');
    } finally {
      setReembedding(false);
    }
  };

  const tableDropdownItems = tables.map(t => ({
    id: t.tableId,
    label: `${t.catalog}.${t.tableName}`,
  }));

  const cdcHeaders = [
    { key: 'snapshotId', header: 'Snapshot ID' },
    { key: 'timestamp', header: 'Timestamp' },
    { key: 'operation', header: 'Operation' },
    { key: 'changes', header: 'Changes' },
  ];

  const cdcRows = debugInfo?.cdcHistory.map((event, index) => ({
    id: `cdc-${index}`,
    snapshotId: event.snapshotId,
    timestamp: new Date(event.timestamp).toLocaleString(),
    operation: event.operation,
    changes: event,
  })) || [];

  const neighborHeaders = [
    { key: 'rank', header: 'Rank' },
    { key: 'rowId', header: 'Row ID' },
    { key: 'similarity', header: 'Similarity' },
    { key: 'distance', header: 'Distance' },
    { key: 'text', header: 'Text' },
  ];

  const neighborRows = debugInfo?.nearestNeighbors?.map((neighbor, index) => ({
    id: neighbor.rowId,
    rank: index + 1,
    rowId: neighbor.rowId,
    similarity: neighbor.similarity,
    distance: neighbor.distance,
    text: neighbor.text,
  })) || [];

  const formatEmbedding = (embedding: number[]): string => {
    if (embedding.length <= 10) {
      return `[${embedding.map(v => v.toFixed(4)).join(', ')}]`;
    }
    const preview = embedding.slice(0, 5).map(v => v.toFixed(4)).join(', ');
    return `[${preview}, ... (${embedding.length} dimensions)]`;
  };

  return (
    <div className="debug-panel">
      <div className="debug-panel__header">
        <h1>Debug Panel</h1>
        <p>Row-level debugging and inspection for vectorization pipeline</p>
      </div>

      <div className="debug-panel__search">
        <div className="debug-panel__search-inputs">
          <Dropdown
            id="table-selector"
            titleText="Table"
            label="Select table"
            items={tableDropdownItems}
            selectedItem={tableDropdownItems.find(t => t.id === selectedTable)}
            onChange={({ selectedItem }) => setSelectedTable(selectedItem?.id || '')}
            disabled={loading}
          />

          <TextInput
            id="row-id"
            labelText="Row ID"
            placeholder="Enter row ID to inspect"
            value={rowId}
            onChange={(e) => setRowId(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleLookup()}
            disabled={loading}
          />

          <Button
            kind="primary"
            renderIcon={Search}
            onClick={handleLookup}
            disabled={loading || !selectedTable || !rowId.trim()}
            className="debug-panel__search-btn"
          >
            {loading ? 'Looking up...' : 'Lookup'}
          </Button>
        </div>
      </div>

      {error && (
        <InlineNotification
          kind="error"
          title="Error"
          subtitle={error}
          onClose={() => setError(null)}
          className="debug-panel__notification"
        />
      )}

      {loading && (
        <div className="debug-panel__loading">
          <Loading description="Fetching row data..." withOverlay={false} />
        </div>
      )}

      {debugInfo && !loading && (
        <div className="debug-panel__results">
          <div className="debug-panel__results-header">
            <div className="debug-panel__breadcrumb">
              <span>{debugInfo.tableName}</span>
              <ChevronRight size={16} />
              <span>{debugInfo.rowId}</span>
            </div>
            <Button
              kind="tertiary"
              renderIcon={Renew}
              onClick={handleReembed}
              disabled={reembedding}
              size="sm"
            >
              {reembedding ? 'Re-embedding...' : 'Re-embed Row'}
            </Button>
          </div>

          <div className="debug-panel__metadata">
            <div className="debug-panel__metadata-item">
              <span className="label">Model:</span>
              <Tag type="blue">{debugInfo.modelUsed}</Tag>
            </div>
            <div className="debug-panel__metadata-item">
              <span className="label">Dimension:</span>
              <Tag type="purple">{debugInfo.embeddingDimension}d</Tag>
            </div>
            <div className="debug-panel__metadata-item">
              <span className="label">Created:</span>
              <span>{new Date(debugInfo.createdAt).toLocaleString()}</span>
            </div>
            <div className="debug-panel__metadata-item">
              <span className="label">Modified:</span>
              <span>{new Date(debugInfo.lastModified).toLocaleString()}</span>
            </div>
          </div>

          <Tabs>
            <TabList aria-label="Debug tabs">
              <Tab>Source Data</Tab>
              <Tab>Embedding</Tab>
              <Tab>CDC History</Tab>
              <Tab>Nearest Neighbors</Tab>
            </TabList>
            <TabPanels>
              <TabPanel>
                <div className="debug-panel__tab-content">
                  <h3>Original Row Data</h3>
                  <CodeSnippet type="multi" feedback="Copied!">
                    {JSON.stringify(debugInfo.sourceData, null, 2)}
                  </CodeSnippet>
                </div>
              </TabPanel>

              <TabPanel>
                <div className="debug-panel__tab-content">
                  <h3>Embedding Vector</h3>
                  <div className="debug-panel__embedding-info">
                    <p>
                      <strong>Dimension:</strong> {debugInfo.embeddingDimension}
                    </p>
                    <p>
                      <strong>Model:</strong> {debugInfo.modelUsed}
                    </p>
                    <p>
                      <strong>Preview:</strong> {formatEmbedding(debugInfo.embedding)}
                    </p>
                  </div>
                  <Accordion>
                    <AccordionItem title="Full Embedding Vector (Click to expand)">
                      <CodeSnippet type="multi" feedback="Copied!">
                        {JSON.stringify(debugInfo.embedding, null, 2)}
                      </CodeSnippet>
                    </AccordionItem>
                  </Accordion>
                </div>
              </TabPanel>

              <TabPanel>
                <div className="debug-panel__tab-content">
                  <h3>Change Data Capture History</h3>
                  <DataTable rows={cdcRows} headers={cdcHeaders}>
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
                              const originalRow = cdcRows.find(r => r.id === row.id)!;
                              return (
                                <TableRow {...getRowProps({ row })} key={row.id}>
                                  <TableCell>{originalRow.snapshotId}</TableCell>
                                  <TableCell>{originalRow.timestamp}</TableCell>
                                  <TableCell>
                                    <Tag
                                      type={
                                        originalRow.operation === 'insert'
                                          ? 'green'
                                          : originalRow.operation === 'update'
                                          ? 'blue'
                                          : 'red'
                                      }
                                    >
                                      {originalRow.operation}
                                    </Tag>
                                  </TableCell>
                                  <TableCell>
                                    <Accordion size="sm">
                                      <AccordionItem title="View changes">
                                        <CodeSnippet type="multi" feedback="Copied!">
                                          {JSON.stringify(
                                            {
                                              before: originalRow.changes.beforeValue,
                                              after: originalRow.changes.afterValue,
                                            },
                                            null,
                                            2
                                          )}
                                        </CodeSnippet>
                                      </AccordionItem>
                                    </Accordion>
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

              <TabPanel>
                <div className="debug-panel__tab-content">
                  <h3>Nearest Neighbors in Vector Space</h3>
                  <p className="debug-panel__tab-description">
                    Rows with similar embeddings (cosine similarity)
                  </p>
                  <DataTable rows={neighborRows} headers={neighborHeaders}>
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
                              const originalRow = neighborRows.find(r => r.id === row.id)!;
                              return (
                                <TableRow {...getRowProps({ row })} key={row.id}>
                                  <TableCell>
                                    <strong>#{originalRow.rank}</strong>
                                  </TableCell>
                                  <TableCell>{originalRow.rowId}</TableCell>
                                  <TableCell>
                                    <Tag
                                      type={
                                        originalRow.similarity >= 0.9
                                          ? 'green'
                                          : originalRow.similarity >= 0.7
                                          ? 'teal'
                                          : 'blue'
                                      }
                                    >
                                      {(originalRow.similarity * 100).toFixed(1)}%
                                    </Tag>
                                  </TableCell>
                                  <TableCell>{originalRow.distance.toFixed(4)}</TableCell>
                                  <TableCell>
                                    <div className="debug-panel__neighbor-text">
                                      {originalRow.text}
                                    </div>
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
            </TabPanels>
          </Tabs>
        </div>
      )}

      {!debugInfo && !loading && (
        <div className="debug-panel__empty-state">
          <Debug size={64} />
          <h3>Ready to debug</h3>
          <p>Select a table and enter a row ID to inspect vectorization details</p>
          
          <div className="debug-panel__sample-ids">
            <h4>Try these sample row IDs:</h4>
            <div className="debug-panel__sample-buttons">
              <Button
                kind="ghost"
                size="sm"
                onClick={() => setRowId('prod-123')}
              >
                prod-123
              </Button>
              <Button
                kind="ghost"
                size="sm"
                onClick={() => setRowId('prod-456')}
              >
                prod-456
              </Button>
              <Button
                kind="ghost"
                size="sm"
                onClick={() => setRowId('prod-789')}
              >
                prod-789
              </Button>
            </div>
          </div>

          <div className="debug-panel__tips">
            <h4>What you can do:</h4>
            <ul>
              <li>View original source data and metadata</li>
              <li>Inspect embedding vectors and dimensions</li>
              <li>Track CDC history and changes over time</li>
              <li>Find nearest neighbors in vector space</li>
              <li>Re-embed rows to refresh vectors</li>
            </ul>
          </div>
        </div>
      )}
    </div>
  );
};
