import React, { useState, useEffect } from 'react';
import {
  Search,
  Button,
  TextArea,
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
} from '@carbon/react';
import { Search as SearchIcon, Debug, Renew } from '@carbon/icons-react';
import { vectorSyncApi } from '../services/api';
import type { TableConfig, SearchResponse, SearchResult } from '../types';
import './SemanticSearch.scss';

export const SemanticSearch: React.FC = () => {
  const [tables, setTables] = useState<TableConfig[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [query, setQuery] = useState('');
  const [topK, setTopK] = useState(10);
  const [useIndex, setUseIndex] = useState(true);
  const [loading, setLoading] = useState(false);
  const [searchResponse, setSearchResponse] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [debugMode, setDebugMode] = useState(false);

  // Load tables on mount
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
      setError('Failed to load tables');
    }
  };

  const handleSearch = async () => {
    if (!query.trim() || !selectedTable) {
      setError('Please enter a query and select a table');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const table = tables.find(t => t.tableId === selectedTable);
      const response = useIndex
        ? await vectorSyncApi.search(query, table?.tableName || selectedTable, topK)
        : await vectorSyncApi.searchExact(query, table?.tableName || selectedTable, topK);
      setSearchResponse(response);
    } catch (err: any) {
      console.error('Search failed:', err);
      setError(err.message || 'Search failed');
      setSearchResponse(null);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSearch();
    }
  };

  const clearResults = () => {
    setSearchResponse(null);
    setError(null);
  };

  const getSimilarityColor = (similarity: number): 'green' | 'teal' | 'blue' | 'gray' => {
    if (similarity >= 0.9) return 'green';
    if (similarity >= 0.7) return 'teal';
    if (similarity >= 0.5) return 'blue';
    return 'gray';
  };

  const tableDropdownItems = tables.map(t => ({
    id: t.tableId,
    label: `${t.catalog}.${t.tableName}`,
  }));

  const topKOptions = [
    { id: '5', label: '5 results' },
    { id: '10', label: '10 results' },
    { id: '20', label: '20 results' },
    { id: '50', label: '50 results' },
  ];

  const headers = [
    { key: 'rank', header: 'Rank' },
    { key: 'similarity', header: 'Similarity' },
    { key: 'text', header: 'Text' },
    { key: 'sourceRowId', header: 'Row ID' },
    { key: 'vectorId', header: 'Vector ID' },
  ];

  const rows = searchResponse?.results.map((result, index) => ({
    id: result.vectorId,
    rank: index + 1,
    similarity: result.similarity,
    text: result.text,
    sourceRowId: result.sourceRowId,
    vectorId: result.vectorId,
    result, // Keep full result for debug mode
  })) || [];

  return (
    <div className="semantic-search">
      <div className="semantic-search__header">
        <h1>Semantic Search Playground</h1>
        <p>Test vector search across your vectorized tables with real-time results</p>
      </div>

      <div className="semantic-search__controls">
        <div className="semantic-search__input-section">
          <div className="semantic-search__query-row">
            <div className="semantic-search__query-input">
              <TextArea
                id="search-query"
                labelText="Search Query"
                placeholder="Enter your search query... (Press Enter to search)"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyPress={handleKeyPress}
                rows={3}
                disabled={loading}
              />
            </div>
            <Button
              kind="primary"
              renderIcon={SearchIcon}
              onClick={handleSearch}
              disabled={loading || !query.trim() || !selectedTable}
              className="semantic-search__search-btn"
            >
              {loading ? 'Searching...' : 'Search'}
            </Button>
          </div>

          <div className="semantic-search__options-row">
            <div className="semantic-search__dropdown">
              <Dropdown
                id="table-selector"
                titleText="Target Table"
                label="Select table"
                items={tableDropdownItems}
                selectedItem={tableDropdownItems.find(t => t.id === selectedTable)}
                onChange={({ selectedItem }) => setSelectedTable(selectedItem?.id || '')}
                disabled={loading}
              />
            </div>

            <div className="semantic-search__dropdown">
              <Dropdown
                id="topk-selector"
                titleText="Results Limit"
                label="Select limit"
                items={topKOptions}
                selectedItem={topKOptions.find(o => o.id === String(topK))}
                onChange={({ selectedItem }) => setTopK(Number(selectedItem?.id) || 10)}
                disabled={loading}
              />
            </div>

            <div className="semantic-search__toggle">
              <Toggle
                id="use-index-toggle"
                labelText="Use HNSW Index"
                labelA="Brute Force"
                labelB="HNSW"
                toggled={useIndex}
                onToggle={(checked) => setUseIndex(checked)}
                disabled={loading}
              />
            </div>

            <div className="semantic-search__toggle">
              <Toggle
                id="debug-mode-toggle"
                labelText="Debug Mode"
                labelA="Off"
                labelB="On"
                toggled={debugMode}
                onToggle={(checked) => setDebugMode(checked)}
                disabled={loading}
              />
            </div>

            {searchResponse && (
              <Button
                kind="ghost"
                renderIcon={Renew}
                onClick={clearResults}
                disabled={loading}
              >
                Clear
              </Button>
            )}
          </div>
        </div>
      </div>

      {error && (
        <InlineNotification
          kind="error"
          title="Search Error"
          subtitle={error}
          onClose={() => setError(null)}
          className="semantic-search__notification"
        />
      )}

      {loading && (
        <div className="semantic-search__loading">
          <Loading description="Searching vectors..." withOverlay={false} />
        </div>
      )}

      {searchResponse && !loading && (
        <div className="semantic-search__results">
          <div className="semantic-search__results-header">
            <div className="semantic-search__results-stats">
              <h2>Search Results</h2>
              <div className="semantic-search__stats-row">
                <Tag type="blue">
                  {searchResponse.totalResults} results
                </Tag>
                <Tag type="green">
                  {searchResponse.executionTimeMs}ms
                </Tag>
                <Tag type={useIndex ? 'purple' : 'gray'}>
                  {useIndex ? 'HNSW Index' : 'Brute Force'}
                </Tag>
              </div>
            </div>
          </div>

          {debugMode && (
            <Accordion className="semantic-search__debug">
              <AccordionItem title="Debug Information">
                <div className="semantic-search__debug-content">
                  <div className="semantic-search__debug-section">
                    <h4>Query Details</h4>
                    <pre>{JSON.stringify({
                      query: searchResponse.query,
                      sourceTable: selectedTable,
                      topK,
                      useIndex,
                      executionTimeMs: searchResponse.executionTimeMs,
                    }, null, 2)}</pre>
                  </div>
                  <div className="semantic-search__debug-section">
                    <h4>Search Algorithm</h4>
                    <p>
                      {useIndex 
                        ? 'Using HNSW (Hierarchical Navigable Small World) approximate nearest neighbor search'
                        : 'Using brute-force exact nearest neighbor search (slower but 100% accurate)'}
                    </p>
                  </div>
                </div>
              </AccordionItem>
            </Accordion>
          )}

          <DataTable rows={rows} headers={headers}>
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
                    {rows.map((row) => (
                      <TableRow {...getRowProps({ row })} key={row.id}>
                        {row.cells.map((cell) => {
                          if (cell.info.header === 'rank') {
                            return (
                              <TableCell key={cell.id}>
                                <strong>#{cell.value}</strong>
                              </TableCell>
                            );
                          }
                          if (cell.info.header === 'similarity') {
                            return (
                              <TableCell key={cell.id}>
                                <Tag type={getSimilarityColor(cell.value)}>
                                  {(cell.value * 100).toFixed(1)}%
                                </Tag>
                              </TableCell>
                            );
                          }
                          if (cell.info.header === 'text') {
                            return (
                              <TableCell key={cell.id}>
                                <div className="semantic-search__text-cell">
                                  {cell.value}
                                </div>
                              </TableCell>
                            );
                          }
                          if (cell.info.header === 'metadata') {
                            return (
                              <TableCell key={cell.id}>
                                {debugMode ? (
                                  <pre className="semantic-search__metadata">
                                    {JSON.stringify(cell.value, null, 2)}
                                  </pre>
                                ) : (
                                  <span className="semantic-search__metadata-compact">
                                    {Object.keys(cell.value || {}).length} fields
                                  </span>
                                )}
                              </TableCell>
                            );
                          }
                          return <TableCell key={cell.id}>{cell.value}</TableCell>;
                        })}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </DataTable>

          {searchResponse.results.length === 0 && (
            <div className="semantic-search__no-results">
              <SearchIcon size={48} />
              <h3>No results found</h3>
              <p>Try adjusting your query or selecting a different table</p>
            </div>
          )}
        </div>
      )}

      {!searchResponse && !loading && (
        <div className="semantic-search__empty-state">
          <SearchIcon size={64} />
          <h3>Ready to search</h3>
          <p>Enter a query above to search across your vectorized data</p>
          <div className="semantic-search__tips">
            <h4>Tips:</h4>
            <ul>
              <li>Use natural language queries for best results</li>
              <li>HNSW index provides faster approximate search</li>
              <li>Brute force search is slower but 100% accurate</li>
              <li>Enable debug mode to see detailed search information</li>
            </ul>
          </div>
        </div>
      )}
    </div>
  );
};
