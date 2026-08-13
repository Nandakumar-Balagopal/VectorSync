import { useState } from 'react';
import { Search, ClickableTile, Tag, Stack, Button } from '@carbon/react';
import { Search as SearchIcon, Play } from '@carbon/icons-react';
import type { SearchResult } from '../types';
import './SemanticPlayground.scss';

interface SemanticPlaygroundProps {
  onSearch: (query: string) => Promise<SearchResult[]>;
}

export const SemanticPlayground = ({ onSearch }: SemanticPlaygroundProps) => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);

  const handleSearch = async () => {
    if (!query.trim()) return;

    setLoading(true);
    setSearched(true);
    try {
      const searchResults = await onSearch(query);
      setResults(searchResults);
    } catch (error) {
      console.error('Search error:', error);
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  return (
    <div className="semantic-playground">
      <Stack gap={5}>
        <div className="playground-header">
          <h3>Semantic Playground</h3>
          <p className="subtitle">Search across vectorized tables</p>
        </div>
        <div className="search-container">
          <Search
            size="lg"
            placeholder="Search vectors, entities, and context..."
            labelText="Search"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyPress={handleKeyPress}
            disabled={loading}
          />
          <Button
            size="md"
            kind="primary"
            className="search-action"
            onClick={() => void handleSearch()}
            disabled={loading || !query.trim()}
            renderIcon={Play}
          >
            Run Search
          </Button>
        </div>
      </Stack>

      <div className="results-container">
        {loading && (
          <div className="loading-state">
            <div className="spinner"></div>
            <p>Searching...</p>
          </div>
        )}

        {!loading && searched && results.length === 0 && (
          <div className="empty-state">
            <SearchIcon size={48} />
            <p>No results found</p>
            <span>Try a different search query</span>
          </div>
        )}

        {!loading && results.length > 0 && (
          <div className="results-list">
            <div className="results-header">
              <span className="results-count">
                {results.length} result{results.length !== 1 ? 's' : ''}
              </span>
            </div>

            {results.map((result) => (
              <ClickableTile
                key={result.vectorId}
                href="#"
                onClick={(event) => event.preventDefault()}
                className="result-card"
              >
                <div className="result-header">
                  <span className="result-title">{result.text.substring(0, 60)}...</span>
                  <Tag type="blue" size="sm" className="similarity-score">
                    {(result.similarity * 100).toFixed(1)}%
                  </Tag>
                </div>

                <div className="result-meta">
                  <Tag type="gray" size="sm">
                    {result.sourceTable}
                  </Tag>
                  <span className="result-id">ID: {result.sourceRowId}</span>
                </div>

                <div className="result-content">
                  {result.text}
                </div>

                {result.metadata && Object.keys(result.metadata).length > 0 && (
                  <div className="result-metadata">
                    {Object.entries(result.metadata).slice(0, 3).map(([key, value]) => (
                      <div key={key} className="metadata-item">
                        <span className="metadata-key">{key}:</span>
                        <span className="metadata-value">{String(value)}</span>
                      </div>
                    ))}
                  </div>
                )}
              </ClickableTile>
            ))}
          </div>
        )}

        {!loading && !searched && (
          <div className="initial-state">
            <SearchIcon size={64} />
            <p>Start searching</p>
            <span>Enter a query to find similar vectors</span>
          </div>
        )}
      </div>
    </div>
  );
};
