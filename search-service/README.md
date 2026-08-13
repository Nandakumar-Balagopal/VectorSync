# Search Service

The search service provides semantic search capabilities over vector embeddings stored in Iceberg tables.

## Responsibilities

- Semantic search API endpoints
- Query embedding generation
- Cosine similarity search
- Row lookup and retrieval
- Response ranking and filtering
- Future ANN index integration

## Architecture

```
search-service/
├── api/              # REST API controllers
├── retrieval/        # Vector retrieval logic
├── ranking/          # Result ranking and scoring
├── cache/            # Query cache (TODO)
├── fetch/            # Row fetching from source tables
├── query/            # Query processing and validation
└── config/           # Configuration
```

## Key Components

### API
- `SearchController`: REST endpoints for semantic search
- Supports query text, filters, and pagination
- Returns ranked results with similarity scores

### Retrieval
- `VectorSyncReader`: Reads vector embeddings from Iceberg
- Performs cosine similarity search
- Supports filtering by source table and metadata

### Ranking
- Scores results by cosine similarity
- Supports custom ranking functions
- Filters by minimum similarity threshold

### Fetch (TODO)
- Fetches full row data from source Iceberg tables
- Enriches search results with original data
- Supports selective field retrieval

## Configuration

```yaml
server:
  port: 8083

embedding:
  provider:
    type: mock  # Options: mock, external
    external:
      url: http://localhost:8000/embed
      timeout: 30000

iceberg:
  catalog:
    warehouse: s3a://warehouse/iceberg
  vector:
    namespace: vector

search:
  default-limit: 10
  max-limit: 100
  min-similarity: 0.7

aws:
  s3:
    endpoint: http://localhost:9000
    access-key: minioadmin
    secret-key: minioadmin
    region: us-east-1
```

## Running

```bash
# Build
mvn clean package

# Run
java -jar target/vectorsync-search-service-0.1.0-SNAPSHOT.jar

# Or with Maven
mvn spring-boot:run
```

## Environment Variables

```bash
EMBEDDING_PROVIDER_TYPE=mock
EMBEDDING_PROVIDER_EXTERNAL_URL=http://localhost:8000/embed
ICEBERG_CATALOG_WAREHOUSE=s3a://warehouse/iceberg
AWS_S3_ENDPOINT=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
SEARCH_DEFAULT_LIMIT=10
SEARCH_MAX_LIMIT=100
SEARCH_MIN_SIMILARITY=0.7
```

## API Endpoints

### Semantic Search
```http
POST /api/search
Content-Type: application/json

{
  "query": "search query text",
  "sourceTable": "my_table",  // optional filter
  "limit": 10,
  "minSimilarity": 0.7
}
```

Response:
```json
{
  "results": [
    {
      "id": "row-123",
      "sourceTable": "my_table",
      "similarity": 0.95,
      "textContent": "matching text content",
      "metadata": {
        "key": "value"
      }
    }
  ],
  "totalResults": 1,
  "queryTime": 45
}
```

### Health Check
```http
GET /actuator/health
```

## How It Works

1. **Query Processing**: Validates and processes search query
2. **Embedding Generation**: Generates embedding for query text
3. **Vector Search**: Performs cosine similarity search in Iceberg
4. **Ranking**: Ranks results by similarity score
5. **Filtering**: Applies filters (source table, min similarity)
6. **Response**: Returns ranked results with metadata

## Similarity Calculation

Uses cosine similarity:
```
similarity = (A · B) / (||A|| × ||B||)
```

Where:
- A = query embedding vector
- B = stored embedding vector
- Range: -1 to 1 (higher is more similar)

## Future Enhancements

### ANN Index Integration (TODO)
Replace brute-force search with approximate nearest neighbor (ANN) index:

Options:
- **FAISS**: Facebook AI Similarity Search
- **Annoy**: Spotify's ANN library
- **HNSW**: Hierarchical Navigable Small World graphs
- **ScaNN**: Google's Scalable Nearest Neighbors

Benefits:
- Sub-linear search time (O(log n) vs O(n))
- Handles millions/billions of vectors
- Configurable accuracy/speed tradeoff
- Reduced memory footprint

Example configuration:
```yaml
search:
  index:
    type: faiss
    metric: cosine
    nlist: 100  # number of clusters
    nprobe: 10  # clusters to search
    rebuild-interval: 3600  # rebuild every hour
```

### Query Cache (TODO)
Cache frequent queries:
```yaml
cache:
  enabled: true
  type: redis
  redis:
    host: localhost
    port: 6379
  ttl: 300  # 5 minutes
```

Benefits:
- Faster response for repeated queries
- Reduced compute costs
- Lower latency

### Hybrid Search (TODO)
Combine semantic and keyword search:
```yaml
search:
  hybrid:
    enabled: true
    semantic-weight: 0.7
    keyword-weight: 0.3
```

### Advanced Features (TODO)
- Multi-vector search (search multiple embeddings)
- Filtered search (metadata-based filtering)
- Faceted search (group by categories)
- Search analytics and logging
- Query suggestions and autocomplete
- Personalized ranking

## Performance Optimization

### Current (Brute Force)
- Time complexity: O(n) where n = number of vectors
- Suitable for: < 100K vectors
- Latency: 10-100ms for small datasets

### With ANN Index (Future)
- Time complexity: O(log n)
- Suitable for: millions/billions of vectors
- Latency: 1-10ms even for large datasets

### Recommendations
- Use brute force for < 100K vectors
- Implement ANN index for > 100K vectors
- Consider sharding for > 10M vectors
- Monitor query latency and adjust accordingly

## Monitoring

Health endpoint: `http://localhost:8083/actuator/health`

Metrics to monitor:
- Query latency (p50, p95, p99)
- Queries per second
- Embedding generation time
- Vector scan time
- Result count distribution
- Cache hit rate (future)
- Index rebuild time (future)

## Troubleshooting

### Slow queries
- Check vector table size
- Consider implementing ANN index
- Enable query caching
- Optimize similarity threshold
- Review partition strategy

### Low similarity scores
- Verify embedding model consistency
- Check query text quality
- Review embedding dimensions
- Validate vector normalization

### Missing results
- Check min similarity threshold
- Verify source table filter
- Review vector table data
- Check embedding generation

### High memory usage
- Implement result pagination
- Reduce default limit
- Consider streaming results
- Monitor vector table size