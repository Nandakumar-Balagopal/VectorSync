# Embedding Worker

The embedding worker is responsible for consuming CDC events, generating embeddings, and writing vector records to Iceberg tables.

## Responsibilities

- Consume CDC events from CDC workers
- Batch records for efficient embedding generation
- Generate embeddings using configured provider
- Write vector records to Iceberg vector tables
- Retry failed embedding jobs
- Cache embeddings for duplicate content

## Architecture

```
embedding-worker/
├── consumer/         # Event consumption (TODO: Kafka)
├── batching/         # Record batching logic
├── provider/         # Embedding provider implementations
├── writer/           # Vector table writer
├── retry/            # Retry logic for failures
├── cache/            # Embedding cache (TODO)
└── config/           # Configuration
```

## Key Components

### Consumer
- `EmbeddingProcessor`: Processes CDC events and coordinates embedding generation
- Currently processes events in-memory
- **Future**: Kafka consumer for event-driven architecture

### Provider
- `EmbeddingProvider`: Interface for embedding generation
- `MockEmbeddingProvider`: Mock provider for testing (random vectors)
- `ExternalEmbeddingProvider`: HTTP-based provider (OpenAI, Gemini, etc.)
- Configurable via `embedding.provider.type` property

### Writer
- `VectorWriter`: Writes embeddings to Iceberg vector tables
- Handles schema management and partitioning
- Supports batch writes for efficiency

## Configuration

```yaml
server:
  port: 8082

embedding:
  provider:
    type: mock  # Options: mock, external
    external:
      url: http://localhost:8000/embed
      timeout: 30000
      batch-size: 100
  batch:
    size: 100
    timeout: 5000

iceberg:
  catalog:
    warehouse: s3a://warehouse/iceberg
  vector:
    namespace: vector

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

# Run with mock provider
java -jar target/vectorsync-embedding-worker-0.1.0-SNAPSHOT.jar

# Run with external provider
java -jar target/vectorsync-embedding-worker-0.1.0-SNAPSHOT.jar \
  --embedding.provider.type=external \
  --embedding.provider.external.url=http://localhost:8000/embed
```

## Environment Variables

```bash
EMBEDDING_PROVIDER_TYPE=mock
EMBEDDING_PROVIDER_EXTERNAL_URL=http://localhost:8000/embed
EMBEDDING_PROVIDER_EXTERNAL_TIMEOUT=30000
EMBEDDING_BATCH_SIZE=100
ICEBERG_CATALOG_WAREHOUSE=s3a://warehouse/iceberg
AWS_S3_ENDPOINT=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
```

## Embedding Providers

### Mock Provider
For testing and development:
```yaml
embedding:
  provider:
    type: mock
```
- Generates random 384-dimensional vectors
- No external dependencies
- Instant response

### External Provider
For production with real embeddings:
```yaml
embedding:
  provider:
    type: external
    external:
      url: http://embedding-service:8000/embed
      timeout: 30000
      batch-size: 100
```

Supports:
- OpenAI text-embedding-3-small/large
- Google Gemini embedding-001
- Sentence Transformers (self-hosted)
- Custom embedding services

Request format:
```json
{
  "texts": ["text1", "text2"],
  "model": "text-embedding-3-small"
}
```

Response format:
```json
{
  "embeddings": [[0.1, 0.2, ...], [0.3, 0.4, ...]],
  "model": "text-embedding-3-small",
  "dimensions": 384
}
```

## How It Works

1. **Event Consumption**: Receives CDC events (currently in-memory, TODO: Kafka)
2. **Batching**: Groups records into batches for efficient processing
3. **Embedding Generation**: Calls configured provider to generate embeddings
4. **Vector Writing**: Writes embeddings to Iceberg vector table
5. **Retry**: Retries failed operations with exponential backoff
6. **State Update**: Updates processing state

## Vector Table Schema

```sql
CREATE TABLE vector.{source_table}_vectors (
  id STRING,
  source_table STRING,
  embedding ARRAY<DOUBLE>,
  text_content STRING,
  metadata MAP<STRING, STRING>,
  created_at TIMESTAMP,
  snapshot_id BIGINT
) PARTITIONED BY (source_table)
```

## Future Enhancements

### Kafka Integration (TODO)
Replace in-memory event consumption with Kafka:
```yaml
kafka:
  bootstrap-servers: localhost:9092
  consumer:
    group-id: embedding-worker-group
    topic: cdc-events
    auto-offset-reset: earliest
```

Benefits:
- Horizontal scaling of embedding workers
- Better fault tolerance and replay capability
- Load balancing across workers
- Consumer group coordination

### Embedding Cache (TODO)
Cache embeddings for duplicate content:
```yaml
cache:
  enabled: true
  type: redis
  redis:
    host: localhost
    port: 6379
  ttl: 86400  # 24 hours
```

Benefits:
- Reduce embedding API costs
- Faster processing for duplicate content
- Lower latency

### Advanced Features (TODO)
- Adaptive batching based on provider latency
- Multi-provider support with fallback
- Embedding quality validation
- Cost tracking and optimization
- Incremental embedding updates

## Monitoring

Health endpoint: `http://localhost:8082/actuator/health`

Metrics to monitor:
- Events consumed per second
- Embedding generation latency
- Batch size and throughput
- Provider API errors and retries
- Vector write latency
- Cache hit rate (future)

## Troubleshooting

### High embedding latency
- Check provider API response times
- Increase batch size for better throughput
- Consider caching for duplicate content
- Monitor provider rate limits

### Vector write failures
- Check Iceberg catalog connectivity
- Verify S3/MinIO credentials
- Check disk space and memory
- Review partition strategy

### Provider API errors
- Verify provider URL and credentials
- Check API rate limits and quotas
- Review request/response formats
- Monitor provider service health

### Memory issues
- Reduce batch size
- Implement streaming for large batches
- Monitor JVM heap usage
- Consider distributed processing