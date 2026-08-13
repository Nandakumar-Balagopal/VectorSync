# Search Service

The search service turns a user query into an embedding and searches the generated VectorSync Iceberg vector table for the closest rows.

## Responsibilities

- Accept semantic search requests through `/api/search`
- Generate query embeddings through the configured embedding provider
- Read vector rows from Iceberg/MinIO
- Score candidates with cosine similarity
- Return ranked source rows and metadata

## Configuration

The phase-one prototype supports two embedding providers:

- `mock`: deterministic local test vectors
- `external`: HTTP embedding service, used by the real local E2E path

Useful environment variables:

```bash
EMBEDDING_PROVIDER=external
EMBEDDING_EXTERNAL_API_URL=http://embedding-service:8000/api/v1/embed-query
ICEBERG_CATALOG_WAREHOUSE=s3a://vectorsync/warehouse
ICEBERG_VECTOR_NAMESPACE=vectorsync
AWS_S3_ENDPOINT=http://minio:9000
SEARCH_DEFAULT_LIMIT=10
SEARCH_MAX_LIMIT=100
SEARCH_MIN_SIMILARITY=0.0
```

For host-local development, copy `.env.host.example` to `.env.host` or run:

```bash
./scripts/start-local.sh
```

That keeps Java services on the host so Maven can reuse the local `~/.m2` cache, while Postgres, MinIO, and the embedding service run in Docker.

## API

```http
POST /api/search
Content-Type: application/json

{
  "query": "lightweight running shoe",
  "sourceTable": "default.products",
  "topK": 5,
  "minSimilarity": 0.0
}
```

Health checks:

```http
GET /actuator/health
GET /api/search/health
```

## Current implementation note

Phase one uses brute-force cosine similarity over the Iceberg vector table. That is acceptable for the prototype and E2E correctness testing, but a later distributed phase should replace this with partition-aware retrieval, ANN indexing, or a coordinator/worker search plan.
