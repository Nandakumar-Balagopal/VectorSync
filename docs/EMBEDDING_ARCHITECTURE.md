# Embedding Architecture: Service vs Worker

## Overview

VectorSync has two separate components for embeddings:
1. **embedding-service** (Python) - The embedding API
2. **embedding-worker** (Java) - The orchestration service

This document explains their relationship and responsibilities.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         VectorSync System                        │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐
│  CDC Worker  │
│   (Java)     │
└──────┬───────┘
       │ publishes CDC events
       │ (new/changed records)
       ▼
┌──────────────────────────────────────────────────────────────────┐
│                      Embedding Worker (Java)                      │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ 1. EmbeddingProcessor                                       │ │
│  │    - Consumes CDC events                                    │ │
│  │    - Batches records (e.g., 100 records)                   │ │
│  │    - Extracts text content                                  │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                    │
│                              ▼                                    │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ 2. EmbeddingProvider (Interface)                            │ │
│  │    - MockEmbeddingProvider (for testing)                    │ │
│  │    - ExternalEmbeddingProvider (HTTP client)                │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                    │
│                              │ HTTP POST /embed                   │
│                              │ {"texts": ["text1", "text2"]}     │
│                              ▼                                    │
└──────────────────────────────┼────────────────────────────────────┘
                               │
                               │
┌──────────────────────────────┼────────────────────────────────────┐
│                              │                                     │
│                              ▼                                     │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │              Embedding Service (Python)                     │  │
│  │                                                              │  │
│  │  FastAPI HTTP Server (Port 8000)                           │  │
│  │                                                              │  │
│  │  Endpoints:                                                  │  │
│  │  - POST /embed                                              │  │
│  │  - GET /health                                              │  │
│  │                                                              │  │
│  │  Providers:                                                  │  │
│  │  - OpenAI (text-embedding-3-small/large)                   │  │
│  │  - Google Gemini (embedding-001)                           │  │
│  │  - Sentence Transformers (self-hosted models)              │  │
│  │  - Custom providers                                         │  │
│  └────────────────────────────────────────────────────────────┘  │
│                              │                                     │
│                              │ returns embeddings                  │
│                              │ {"embeddings": [[0.1, 0.2, ...]]}  │
│                              ▼                                     │
└──────────────────────────────┼────────────────────────────────────┘
                               │
                               │
┌──────────────────────────────┼────────────────────────────────────┐
│                              │                                     │
│                              ▼                                     │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ 3. VectorWriter                                             │  │
│  │    - Writes embeddings to Iceberg vector table             │  │
│  │    - Handles schema and partitioning                        │  │
│  │    - Batch writes for efficiency                            │  │
│  └────────────────────────────────────────────────────────────┘  │
│                              │                                     │
│                              ▼                                     │
│                      ┌───────────────┐                            │
│                      │ Iceberg Table │                            │
│                      │ (S3/MinIO)    │                            │
│                      └───────────────┘                            │
│                                                                    │
│                      Embedding Worker (Java)                      │
└────────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

### Embedding Worker (Java - Port 8082)
**Role**: Orchestration and coordination

**Responsibilities:**
1. **Event Consumption**: Receives CDC events from cdc-worker
2. **Batching**: Groups records into efficient batches (e.g., 100 records)
3. **Text Extraction**: Extracts text content from records
4. **Provider Selection**: Chooses embedding provider (mock or external)
5. **HTTP Client**: Calls embedding-service API
6. **Retry Logic**: Handles failures and retries
7. **Vector Writing**: Writes embeddings to Iceberg
8. **State Management**: Tracks processing progress

**Does NOT:**
- Generate embeddings itself (delegates to embedding-service)
- Implement ML models (that's embedding-service's job)

**Configuration:**
```yaml
embedding:
  provider:
    type: external  # or "mock" for testing
    external:
      url: http://embedding-service:8000/embed
      timeout: 30000
      batch-size: 100
```

---

### Embedding Service (Python - Port 8000)
**Role**: ML model execution and embedding generation

**Responsibilities:**
1. **HTTP API**: Exposes REST endpoints for embedding generation
2. **Model Management**: Loads and manages ML models
3. **Embedding Generation**: Executes ML models to generate vectors
4. **Provider Abstraction**: Supports multiple providers:
   - OpenAI API
   - Google Gemini API
   - Sentence Transformers (local)
   - Custom models
5. **Batching**: Efficient batch processing of texts
6. **Error Handling**: Returns errors for invalid inputs

**Does NOT:**
- Know about Iceberg or VectorSync internals
- Manage CDC events or sync state
- Write to databases

**API Contract:**

Request:
```json
POST /embed
{
  "texts": ["text1", "text2", "text3"],
  "model": "text-embedding-3-small"
}
```

Response:
```json
{
  "embeddings": [
    [0.1, 0.2, 0.3, ...],  // 384 dimensions
    [0.4, 0.5, 0.6, ...],
    [0.7, 0.8, 0.9, ...]
  ],
  "model": "text-embedding-3-small",
  "dimensions": 384
}
```

## Why Two Separate Components?

### 1. Language Specialization
- **Java (embedding-worker)**: Better for enterprise integration, Iceberg, Spring Boot
- **Python (embedding-service)**: Better for ML models, transformers, numpy

### 2. Deployment Flexibility
- **embedding-service** can be:
  - Local (Docker container)
  - SaaS (OpenAI, Gemini)
  - Self-hosted (on GPU servers)
  - Scaled independently

### 3. Provider Abstraction
- embedding-worker doesn't care HOW embeddings are generated
- Can switch providers without changing worker code
- Can use multiple providers simultaneously

### 4. Resource Optimization
- ML models need GPUs → run embedding-service on GPU nodes
- Orchestration doesn't need GPUs → run embedding-worker on CPU nodes

### 5. Reusability
- embedding-service can be used by other systems
- Not tightly coupled to VectorSync

## Deployment Scenarios

### Scenario 1: Local Development (Mock)
```yaml
embedding-worker:
  environment:
    EMBEDDING_PROVIDER_TYPE: mock
```
- No embedding-service needed
- Generates random vectors for testing
- Fast and simple

### Scenario 2: Local Development (Real Embeddings)
```yaml
embedding-service:
  image: vectorsync-embedding-service
  ports:
    - "8000:8000"

embedding-worker:
  environment:
    EMBEDDING_PROVIDER_TYPE: external
    EMBEDDING_PROVIDER_EXTERNAL_URL: http://embedding-service:8000/embed
```
- Runs Python embedding-service locally
- Uses Sentence Transformers (CPU)
- Real embeddings for testing

### Scenario 3: Production (OpenAI)
```yaml
embedding-worker:
  environment:
    EMBEDDING_PROVIDER_TYPE: external
    EMBEDDING_PROVIDER_EXTERNAL_URL: https://api.openai.com/v1/embeddings
    EMBEDDING_PROVIDER_EXTERNAL_API_KEY: sk-...
```
- No embedding-service container needed
- Calls OpenAI API directly
- Pay-per-use pricing

### Scenario 4: Production (Self-Hosted GPU)
```yaml
embedding-service:
  image: vectorsync-embedding-service
  deploy:
    resources:
      reservations:
        devices:
          - driver: nvidia
            count: 1
            capabilities: [gpu]

embedding-worker:
  environment:
    EMBEDDING_PROVIDER_TYPE: external
    EMBEDDING_PROVIDER_EXTERNAL_URL: http://embedding-service:8000/embed
```
- Runs embedding-service on GPU server
- Uses local models (e.g., all-MiniLM-L6-v2)
- No API costs, full control

## Communication Flow

### Step-by-Step Example

1. **CDC Worker** detects 500 new records in Iceberg table
   ```
   CDC Event: {records: [record1, record2, ..., record500]}
   ```

2. **Embedding Worker** receives event
   - Batches into 5 groups of 100 records
   - Extracts text content from each record

3. **Embedding Worker** calls **Embedding Service** (Batch 1)
   ```http
   POST http://embedding-service:8000/embed
   {
     "texts": ["text1", "text2", ..., "text100"],
     "model": "text-embedding-3-small"
   }
   ```

4. **Embedding Service** processes request
   - Loads model (if not cached)
   - Generates embeddings using ML model
   - Returns 100 vectors

5. **Embedding Worker** receives response
   ```json
   {
     "embeddings": [[...], [...], ..., [...]],  // 100 vectors
     "dimensions": 384
   }
   ```

6. **Embedding Worker** writes to Iceberg
   - Combines embeddings with metadata
   - Writes batch to vector table
   - Updates sync state

7. Repeat steps 3-6 for remaining 4 batches

## Configuration Examples

### Using Mock Provider (No embedding-service)
```yaml
# embedding-worker application.yml
embedding:
  provider:
    type: mock
```

### Using Local Embedding Service
```yaml
# embedding-worker application.yml
embedding:
  provider:
    type: external
    external:
      url: http://embedding-service:8000/embed
      timeout: 30000
      batch-size: 100
```

### Using OpenAI
```yaml
# embedding-worker application.yml
embedding:
  provider:
    type: external
    external:
      url: https://api.openai.com/v1/embeddings
      api-key: ${OPENAI_API_KEY}
      model: text-embedding-3-small
      timeout: 30000
      batch-size: 100
```

## Key Takeaways

1. **embedding-worker** = Orchestrator (Java)
   - Manages workflow
   - Handles Iceberg integration
   - Coordinates batching and retries

2. **embedding-service** = ML Engine (Python)
   - Generates embeddings
   - Manages ML models
   - Provides HTTP API

3. **Separation of Concerns**
   - Worker doesn't know about ML models
   - Service doesn't know about Iceberg
   - Clean interface between them

4. **Flexibility**
   - Can use mock, local, or SaaS providers
   - Can scale independently
   - Can switch providers easily

5. **Optional Dependency**
   - embedding-service is optional
   - Can use mock provider for testing
   - Can use SaaS APIs directly