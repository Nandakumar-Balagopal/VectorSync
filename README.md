# VectorSync

**Semantic vector search for Apache Iceberg, continuously synced.**

Automatically detect changes in Iceberg tables, generate embeddings, and enable semantic search — all without moving your data.

* * *

## 📋 What This Does

VectorSync brings **AI capabilities to your lakehouse**:

-   Detect changes in Iceberg tables (snapshot-based CDC)
-   Generate embeddings automatically
-   Store vectors in Iceberg (no external DB)
-   Query data using semantic search

* * *

## 🎯 Why This Exists

Modern data stacks already use **Apache Iceberg + object storage**, but enabling:

-   semantic search
-   RAG pipelines
-   similarity matching

usually requires:

- exporting data to vector databases  
- maintaining sync pipelines  
- duplicating data

* * *

### ✨ VectorSync solves this

> Keep your data where it is. Add semantic search on top.

* * *

## 🚦 Project Phase

VectorSync is currently in **Phase 1: single-cluster MVP**.

Phase 1 uses a combined `worker/` service that performs Iceberg change detection, embedding generation, and vector writes in one process. This keeps the first working version simple enough to test end-to-end while preserving the long-term direction: a distributed coordinator and worker-pool architecture inspired by systems like Presto, Trino, Spark, and Flink.

Phase 1 now has a working end-to-end correctness path for:

- `INSERT`: new source rows produce new vector records.
- `UPDATE`: changed source rows replace or supersede the old vector representation.
- `DELETE`: removed source rows are removed, tombstoned, or excluded from semantic search results.

The current implementation has been manually verified against real Iceberg snapshots with the live embedding service. The next milestone is to turn that manual flow into an automated integration test and then replace table-level diffing with distributed Iceberg file-level tasks.

* * *

## 🏗️ Architecture

### Phase 1: Current Working Architecture

The current working system is intentionally compact:

```
Dashboard / API clients
        │
        ▼
Control Plane ───────────► PostgreSQL
   │
   │ table configs + sync state
   ▼
Worker
   │
   ├── reads Iceberg source tables
   ├── detects snapshot changes
   ├── generates embeddings
   └── writes Iceberg vector tables
        │
        ▼
Search Service
        │
        ▼
Semantic search results
```

In Phase 1, the `worker/` module is the real end-to-end execution path. Planned distributed coordinator/worker pieces should be added as new modules only when the durable task model exists.

### Modular Monorepo Structure

VectorSync is organized as a clean, scalable Maven multi-module project:

```
vectorsync/
├── common/              # Shared DTOs, models, utilities
├── control-plane/       # Metadata management, scheduler, APIs
├── worker/              # Iceberg CDC polling, embedding generation, vector writes
├── search-service/      # Semantic search and retrieval
├── dashboard/           # React frontend (Carbon Design System)
├── embedding-service/   # Python embedding service (optional)
├── docker-compose.yml   # Multi-service orchestration
└── docs/                # Architecture and guides
```

### Component Responsibilities

#### 1. **common/** - Shared Library
- DTOs and data models
- Event models
- Constants and utilities
- Vector similarity helpers
- Common exceptions
- Shared configuration objects

**No business logic** - pure shared code only.

#### 2. **control-plane/** - Control & Coordination (Port 8080)
- Table registration APIs
- Metadata management (PostgreSQL)
- Sync state tracking
- Phase 1 scheduler metadata
- Future distributed coordinator responsibilities:
  - Worker registration and heartbeats
  - Task planning from Iceberg snapshots/manifests
  - Task leases and reassignment
  - Retry and failure recovery
  - Backpressure and capacity-aware scheduling

#### 3. **worker/** - Sync Worker (Port 8081)
- Polls registered Iceberg tables
- Detects snapshot changes
- Generates embeddings via the configured provider
- Writes vector records back to Iceberg
- Exposes demo and vector inspection endpoints

Future split workers should be introduced behind a coordinator/task-lease abstraction rather than as disconnected experimental services.

#### 4. **search-service/** - Semantic Search (Port 8083)
- Semantic search REST API
- Query embedding generation
- Cosine similarity search
- Row lookup and retrieval
- Response ranking
- Future: ANN index integration (FAISS/HNSW)

#### 5. **dashboard/** - Frontend (Port 3000)
- React + TailwindCSS + Carbon Design System
- Dashboard overview
- Registered tables management
- Table details and sync status
- Semantic search interface

### Phase 1 Logical Flow

```
┌─────────────────┐
│ Iceberg Table   │
│  (Source Data)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐
│  Control Plane  │◄────►│   PostgreSQL     │
│  (Metadata API) │      │   (Metadata)     │
└────────┬────────┘      └──────────────────┘
         │ table configs + sync state
         ▼
┌─────────────────┐      ┌──────────────────┐
│     Worker      │◄────►│ Embedding API    │
│ CDC + Embed +   │      │ Mock/Local/SaaS  │
│ Vector Writes   │      │                  │
└────────┬────────┘      └──────────────────┘
         │ writes vectors
         ▼
┌─────────────────┐
│ Vector Table    │
│   (Iceberg)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Search Service  │
│ (Semantic API)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Dashboard     │
│   (React UI)    │
└─────────────────┘
```

### Phase 2+ Planned Distributed Architecture

The long-term architecture should evolve from a single combined worker into a coordinator-driven distributed execution model:

```
Dashboard / API clients
        │
        ▼
Control Plane / Coordinator
        │
        ├── creates sync jobs
        ├── splits jobs into Iceberg-aware tasks
        ├── leases tasks to workers
        ├── tracks progress and retries
        └── marks snapshots fully vectorized
        │
        ▼
Distributed Worker Pool
        │
        ├── Worker 1: process data-file task A
        ├── Worker 2: process data-file task B
        └── Worker 3: process data-file task C
        │
        ▼
Iceberg Vector Tables
        │
        ▼
Search Service / Index Layer
```

The target design is:

> Trino/Presto distributes SQL over lakehouse tables. Spark distributes compute over lakehouse data. VectorSync should distribute semantic indexing over Iceberg tables.

The coordinator should be Iceberg-aware. Instead of pushing generic queue messages, it should plan work from Iceberg metadata:

- Current and previous snapshots
- Manifest files
- Added, removed, and rewritten data files
- Delete files
- Partition specs
- Schema evolution
- Embedding model/version

The preferred distributed work unit is an **Iceberg data-file task**. File-level tasks provide better parallelism than table-level or snapshot-level tasks while avoiding the overhead of row-level scheduling.

Future orchestration metadata should include:

```
workers
- worker_id
- hostname
- status
- last_heartbeat_at
- max_concurrent_tasks
- current_task_count

sync_jobs
- job_id
- table_id
- snapshot_from
- snapshot_to
- status
- created_at
- started_at
- completed_at

sync_tasks
- task_id
- job_id
- table_id
- snapshot_id
- data_file_path
- partition
- operation
- assigned_worker_id
- lease_expires_at
- attempts
- status
- error_message
```

Tasks should be leased, not permanently assigned. If a worker dies, its lease expires and another worker can safely claim the task. Worker execution must be idempotent so retries do not create duplicate live vectors.

Recommended idempotency key:

```
source_table
source_snapshot_id
source_data_file_path
source_record_id
embedding_model
embedding_version
```

### Architecture Diagram

See [docs/architecture.md](docs/architecture.md) for detailed flow diagrams.

* * *

## 🚀 Services

| Service | Port | URL | Description |
| --- | --- | --- | --- |
| **Control Plane** | 8080 | http://localhost:8080 | Metadata, scheduler, APIs |
| **Worker** | 8081 | http://localhost:8081 | CDC, embeddings, vector writes |
| **Search Service** | 8083 | http://localhost:8083 | Semantic search API |
| **Dashboard** | 3000 | http://localhost:3000 | React frontend |
| **Embedding Service** | 8000 | http://localhost:8000 | Python embedding API (optional) |
| **MinIO Console** | 9001 | http://localhost:9001 | S3 storage UI (optional) |

* * *

## 🚀 Quick Start (Docker)

### 1. Choose Your Deployment

VectorSync supports flexible deployment configurations:

| Scenario | Configuration File | Command |
| --- | --- | --- |
| **Local Development** | `.env.local` | `docker-compose --profile local-storage --profile local-embedding up -d` |
| **Production (SaaS)** | `.env.openai` | `docker-compose up -d` |
| **Hybrid** | `.env.hybrid` | `docker-compose --profile local-storage up -d` |

📖 **See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for detailed deployment scenarios.**

* * *

### 2. Quick Local Setup

For a quick local demo with all services:

```bash
# Copy local configuration
cp .env.local .env

# Start all services (MinIO + Embedding Service)
docker-compose --profile local-storage --profile local-embedding up -d --build

# Check service health
docker-compose ps

# View logs
docker-compose logs -f
```

* * *

## 🎬 End-to-End Demo

### 1. Seed demo data

```bash
curl -s -X POST http://localhost:8081/api/demo/seed | jq
```

### 2. Trigger sync

```bash
curl -s -X POST http://localhost:8081/api/demo/sync | jq
```

### 3. Check vector count

```bash
curl -s http://localhost:8081/api/vectors/count
```

### 4. Run semantic search

```bash
curl -s -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{"query":"affordable shoes","topK":5,"sourceTable":"products"}' | jq
```

### 5. Open Dashboard

Visit http://localhost:3000 to explore the UI.

* * *

## ⚙️ Configuration

### Deployment Profiles

VectorSync uses Docker Compose profiles for flexible deployment:

- **`local-storage`** - Starts MinIO for local S3 storage
- **`local-embedding`** - Starts Python embedding service
- **No profiles** - Core services only (use external S3 and SaaS embeddings)

### Configuration Files

Three pre-configured templates are provided:

- **`.env.local`** - Full local stack (MinIO + embedding service)
- **`.env.openai`** - Production with AWS S3 + OpenAI embeddings
- **`.env.hybrid`** - Mix local and cloud services

### Required Variables

**Database:**
- `SPRING_DATASOURCE_URL` - PostgreSQL connection
- `SPRING_DATASOURCE_USERNAME` / `SPRING_DATASOURCE_PASSWORD`

**Iceberg & Storage:**
- `ICEBERG_CATALOG_WAREHOUSE` - S3 warehouse path
- `AWS_S3_ENDPOINT` - S3 endpoint URL
- `AWS_S3_ACCESS_KEY` / `AWS_S3_SECRET_KEY`
- `AWS_REGION`

**Service URLs:**
- `CONTROL_API_URL` - Control plane endpoint (default: http://control-plane:8080)
- `EMBEDDING_EXTERNAL_API_URL` - Embedding service URL

**Embedding Configuration:**
- `EMBEDDING_PROVIDER` - `mock`, `external`, or `local`
- `EMBEDDING_EXTERNAL_TIMEOUT_MS` - API timeout (ms)

📖 **See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for complete configuration reference.**

* * *

## 🛠️ Local Development

### Build All Modules

```bash
mvn clean install
```

### Run Services Individually

```bash
# Control Plane
cd control-plane && mvn spring-boot:run

# Worker
cd worker && mvn spring-boot:run

# Search Service
cd search-service && mvn spring-boot:run

# Dashboard
cd dashboard && npm install && npm run dev
```

### Module Documentation

Each module has its own README with detailed information:

- [common/README.md](common/README.md) - Shared library
- [control-plane/README.md](control-plane/README.md) - Control & scheduler
- [worker/](worker/) - End-to-end sync worker
- [search-service/README.md](search-service/README.md) - Semantic search

* * *

## 🧪 Test Coverage

Generate coverage report:

```bash
mvn clean verify
```

Open:

```bash
target/site/jacoco-aggregate/index.html
```

### End-to-End CDC Correctness Gate

Before Phase 1 is considered complete, VectorSync needs an end-to-end test that runs against a real Iceberg table and verifies all row lifecycle operations:

| Operation | Source Table Action | Expected Vector Behavior | Search Expectation |
| --- | --- | --- | --- |
| `INSERT` | Add a new row with searchable text | A new vector record is written for the source row | The new row appears in relevant semantic search results |
| `UPDATE` | Change one or more embedded text columns | The old vector is replaced, superseded, or marked inactive; the new vector becomes searchable | Queries matching the old text stop returning the stale row; queries matching the new text return it |
| `DELETE` | Delete a source row | The vector is removed, tombstoned, or filtered from search | Deleted rows never appear in semantic search results |

The minimum E2E test should:

1. Start Postgres, MinIO, control-plane, worker, search-service, and dashboard dependencies.
2. Create or seed an Iceberg source table.
3. Register the table with embedding columns and primary key metadata.
4. Run initial sync and verify vector count/search results.
5. Insert a row, sync, and verify the new vector is searchable.
6. Update the row's embedded text, sync, and verify stale text no longer wins while new text does.
7. Delete the row, sync, and verify the deleted row is absent from vector reads and search results.
8. Verify sync state advances only after successful vector writes.
9. Repeat at least one sync to confirm idempotency and no duplicate live vectors.

This should become an automated script or integration test, not a manual demo-only checklist.

* * *

## 🛠 Troubleshooting

### View logs

```bash
docker-compose logs -f worker
docker-compose logs -f search-service
```

### Reset environment

```bash
docker-compose down -v
docker-compose up -d --build
```

### Check service health

```bash
curl http://localhost:8080/actuator/health  # Control Plane
curl http://localhost:8081/actuator/health  # Worker
curl http://localhost:8083/actuator/health  # Search Service
```

* * *

## ✨ Features

### ✅ Implemented

- Modular monorepo architecture (Maven multi-module)
- Snapshot-based change detection for Iceberg tables
- Automatic embedding generation
- Vector storage in Iceberg
- Cosine similarity search
- Configurable embedding providers (mock/external)
- Docker Compose deployment with profiles
- React dashboard with IBM Carbon Design System
- Health checks and monitoring endpoints

### 🚧 Current Limitations

- Brute-force similarity search (ANN index planned)
- CDC correctness for `INSERT`, `UPDATE`, and `DELETE` has been manually verified; automated E2E validation is still needed
- Current runtime uses a single combined worker
- Distributed coordinator, worker leasing, and file-level task assignment are planned

* * *

## 🗺️ Roadmap

### Phase 1 Completion

- ✅ **Manual CDC E2E Test** - Prove insert, update, and delete correctness against real Iceberg snapshots
- 🔄 **Automated CDC E2E Test** - Promote the manual no-mock flow into a repeatable integration test
- 🔄 **Idempotent Vector Writes** - Ensure retries do not create duplicate live vectors
- ✅ **Delete/Tombstone Semantics** - Removed source rows are tombstoned and filtered from vector reads/search
- ✅ **Update Supersession Semantics** - New vectors supersede stale vectors for the same source primary key

### Phase 2: Distributed Coordinator

- 📋 **Coordinator Planning** - Plan sync jobs from Iceberg snapshots, manifests, and data files
- 📋 **Worker Registration** - Track worker capacity and heartbeats
- 📋 **Task Leasing** - Assign file-level tasks with lease expiry and retry
- 📋 **Backpressure** - Avoid overloading embedding providers or object storage

### Phase 3: Performance and Indexing

- 📋 **ANN Indexing** - FAISS/HNSW/Lucene-backed sub-linear search
- 📋 **Embedding Cache** - Redis-based caching for duplicate content
- 📋 **Query Cache** - Cache frequent search queries
- 📋 **Hybrid Search** - Combine semantic + keyword search

### Phase 4: Ecosystem Integrations

- 📋 **Presto Integration** - Query vectors via SQL
- 📋 **Spark Integration** - Batch embedding generation
- 📋 **Multi-Model Support** - Multiple embedding models per table
- 📋 **Auto-Scaling** - Dynamic worker scaling based on load

📖 **See [PERFORMANCE_ROADMAP.md](PERFORMANCE_ROADMAP.md) for detailed optimization plans.**

* * *

## 📚 Documentation

- **[Deployment Guide](DEPLOYMENT_GUIDE.md)** - Deployment scenarios and configuration
- **[Embedding Integration](EMBEDDING_INTEGRATION.md)** - Embedding service architecture
- **[Performance Roadmap](PERFORMANCE_ROADMAP.md)** - Optimization plans
- **[Architecture](docs/architecture.md)** - System architecture and flow
- **Module READMEs** - Detailed documentation for each module

* * *

## 💡 Vision

> Bring vector search natively to the Iceberg lakehouse.

No data movement.  
No separate vector database.  
Just your data — now searchable with AI.

* * *

## 📄 License

Apache 2.0
