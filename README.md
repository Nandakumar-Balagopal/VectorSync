# VectorSync

**Versioned, reproducible embeddings for the Iceberg lakehouse.**

Embeddings and vector indexes as first-class data products bound to Iceberg snapshots — so you can
migrate embedding models without downtime, prove which version produced an answer, and roll back
in one commit.

* * *

## 📋 What this is

VectorSync makes Iceberg the system of record for the **whole embedding lifecycle**:

```
generate → version → index → evaluate → promote → query → evolve → rollback
```

- Detect source changes via Iceberg snapshot diffing
- Materialize embeddings tagged with model, version, and preprocessing lineage
- Store vectors in Iceberg — multiple embedding versions coexist
- Build durable, versioned ANN index artifacts with full lineage
- Evaluate a candidate index before it serves traffic
- Promote and roll back with a single Iceberg commit
- Trace any search result back to the source row as it was when embedded

## 🎯 What this is not

Not "a vector database that stores in Iceberg." It does not compete on ANN latency, and it is not
a sub-10ms serving engine. Object storage has a latency floor, and purpose-built vector databases
will beat it there.

What it offers instead is the thing they are bad at: **model migration with lineage, evaluation,
and rollback.** Re-embedding a large corpus with a new model is normally a re-ingest with no
lineage, no A/B, and no way back. Here it is a version bump.

* * *

## 🏗️ Architecture

```
              Iceberg source table ── snapshot N
                         │
                         ▼
        ┌────────────────────────────────┐
        │  Embedding Materializer        │  worker :8081
        │  diff(N-1, N) → batch embed    │
        └────────────────┬───────────────┘
                         ▼
   ╔══════════ ICEBERG (system of record) ══════════════╗
   ║  vector_embeddings      vectors × model version    ║
   ║  vector_index_manifest  artifacts + lineage        ║
   ║  vector_index_alias     what serves production     ║
   ╚════════════════════════┬═══════════════════════════╝
                            ▼
        ┌────────────────────────────────┐
        │  Index Builder (Lucene HNSW)   │  search-service :8083
        └────────────────┬───────────────┘
                         │ artifacts → object storage
        ┌────────────────┼──────────────┬────────────────┐
        ▼                ▼              ▼                ▼
    Serving          Evaluation    Provenance      Exact search
  alias→manifest   recall vs      result→index    the ground truth
  →artifact        exact KNN      →model→row
```

Full detail in **[docs/architecture.md](docs/architecture.md)**.

### Modules

| Module | Port | Role |
|---|---|---|
| `vectorsync-format` | — | Canonical format contract. **No framework dependencies** — usable from Spark or a Trino plugin |
| `common` | — | Pure DTOs, constants, cosine utility |
| `control-plane` | 8080 | Table + embedding-version registry, sync watermark |
| `worker` | 8081 | Snapshot diff → batch embed → versioned write |
| `search-service` | 8083 | Index build, alias serving, evaluation, provenance |
| `embedding-service` | 8000 | Python FastAPI, sentence-transformers or managed API |
| `dashboard` | 3000 | React UI |

* * *

## 🚀 Quick start

```bash
# full stack in containers
docker compose --profile local-storage --profile local-embedding up -d --build

# or: infra in Docker, Java on the host (faster iteration)
./scripts/start-local.sh
```

Then walk the lifecycle: **[docs/LIFECYCLE.md](docs/LIFECYCLE.md)**.

Shortest possible loop:

```bash
curl -s -X POST localhost:8081/api/demo/seed
curl -s -X POST localhost:8080/api/tables/register -H 'Content-Type: application/json' \
  -d '{"catalog":"default","tableName":"default.products",
       "embeddingColumns":["name","description"],
       "modelName":"all-MiniLM-L6-v2","embeddingVersion":"v1","enabled":true}'
curl -s -X POST localhost:8081/api/demo/sync
curl -s -X POST localhost:8083/api/lifecycle/index/build -H 'Content-Type: application/json' \
  -d '{"sourceTable":"default.products","modelVersion":"all-MiniLM-L6-v2:v1"}'
# promote the returned indexId, then search
curl -s -X POST localhost:8083/api/search -H 'Content-Type: application/json' \
  -d '{"query":"affordable shoes","topK":5,"sourceTable":"default.products"}'
```

* * *

## 🔌 API

**Lifecycle** (`search-service`)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/lifecycle/index/build` | Build an index over one `(table, model:version)` |
| `POST` | `/api/lifecycle/promote` | Point production at an index — one commit |
| `POST` | `/api/lifecycle/rollback` | Restore the previously served index |
| `POST` | `/api/lifecycle/evaluate` | Index recall vs exact search, plus precision if labelled |
| `GET` | `/api/lifecycle/indexes` | All index artifacts with lineage and metrics |
| `GET` | `/api/lifecycle/promoted` | What currently serves |
| `GET` | `/api/lifecycle/history` | Full promotion and rollback history |
| `GET` | `/api/lifecycle/model-versions` | Which embedding versions are materialized |

**Search**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/search` | Serve through the promoted index |
| `POST` | `/api/search/index/{indexId}` | Query a specific version without promoting it |
| `POST` | `/api/search/exact` | Exhaustive cosine scan — the evaluation ground truth |

**Provenance**

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/provenance/vector/{vectorId}` | Why a result exists, back to the source row |
| `GET` | `/api/provenance/row` | Every stored embedding version of one row |

**Tables** (`control-plane`)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/tables/register` | Register a table at an embedding version |
| `PUT` | `/api/tables/{tableId}` | Bump `embeddingVersion` — starts a migration |
| `GET` | `/api/tables` | Registered tables |

* * *

## ⚙️ Configuration

Profiles: `local-storage` (MinIO), `local-embedding` (Python service). Neither for external S3 and
a managed embedding API.

Files: `.env` (all-in-Docker), `.env.host.example` (host-local dev, copied to `.env.host` by
`scripts/start-local.sh`).

Key variables:

- `ICEBERG_CATALOG_WAREHOUSE`, `AWS_S3_ENDPOINT`, `AWS_S3_ACCESS_KEY`, `AWS_S3_SECRET_KEY`, `AWS_REGION`
- `EMBEDDING_PROVIDER` — `mock` or `external`
- `EMBEDDING_EXTERNAL_API_URL` — single-text endpoint
- `EMBEDDING_EXTERNAL_BATCH_API_URL` — batch endpoint; set this, it is far faster
- `VECTORSYNC_INDEX_BASE_URI` — artifact location (defaults to `<warehouse>/indexes`)

> `EMBEDDING_PROVIDER` defaults to `mock`, which returns **random vectors**. Search results are
> meaningless until you set it to `external`.

* * *

## 🧪 Tests

```bash
mvn clean verify          # 74 unit + integration tests, no Docker needed
```

The format and search-service suites drive the full lifecycle against a real Iceberg catalog on the
local filesystem: insert/update/delete resolution, two coexisting embedding versions, index build,
promote, serve, evaluate, roll back, and provenance.

With Docker, against real embeddings (both verified passing on Colima):

```bash
deployment/test-e2e-lifecycle.sh        # the full lifecycle loop, 8 stages
deployment/test-e2e-real-embeddings.sh  # CDC insert/update/delete correctness
```

* * *

## ✅ Implemented

- Engine-neutral format module with no framework dependencies
- Snapshot-based CDC with insert / update / delete correctness
- Batch embedding generation
- Format v2: typed lineage columns, float32 vectors, partitioned by model version
- Deterministic, lineage-derived vector identity — retries are idempotent
- Resolution ordered by Iceberg's monotonic snapshot sequence number, with point-in-time `asOf` reads
- Deletes tombstoned across every materialized embedding version
- Durable, versioned index artifacts with a queryable manifest
- Single-commit promotion and rollback with full audit history
- Index recall and retrieval precision as separate, recorded metrics
- Provenance from a search result to the source row via time travel

## 🚧 Limitations

- Index build is single-process; a large corpus needs partition-scoped parallel builds
- Incremental maintenance rebuilds a whole `(table, model_version)`, not a partition
- No chunking: one source row is one chunk (`chunk_ordinal` exists but is always 0)
- Text is duplicated across embedding versions of the same row
- Alias promotion assumes a single writer
- ANN indexes are not engine-neutral and cannot be; only metadata and embeddings are
- Dashboard covers overview, lifecycle, search and configuration; there is no evaluation or
  provenance UI yet (both are API-only)

## 🗺️ Next

- Partition-scoped parallel index builds
- Chunking: split a row into many chunks, each independently embedded
- Quantized embeddings (int8) to make very large version coexistence affordable
- Batch semantic operations on Spark — similarity join, dedup, clustering
- Dashboard: evaluation comparison and a provenance viewer

* * *

## 📄 License

Apache 2.0
