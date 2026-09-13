# Architecture

VectorSync treats embeddings and vector indexes as **versioned data products bound to Iceberg
snapshots**, with Iceberg as the system of record for the whole lifecycle:

```
generate -> version -> index -> evaluate -> promote -> query -> evolve -> rollback
```

It is deliberately not a vector database that happens to store in Iceberg. The value is
reproducibility, auditability, and interoperability — not ANN latency.

## Components

```
              Iceberg source table -- snapshot N
                         |
                         v
        +--------------------------------+
        |  Embedding Materializer        |  worker :8081
        |  diff(N-1, N) -> batch embed   |
        +----------------+---------------+
                         v
   === ICEBERG (system of record) =======================
     vector_embeddings      vectors x (row, chunk, model version)
     vector_index_manifest  index artifacts + full lineage
     vector_index_alias     append-only log of what serves production
   ======================+===============================
                         v
        +--------------------------------+
        |  Index Builder (Lucene HNSW)   |  search-service :8083
        +----------------+---------------+
                         |  artifact files -> object storage
        +----------------+----------------+-----------------+
        v                v                v                 v
    Serving          Evaluation       Provenance        Exact search
  alias -> manifest  recall vs        result -> index   exhaustive cosine,
  -> artifact        exact KNN        -> model -> row    the ground truth

   control-plane :8080  table + version registry, sync watermark (PostgreSQL)
   embedding-service :8000  FastAPI, sentence-transformers or a managed API
```

## What lives where

| Store | Holds | Why |
|---|---|---|
| Iceberg | embeddings, index manifest, alias log | Must be versioned, auditable, and readable by any engine |
| Object storage | index artifact binaries | Opaque blobs; referenced by path from the manifest |
| PostgreSQL | table config, embedding version, sync watermark | Operational state only; nothing that needs auditing |

## The vector table

`vector.vector_embeddings`, format version 3, partitioned by `(source_table, model_version)` so
reading one embedding version prunes partitions instead of scanning every version ever built.

| Column | Type | Notes |
|---|---|---|
| `vector_id` | string | SHA-256 of the lineage below; deterministic, so retries are idempotent |
| `source_table`, `source_row_id` | string | Source identity |
| `source_snapshot_id` | long | Which source snapshot this derives from. **Identity only — Iceberg snapshot ids are random longs, never an order** |
| `source_sequence_number` | long | Iceberg's per-table monotonic snapshot sequence. **This is the ordering key** |
| `source_committed_at` | long | Commit time of the source snapshot, from table metadata. Tiebreak only |
| `chunk_ordinal` | int | Position in the row's chunk sequence; 0 when the row is one chunk |
| `embedding_model`, `embedding_version` | string | Model identity; versions coexist |
| `model_version` | string | Synthetic `model:version` partition value |
| `embedding_dim` | int | |
| `preprocessing_id` | string | Hash of the text-construction config |
| `embedding` | `list<float>` | float32: models emit float32, so float64 doubled cost for nothing |
| `text` | string, optional | Absent on a tombstone |
| `deleted` | boolean | Typed, not a metadata string |
| `metadata` | `map<string,string>`, optional | User passthrough only; lineage is typed |
| `created_at` | timestamptz | Operational; never decides which version is live |

Field IDs are part of the on-disk contract and must never be renumbered — Iceberg resolves columns
by ID, not name.

### Resolution

The table is append-only, so readers collapse history to a current view. The rule lives in exactly
one place, `VectorResolution`, because every reader must agree on it:

- key: `(source_table, source_row_id, chunk_ordinal, model_version)`
- order: `source_sequence_number`, then `source_committed_at`, then `created_at`
- a `deleted` winner hides the row

**Do not order by `source_snapshot_id`.** Iceberg snapshot ids are random longs, so comparing them
numerically reverses history roughly half the time: a real run produced snapshot
`7139976223410259010` followed by `2135807640327332542`, and a delete tombstone lost to the row it
was meant to remove. The sequence number is the only field the Iceberg spec guarantees to be
ordered.

Nor wall clock. `created_at` is metadata about the pipeline run, is non-deterministic across
machines, and cannot answer "what was live at source version N?". It survives only as a final
tiebreak.

`liveVectorsAsOf(sequenceNumber)` therefore always returns the same set for the same inputs, which
is what makes a materialization reproducible rather than merely current.

### Deletes span every version

A deleted source row is tombstoned under **every** materialized embedding version, not just the
configured one. Resolution is keyed by model version, so tombstoning only the current version would
leave the row live — and discoverable — through every older version and its index.

## Index artifacts

An index covers exactly one `(source_table, model_version)`. That makes the artifact its own
filter, so search needs no post-filtering, and it makes builds parallel and incremental
maintenance mean "rebuild the affected partitions".

Artifacts are Lucene HNSW directories uploaded through Iceberg's `FileIO`, so the same code path
serves an `s3a://` warehouse and a local filesystem. They are immutable and content-addressed by
index id, which makes caching trivially safe and means a promotion needs no cache invalidation —
the next query simply resolves a different id.

**Not Puffin.** Puffin has no standardized ANN blob type and no engine reads one, so it would buy
zero interoperability today while coupling index lifecycle to table-metadata commits. A plain
manifest table is queryable from any Iceberg engine now, and remains the migration path if the
spec later standardizes a vector index blob.

## Promotion

`vector_index_alias` is an append-only log of `(alias, source_table) -> index_id`. Promotion is one
Iceberg commit; rollback is another append naming the earlier index. Nothing is mutated, so the
promotion history doubles as an audit trail and `previous()` gives the rollback target directly.

Alias ordering *is* by wall clock, which is correct here in a way it is not for vectors: a
promotion genuinely is an operational event in time. This assumes a single promoting writer.

## Evaluation

Two different measurements, kept separate because conflating them produces misleading promotion
decisions — a new model can have perfect index recall while retrieving worse documents:

- **index recall@k** — overlap with an exhaustive cosine scan. No labels needed. Measures the index.
- **precision@k** — against judged relevance. Measures the embedding model.

The exhaustive scan is scoped to the index's own model version. Scanning every version at once
mixes embedding spaces — a query embedded with one model scored against vectors from another — and
returns each row once per version, which understates recall.

Metrics are written back onto the manifest entry, so the numbers a promotion was based on stay
attached to the artifact.

## Interoperability, scoped honestly

- engine-neutral **metadata** — available today; the manifest and alias log are plain Iceberg tables
- engine-neutral **embeddings** — available today; a fixed-width vector column any engine can read
- engine-neutral **ANN index** — **not** available; this needs connector-level index pushdown that
  does not exist in Trino. Exact search from any engine does work, which is sufficient for batch
  semantic operations.

`vectorsync-format` therefore carries no framework dependencies, so a Spark job or a future Trino
plugin shares the same format definition as the Spring services.

## Known limitations

- Index build is single-process; a very large corpus needs partition-scoped parallel builds
- Incremental index maintenance rebuilds a whole `(table, model_version)` rather than a partition
- No chunking yet: one source row is one chunk (`chunk_ordinal` exists but is always 0)
- Text is stored per model version, so it is duplicated across versions of the same row
- Alias promotion assumes a single writer
