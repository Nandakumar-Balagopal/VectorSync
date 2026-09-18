# VectorSync

**Incremental, content-addressed embedding derivation over Apache Iceberg.**

Your vectors live in Iceberg tables you already own, queried by Spark or Trino with no VectorSync
process in the query path. As the source table changes, VectorSync keeps the derived embeddings
correct — and re-embeds only content it has never seen.

---

## The idea

An embedding is a pure function of `(content, model, configuration)`.

So a vector is keyed by **content**, not by the row it arrived on. That single decision is what
separates this from a sync pipeline:

- A row whose `price` changed but whose description did not costs **nothing** to re-embed.
- The same text in 500 rows, or across 50 tables, is **one** inference call.
- A data-file rewrite or compaction changes no content, so it costs **nothing** — no re-embedding
  and no index rebuild.
- A model migration re-embeds novel content, not your corpus.

Everything else here exists to avoid calling the model.

## Storage model

Three Iceberg tables, and the separation is the design.

```
TIER 1   embedding_store    one row per distinct content, never recomputed
         content_map        append-only row → content history, with tombstones
            ↓
TIER 2   vectors_<table>_<configId>    flat serving projection, vector inlined
            ↓
TIER 3   clustered_<table>  IVF index as Iceberg partitions
         vector_centroids   centroids per scope
         vector_index_coverage   what content each index covers
```

Tier 1 is normalised because deduplication and lineage demand it. Tier 2 is that join already done,
because a similarity scan cannot afford a join per candidate row. Tier 3 partitions by nearest
centroid so an ordinary query engine performs the candidate reduction an ANN index would.

`config_id` is a 16-hex SHA-256 over the embedding columns, separator, chunker, chunk size and
overlap, model name, model revision, embedding version and normalisation. It deliberately
**excludes** the source table and key columns, so deduplication crosses tables.

## Serving

Vectors are a table. Query them with the engine you already run.

```
GET /api/derive/view?sourceTable=default.products&configId=<cfg>&engine=trino
```

returns the view DDL, and the generated Trino view uses the built-in `cosine_similarity` with a
`cardinality` guard on the query vector:

```sql
SELECT source_row_id, text,
       cosine_similarity(CAST(embedding AS array(double)), q.qv) AS similarity
FROM iceberg.vector.vectors_default_products_a1b2c3d4 v
CROSS JOIN query q
WHERE cardinality(q.qv) = 384
ORDER BY similarity DESC LIMIT 10
```

For cluster-pruned search, rank centroids first and pass the nearest few as **literal** cluster
ids — partition pruning happens at plan time, so a subquery does not prune:

```sql
WHERE cluster_id IN (3, 7)   -- literals, verified: 135 of 2,000 rows read
```

## Measured

`all-MiniLM-L6-v2`, MinIO-backed Iceberg warehouse, single laptop. Distinct content is computed
independently in `bench/corpus.py`; inference calls are the service's own counter.

### Cost tracks content, not rows

| corpus | rows | distinct content | inference calls | avoided |
|---|---|---|---|---|
| dup00 | 10,000 | 10,000 | **10,000** | 0.0% |
| dup50 | 10,000 | 4,335 | **4,335** | 56.6% |
| dup80 | 10,000 | 1,979 | **1,979** | 80.2% |
| dup95 | 10,000 | 500 | **500** | 95.0% |

Inference calls equal distinct content **exactly** in all four runs. In `dup95`, inference plateaus
at 500 by table 50 — tables 51 through 100 cost zero, because their content was already embedded by
earlier tables. Cross-table deduplication is the property a row-keyed design lacks.

The `dup00` row is the control: every row distinct, nothing to reuse, so the design wins nothing and
pays full price.

**Savings are a property of your workload.** On real IR corpora, where documents are unique by
construction, deduplication is near zero: NFCorpus deduplicated 1.1% (3,633 documents to 3,593
vectors) and FiQA 0.00%. The cost argument applies to catalogs, logs, support macros and document
revisions — not to search corpora. On those, the value is the incremental and reproducibility
machinery rather than deduplication.

### Cluster-pruned search on real data

FiQA, 20,000 financial forum posts, 256 clusters, 40 judged queries:

| probes | fraction of table read | recall@10 | nDCG@10 |
|---|---|---|---|
| 1 | 0.5% | 0.412 | 0.121 |
| 8 | **3.3%** | 0.847 | **0.135** |
| 256 (full scan) | 100% | 1.000 | 0.135 |

**8 of 256 clusters gives full-scan retrieval quality while reading 3.3% of the table.**

Verified against real Trino: a full scan read 2,000 rows across 32 partitions; `cluster_id IN (3,7)`
read 82 rows across 2. End to end, the two-step query read 135 of 2,000 rows (6.75%) and returned an
identical top-5.

### Mutation, on real data through the live stack

Visible lag from source commit to queryable: **15.4s p50 / 15.9s p95** — bounded by the scheduler
interval rather than by derivation work. Query latency **77ms p50 / 97ms p95** on the exact
reference scan.

## Change detection

Built on Iceberg's own metadata, ordered by **sequence number** and never by snapshot id (snapshot
ids are random longs).

| Source operation | Behaviour |
|---|---|
| `INSERT` / append | Incremental append scan; only new files are read |
| Compaction, `rewrite_data_files`, sort reorganisation | No re-embedding, and no index rebuild — the content digest is unchanged |
| `DELETE` | Reconciled: the partition is re-derived and vanished keys are tombstoned |
| `UPDATE` / `MERGE` | Reconciled the same way; row-id keying and sequence collapse pick the new version |
| Schema change on an embedded column | New `config_id`, materialised side by side |

Deletes and updates resolve through a key-set sweep: VectorSync asks which keys the source still
holds and tombstones the difference. That reads only current state, so it does not depend on delete
files or on snapshot retention.

## Maintenance

The derived tables commit once per derive pass and each commit writes at least one file per
partition it touches, so they fragment in proportion to how incrementally they are maintained — the
tables that benefit most from incremental derivation are the ones that end up with the most tiny
files, until planning a scan costs more than reading the data.

`TableMaintenance` does three things, cheapest first, all from `iceberg-core` and `iceberg-parquet`
so none of it needs Spark: expire snapshots (bounds `metadata.json`, re-parsed on every catalog load
because caching is deliberately off), rewrite manifests, and rewrite small data files.

Measured on a 900k-row warehouse, one pass of 119 seconds:

| table | data files | records per file | records |
|---|---|---|---|
| `content_map` | 184 → **22** | 5,038 → **42,136** | 927,006 → 927,006 |
| `embedding_store` | 534 → **19** | 1,700 → **47,789** | 908,006 → 908,006 |

677 files eliminated and 1,310 MiB rewritten, with both record counts identical to the byte.
Provenance still resolves a row through the compacted content map to a live vector in the compacted
store.

Compaction is safe for this schema because **nothing derived orders by Iceberg sequence numbers** —
the content map collapses on its own `source_sequence_number` column, and every reader of an Iceberg
sequence number reads it from the source table. A rewrite changes file metadata and no column, and
commits a `replace` snapshot, so an incremental append scan does not see compacted files as new rows
either.

Maintenance **yields to writers**. A table committed to within the last 30 seconds is skipped
entirely: both sides compare-and-set, and only one loses well — maintenance losing costs a tick,
while the derive path losing costs one of a work item's three attempts. Fragmentation is a slow
problem and no amount of it is worth interrupting a writer for.

```
GET  /api/maintenance/status          fragmentation per derived table
POST /api/maintenance/run?compact=    expire + manifests, optionally rewrite data
GET  /api/maintenance/catalog-health  whether the catalog can be committed to at all
```

Rewrite groups are bounded by **rows** as well as bytes, because the payload is vectors: a
384-dimension embedding is about 1.5 KiB in Parquet and roughly 9 KiB on heap as a boxed
`List<Double>`, so Parquet size understates heap by nearly six times.

## Reproducibility

Given a source snapshot and a `config_id`, the vector set is reproducible — and an incremental
rebuild is checkable against a full one.

`ReproducibilityTest` asserts that a single full pass and five incremental batches produce
**identical committed state** and **identical inference counts**. Not equal outputs reached by
embedding the same content repeatedly: the same answer for the same price.

## Control plane

Postgres-backed. Materializations move through
`REGISTERED → VALIDATED → BACKFILLING → LIVE`, with `PAUSED`, `DEGRADED`, `MIGRATING` and
`RETIRING → RETIRED` as needed. Work is leased with `SELECT … FOR UPDATE SKIP LOCKED`, scoped per
materialization, with capped retries and expiry reclaim.

The deduplication record is written in the same transaction that completes the work which produced
it, so "this content has a vector" cannot outlive the commit that wrote it.

Retirement never deletes a vector. The embedding store is shared by every materialization whose text
hashed identically, so per-table deletion is refused; a purge marks rows for the reclaim sweeper,
the only component with the global view to decide that content is referenced by nothing.

## Modules

| Module | Role |
|---|---|
| `common` | DTOs and constants. No Spring. |
| `vectorsync-format` | The Iceberg format layer: content identity, the three tiers, SQL generation. No Spring. |
| `control-plane` | :8080 — admission, state machine, leased work queue, dedup record |
| `worker` | :8081 — change detection, derivation, projection, clustering |
| `embedding-service` | :8000 — FastAPI + sentence-transformers |
| `dashboard` | :3000 — React UI |

`common` and `vectorsync-format` are framework-free, enforced by `maven-enforcer-plugin` rather than
by convention.

## Quick start

```bash
cp .env.example .env
docker compose up -d
./deployment/demo-run.sh
```

Build and test:

```bash
mvn -o clean test-compile && mvn -o clean test
```

On macOS with Colima, the Testcontainers tests need:

```bash
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_RYUK_DISABLED=true
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for conventions, and [docs/](docs/) for the architecture,
the demo walkthrough and the repository map.

### If derivation runs but nothing is ever materialised

Check the catalog first:

```
GET /api/maintenance/catalog-health
```

Iceberg's JDBC catalog commits by compare-and-set against `iceberg_tables`, and the schema that
added view support made `iceberg_type` part of that predicate. A catalog created under the older
schema and later upgraded leaves its existing rows with `iceberg_type` NULL — and those rows can
then **never** satisfy the predicate, so every commit to those specific tables fails forever while
creates, loads and reads all keep working perfectly.

From the inside this is close to undiagnosable: derivation runs, the model is called, vectors are
written, the append raises `CommitFailedException`, the derive path correctly reports that nothing
landed, the control plane correctly spends one of three attempts, and three deterministic failures
later the materialization is `DEGRADED` with "rows did not materialize". Every layer behaves
correctly and the apparent cause — a lost commit race — is the one thing that is not happening.
Newer tables in the same catalog commit fine, which makes it look like contention.

The worker now reports this at startup and on the endpoint above, with the repair:

```sql
UPDATE iceberg_tables SET iceberg_type = 'TABLE' WHERE iceberg_type IS NULL;
```

Relatedly, `content_map` and `embedding_store` are shared by every materialization, which makes them
the most contended tables in the system by construction. They are created with a raised
commit-retry budget (20 retries backing off to 5s, against Iceberg's default of 4), applied to
existing warehouses on load as well as at creation.

## Current limits

Stated plainly, because they shape where this fits:

- **Latency.** Purpose-built vector databases answer in single-digit milliseconds. This reads a
  table: 77–97ms exact, 83–150ms cluster-pruned. It is built for analytical and batch retrieval, and
  it feeds a vector database rather than replacing one.
- **Mutation cost.** A reconcile re-derives the affected source partitions and rebuilds the Tier-2
  projection. Appends are incremental; heavy mutation is proportionally more expensive.
- **Cluster count** is sized `sqrt(n)` from the corpus. There is no adaptive tuning, and the choice
  matters: NFCorpus needed 53% of the table at 16 clusters for the quality it reached at 3.8% with
  64.
- **Authentication** ships behind `vectorsync.auth.enabled`. See [SECURITY.md](SECURITY.md).
- **Tier-2 projection rebuild is the scaling limit, and it is a hard one.** Every publish rebuilds
  the whole projection, so it is O(rows) in both time and memory rather than O(change) — Tier 1 is
  genuinely incremental, Tier 2 is not. Measured at one million rows: Tier 1 absorbed all 1,000,000
  chunks and the derive queue reached the last of 200 files, while the projection never published
  past its first 5,000-row block. It does not fail cleanly. With no container memory limit the
  kernel SIGKILLs the worker; with a 6 GB limit the heap instead sits at 5.09 GiB of 6 with the CPU
  pegged at 235–275% — parallel GC burning two and a half cores while the application advances
  nothing, indefinitely. Roughly 4.5 GB of heap is not enough to publish a million rows, and adding
  RAM only moves the number. The fix is to make the publish streaming or partition-incremental.
  Until then, size Tier-2 scopes to what a publish can hold.
- **Give the worker a container memory limit.** The image sets `-XX:MaxRAMPercentage=75`, which
  computes from the container limit — with no limit set it sizes the heap from the whole host and
  total RSS can exceed it, at which point the kernel sends `SIGKILL` and you get no
  `OutOfMemoryError` and no heap dump. `docker-compose.scale.yml` sets `mem_limit: 6g`.
- **Scale.** Characterised to one million rows on a single laptop with one worker: source seeding at
  ~8,000 rows/s and derivation at ~3,800 rows/s with the mock provider. With the real embedding
  service the model dominates at ~10 rows/s, which puts ten million rows near 278 hours — the
  inference-avoidance numbers above are the reason that matters less than it looks, but it is the
  honest single-node rate. Multi-worker leasing is tested; multi-worker throughput is not.
- **An index does not re-size itself.** If a scope's cluster count stops suiting its corpus, nothing
  re-fits it while the index reads `FRESH`.

## Licence

Apache-2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE) and [CONTRIBUTING.md](CONTRIBUTING.md).
