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
- **Derived-table compaction** is not yet automatic; long-running deployments accumulate small files.
- **Scale.** Measured on a single laptop with one worker. Multi-worker leasing is tested; throughput
  at warehouse scale is not yet characterised.

## Licence

Apache-2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE) and [CONTRIBUTING.md](CONTRIBUTING.md).
