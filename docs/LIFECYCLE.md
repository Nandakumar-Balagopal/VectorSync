# Embedding lifecycle walkthrough

How to migrate a table from one embedding version to another without downtime, evaluate the
candidate before it serves traffic, and roll back if it disappoints.

The automated version of this is `deployment/test-e2e-lifecycle.sh`.

## 0. Start the stack

```bash
docker compose --profile local-storage --profile local-embedding up -d --build
```

Ports: control-plane 8080, worker 8081, search-service 8083, embedding-service 8000.

## 1. Register a table at v1

```bash
curl -s -X POST localhost:8080/api/tables/register \
  -H 'Content-Type: application/json' \
  -d '{"catalog":"default","tableName":"default.products",
       "embeddingColumns":["name","description"],
       "modelName":"all-MiniLM-L6-v2","embeddingVersion":"v1","enabled":true}'
```

Note the returned `tableId`. Then materialize:

```bash
curl -s -X POST localhost:8081/api/demo/seed
curl -s -X POST localhost:8081/api/demo/sync
```

The worker diffs source snapshots, embeds every changed row **in one batch call**, and appends
vectors tagged `all-MiniLM-L6-v2:v1`.

## 2. Build and promote an index

```bash
curl -s -X POST localhost:8083/api/lifecycle/index/build \
  -H 'Content-Type: application/json' \
  -d '{"sourceTable":"default.products","modelVersion":"all-MiniLM-L6-v2:v1"}'
```

Returns a manifest entry with an `indexId`. The artifact is durable in object storage; the entry
records the source snapshot, model, version, algorithm parameters, metric, dimension, file list,
and vector count.

```bash
curl -s -X POST localhost:8083/api/lifecycle/promote \
  -H 'Content-Type: application/json' \
  -d '{"sourceTable":"default.products","indexId":"<indexId>","promotedBy":"me","note":"initial"}'
```

Searches now serve from that index. Before anything is promoted, search falls back to an exact
scan rather than failing.

## 3. Start a migration

Bump the version. The existing v1 embeddings are **not** touched.

```bash
curl -s -X PUT localhost:8080/api/tables/<tableId> \
  -H 'Content-Type: application/json' -d '{"embeddingVersion":"v2"}'
curl -s -X POST localhost:8081/api/demo/sync
```

Changing the version resets the sync watermark, because a new embedding version is a fresh
materialization of data the worker has already seen — without the reset, incremental CDC would
find no source changes and v2 would never be produced.

Both versions now coexist:

```bash
curl -s "localhost:8083/api/lifecycle/model-versions?sourceTable=default.products"
```

## 4. Evaluate the candidate

Build an index over v2, then evaluate both. `relevantSourceRowIds` is optional; omit it to measure
index recall only.

```bash
curl -s -X POST localhost:8083/api/lifecycle/index/build \
  -H 'Content-Type: application/json' \
  -d '{"sourceTable":"default.products","modelVersion":"all-MiniLM-L6-v2:v2"}'

curl -s -X POST localhost:8083/api/lifecycle/evaluate \
  -H 'Content-Type: application/json' \
  -d '{"indexId":"<indexId>","topK":10,"queries":[
        {"query":"lightweight running shoe","relevantSourceRowIds":["p-100"]},
        {"query":"waterproof hiking boot","relevantSourceRowIds":["p-300"]}]}'
```

Two numbers come back, and they mean different things:

- `indexRecallAtK` — overlap with an exhaustive scan. Measures the **index**. No labels needed.
- `precisionAtK` — against your labels. Measures the **embedding model**.

A candidate can have perfect index recall and worse retrieval quality. Promote on precision;
investigate low index recall as a build-parameter problem.

You can also query a specific index directly, without promoting it:

```bash
curl -s -X POST localhost:8083/api/search/index/<indexId> \
  -H 'Content-Type: application/json' -d '{"query":"running shoe","topK":5}'
```

## 5. Promote, or roll back

```bash
# promote the candidate
curl -s -X POST localhost:8083/api/lifecycle/promote \
  -H 'Content-Type: application/json' \
  -d '{"sourceTable":"default.products","indexId":"<v2 indexId>",
       "promotedBy":"me","note":"recall@10 0.81 vs 0.72"}'

# and if it disappoints
curl -s -X POST "localhost:8083/api/lifecycle/rollback?sourceTable=default.products&rolledBackBy=me"
```

Each is a single Iceberg commit. Every promotion is retained:

```bash
curl -s "localhost:8083/api/lifecycle/history?sourceTable=default.products"
```

## 6. Explain a result

```bash
curl -s localhost:8083/api/provenance/vector/<vectorId>
```

Walks the chain: vector -> serving alias -> index -> embedding model and version ->
source snapshot -> **the source row as it was at that snapshot**, read via Iceberg time travel.

Per-row embedding history:

```bash
curl -s "localhost:8083/api/provenance/row?sourceTable=default.products&sourceRowId=p-100"
```

## Operational notes

**Format version.** The vector table records `vectorsync.format-version`. A table written by an
older build is refused with instructions rather than dropped — embeddings are derived data and can
be rebuilt, but destroying them must be an explicit decision:

```bash
curl -s -X POST localhost:8081/api/admin/vector-table/rebuild
```

**Sync watermark.** It advances only when every change event for a snapshot materialized *and* the
vector write committed. A failed batch holds the watermark so the changes are retried rather than
silently skipped.

**Deletes span versions.** Deleting a source row tombstones it under every materialized embedding
version, not just the configured one — otherwise it would stay discoverable through older versions.
Already-built index artifacts are immutable and still contain the row, so rebuild an index if it
must not be reachable there either.

**Cleaning up artifacts.** Superseded indexes stay in the manifest for rollback and audit. Evict a
cached artifact from a running service with
`POST /api/lifecycle/index/{indexId}/evict`; this releases handles only and does not delete files.
