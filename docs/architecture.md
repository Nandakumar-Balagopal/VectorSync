# Architecture

VectorSync treats embeddings as a **derived data product keyed by content**, with Iceberg as the
system of record for every tier. Nothing about a vector depends on where its row happens to live.

```
source Iceberg table
        |
        |  change detection (incremental append scan, sequence-ordered)
        v
TIER 1  embedding_store     one row per (content_hash, model_version, config_id)
        content_map         append-only row -> content history, with tombstones
        |
        |  projection
        v
TIER 2  vectors_<table>_<configId>    one row per live chunk, vector inlined
        |
        |  spherical k-means, partitioned by nearest centroid
        v
TIER 3  clustered_<table>   IVF index expressed as Iceberg partitions
        vector_centroids    centroids per scope
        vector_index_coverage   content digest per scope
        |
        v
Spark / Trino read Tier 2 or Tier 3 directly. No VectorSync process in the query path.
```

## Content identity

The hinge of the design.

`content_hash` is a full-width SHA-256 over the canonical text: the embedding columns in spec order,
joined by the configured separator. The separator is a real ASCII unit separator, so column
boundaries cannot be forged by moving text across them — `("ab","c")` and `("a","bc")` are different
content.

`config_id` is 16 hex of SHA-256 over everything that changes the output: embedding columns, join
separator, chunker, chunk size, chunk overlap, model name, model revision, embedding version,
normalisation. It **excludes** the source table and the key columns, which is what lets deduplication
cross tables. Including the table name would give every table a private key space and embed identical
text twice.

`storeKey` is `(content_hash, model_version, config_id)`. The same text under a different model or a
different configuration is a different vector, and both coexist.

## Tier 1 — canonical

`embedding_store` holds one row per distinct content, partitioned by `identity(model_version)`. It is
append-only and never recomputed: it is the artifact that costs money.

It once also partitioned by `hash_prefix`, a two-character bucket of the content hash. That was
measured a net negative on both sides — a realistic probe's prefix set covered all 256 buckets, so
reads degraded to full scans, and writes fragmented into tens of thousands of tiny files — and was
retired from the spec. The column remains in the schema at field id 4 because Iceberg resolves
columns by field id and historical partition specs still source it; removing it would make the table
unreadable.

`content_map` is the row-to-content history, partitioned by `identity(source_table, config_id)`. It
is append-only with tombstones, so it is a history rather than a cache: resolution collapses to the
newest entry per chunk by sequence number, and tombstones are filtered only *after* resolution so an
older live entry cannot resurrect a deleted row.

## Tier 2 — serving projection

`vectors_<table>_<configId>` is the Tier-1 join already performed: one row per live row/chunk with
the vector inlined. Derived and disposable — every column is reproducible from Tier 1, which is what
makes a rebuild an ordinary operation.

Published as a single snapshot: a scoped `overwriteByRowFilter` deletes the previous contents and the
new files land in the same commit, so a reader sees the old projection or the new one and never a
mixture. The filter names only identity-partition columns, because Iceberg deletes whole files by row
filter and refuses one it cannot prove covers a file entirely.

A publish is skipped when nothing changed: if the snapshot summary's config id matches, its
unresolved-row count is zero, and the content map's latest sequence number equals the summary's, the
projection on disk is already correct and no snapshot is written.

## Tier 3 — IVF as partitions

An Iceberg-native ANN index is not readable by any engine today. Partitioning is implemented
everywhere. So each vector is assigned to its nearest centroid and the table is partitioned by
`identity(model_version, config_id, cluster_id)`; a query that probes the nearest few clusters reads
only those partitions.

The approximation lives entirely in the partition predicate. Scoring inside a probed partition is
exact, so a missed neighbour is explainable: it sat in a cluster that was not probed.

**Index identity is content, not a snapshot.** `vector_index_coverage` records a digest over the
sorted distinct content hashes a scope was built from. A build recomputes it and, when it matches,
returns without refitting and without committing. Compaction, a data-file rewrite, a sort
reorganisation and a partition rewrite all produce a new Iceberg snapshot and change no content, so
they leave the digest identical and the existing index provably correct.

When content is added and none removed, within a bounded fraction of the scope, the new content is
assigned against the centroids already on disk and appended — nothing is relabelled. Growth past that
fraction, measured against the size at the last refit, triggers a full refit instead, which bounds
centroid drift.

## Change detection

Ordered by Iceberg **sequence number**, never by snapshot id: snapshot ids are random longs, so
ordering by one shuffles history and lets a tombstone lose to the row it was meant to retire.

An incremental append scan reads only files added since the anchor. A range containing deletes or
overwrites cannot be described that way, so it is routed to a reconcile: the affected source is
re-derived at a pinned snapshot, and then a key-set sweep tombstones every mapped row the source no
longer holds.

The sweep asks which keys the source still holds rather than trying to read what was deleted.
Positional deletes and deletion vectors carry `(file_path, position)` and no key values, so a delete
file yields an offset rather than a row id — and the data file it references may already have been
removed by `expire_snapshots`. Asking about current state avoids both problems.

## Control plane

Postgres, with Flyway migrations. `materializations` carries the state machine; `work_items` is a
leased queue using `SELECT … FOR UPDATE SKIP LOCKED`, scoped per materialization with capped retries
and expiry reclaim; `embedded_content` is the durable deduplication record.

Two properties make the deduplication record trustworthy. It is written in the same transaction that
completes the work which produced the vector, so "this content has a vector" cannot outlive the
commit that wrote it. And it is shared, so the measured deduplication holds across workers rather
than for one process.

The worker's in-memory hash index is a read-through cache over that record, and caches **positives
only**: a negative is never cached, because a hash absent now may be present a moment later, and a
failed probe propagates rather than returning "not found".

## Catalog

`cache-enabled=false`, deliberately. `CatalogUtil.buildIcebergCatalog` wraps the catalog in a
`CachingCatalog` whenever the property is absent, and its default is true — precisely wrong for a
system whose entire job is to notice that a source table has a new snapshot.

HadoopCatalog is refused on object storage: its commit is a filename rename with no atomicity, so two
writers can both succeed with one silently lost. Raising `vectorsync.runner.parallelism` above 1 on a
hadoop catalog refuses to start for the same reason.

## Concurrency

The derivation cycle fans out over **configuration groups**, not materializations. `config_id`
excludes the source table, so several materializations routinely share one, and two that share one
address the same Tier-1 content space — derived at once, both would probe the deduplication record
before either had written and both would pay for the same inference. Groups run in parallel; members
of a group run in sequence.
