# VectorSync — Architecture Review

Investigation only. No production code changed.

Sources: direct repository inspection; a 25-agent adversarial review of this tree (67 findings
verified by two independent verifiers per dimension, 9 refuted); live measurement against the running
stack; and ecosystem research (cited in §19).

---

## 1. Current architecture summary

**Stack.** Java 17, Spring Boot 3.4.4, Maven multi-module, **Apache Iceberg 1.5.0**, Postgres,
MinIO/S3, FastAPI + sentence-transformers, React dashboard.

**Modules.** `common` (DTOs, no Spring) · `vectorsync-format` (Iceberg format layer, no Spring — the
core asset) · `control-plane` :8080 (Postgres, admission, queue, state machine) · `worker` :8081
(change detection, derivation) · `search-service` :8083 (legacy serving + evaluation/provenance) ·
`embedding-service` :8000 · `dashboard` :3000.

**Thesis.** An embedding is a pure function of `(content, model, config)`, so vectors are keyed by
content hash rather than by source row. Cost tracks novel content, not changed rows.

**Data model.**

| Tier | Table | Key | Partitioning |
|---|---|---|---|
| 1 | `embedding_store` | `(content_hash, model_version, config_id)` | `(model_version, hash_prefix)` |
| 1 | `content_map` | `(source_table, source_row_id, chunk_ordinal, config_id)` @ `source_sequence_number` | `(source_table, config_id)` |
| 2 | `vectors_<table>_<configId>` | one row per live chunk, vector inlined | `(source_table, model_version)` |
| 3 | — | not built | — |

`config_id` = 16 hex of SHA-256 over `(embeddingColumns, joinSeparator, chunker, chunkSize,
chunkOverlap, modelName, modelRevision, embeddingVersion, normalize)`. It deliberately **excludes**
source table and key columns so dedup crosses tables.

**Control plane.** `materializations` with state machine
`REGISTERED → VALIDATED → BACKFILLING → LIVE → {MIGRATING, PAUSED, DEGRADED} → RETIRING → RETIRED`;
`work_items` leased via `SELECT … FOR UPDATE SKIP LOCKED` ordered `(priority, created_at)`, scoped
per materialization, capped retries, expiry reclaim on a timer. Schema is JPA `ddl-auto: update` —
**no migration tool**.

**Data flow (what actually runs).** `DerivationScheduler` every 15s → `MaterializationRunner`:
plan (Iceberg incremental append scan → added data files) → enqueue → lease (scoped) → read one data
file with column projection → chunk → content-hash → probe `ContentHashIndex` → embed only novel →
append `embedding_store` + `content_map` → complete → on a clean drain, rebuild Tier 2 → advance
watermark.

**Measured.** 100 tables × 10,000 rows per corpus: inference calls equalled independently-computed
distinct content **exactly** at every duplication level (10,000/10,000 · 4,335/4,335 · 1,979/1,979 ·
500/500). 300 appended rows of already-embedded content cost **0** calls. 106 tests.

**Invariants held.** Ordering always by sequence number, never snapshot id. Watermark advances only
on a complete pass. Dedup index records only after commit. Deletes/overwrites detected and refused.
Cosine throws on dimension mismatch.

**Warehouse layout (verified on disk).**

```
embedding_store/data/model_version=all-MiniLM-L6-v2%3Av1/hash_prefix=3f/*.parquet
embedding_store/metadata/v467.metadata.json · version-hint.text · *.avro
```

`embedding_store`: **44,587 data files / 399 MiB = 9.2 KiB per file**, 1,400 metadata files, 467
metadata versions. All 256 `hash_prefix` buckets present under one `model_version`.

---

## 2. Current implementation vs expected architecture

| Area | Current | Desired | Gap | Difficulty | Priority |
|---|---|---|---|---|---|
| Embedding identity | content-addressed, `config_id` excludes table/keys | same | **none** — correct | — | keep |
| Dedup | process-local exact `HashSet`, positives unfalsifiable | shared, durable, transactional | **large, correctness** | M | **P0** |
| Backfill | pull queue, unit = whole data file | bounded row batches; pluggable executor | large | M | **P0** |
| Tier-2 | full rebuild per publish, whole history in heap | incremental | **largest, blocks everything** | L | **P0** |
| ANN | none (legacy Lucene artifacts, unwired) | base+delta, sharded, versioned | all of it | L | P2 |
| Sharding | none | content-hash shards + routing | all of it | L | P2 |
| Index lifecycle | manifest+alias exist for legacy path only | state machine reusing materialization pattern | moderate | M | P2 |
| Snapshot consistency | sequence watermark on materialization | per-index coverage watermark | moderate | M | P2 |
| Deletes | detected, **refused** | supported or explicitly classified | large | L | **P1** |
| Model migration | side-by-side `config_id` works; no promotion flow for Tier 2/3 | validate → promote → retire | moderate | M | P1 |
| Serving | `search-service` reads legacy table only; dead by default | engine-native; no VectorSync in query path | moderate | M | P1 |
| Presto/Trino | **zero** — no `hadoop` catalog type in Trino | REST catalog + SQL views | small, high leverage | S | **P0** |
| Auth / metrics | none, 62 endpoints | authn + Micrometer | moderate | S–M | **P0** |
| Tests | control-plane **0**, worker 1 file | cover new safety-critical code | moderate | M | **P0** |

---

## 3. Critical correctness problems

**C1 — Dedup positives are unfalsifiable process-local state.** `ContentHashIndex` assumes every
commit the process believed it made survived. If one did not, it reports a cache hit forever, skips
inference, and `content_map` commits a pointer to a vector that does not exist. `ProjectionBuilder`
then caps coverage at `oldestUnresolvedSequenceNumber - 1`, so the Tier-2 watermark **stalls
permanently**. `invalidate()` has zero callers. With more than one worker the headline measurement no
longer holds. The practitioner literature names this exact failure ("split-brain where cache and
index disagree"; "failed or cancelled batches must never publish partial cache state").

**C2 — Deletes are refused, not handled.** An append scan cannot see delete files. One `MERGE INTO`
parks a materialization `DEGRADED` until manual reconcile. The refusal is honest and detectable,
which is the right failure mode — but it makes CDC-style tables unusable.

**C3 — HadoopCatalog commit is not atomic.** `version-hint.text` (currently `467`) is a plain text
pointer updated by rename. Two writers read `467`, both write `v468.metadata.json`, one snapshot
disappears with **no `CommitFailedException`** and therefore no retry. Now guarded, but the compose
stack opts in.

**C4 — Multi-worker commit contention untested.** All writers append to two shared Iceberg tables.
No batching committer; no measured retry behaviour.

**C5 — Zero tests on the newest safety-critical code.** `control-plane` holds admission, the state
machine, and the leased queue, and has **no test files**. The derive path has none either. Four
blockers in this area were introduced and caught only by review, not by tests.

**C6 — No authentication on ~62 endpoints**, including one that drops the entire embedding store.

---

## 4. Critical scalability problems

**S1 — Tier-2 rebuild is O(all history) per publish, on a 15s timer.**
`ContentMap.liveEntriesAsOf` materializes the full append-only history before collapsing it.
Estimated ~120 GB peak heap for 100M rows × 3 revisions. Assembly made this *reachable*: it was dead
code, now it runs every cycle. **Freshness cannot be improved by running more often.**

**S2 — Unit of work is a whole data file.** Iceberg's default 512 MB target ≈ 1.5–2M rows read into
one in-memory list. No `-Xmx`, no container memory limit, and a TCP-connect healthcheck that will not
notice an OOM'd JVM. Works on the demo table, dies on a real one while reporting healthy.

**S3 — `hash_prefix` partitioning is a net negative, measured on both sides.**
Reads: content hashes are uniform, so a 500-hash batch touches ~all 256 buckets — pruning does
nothing and the probe degraded to a full store scan (9.3 s → 28.2 s of Iceberg time per table as the
store grew 5k → 20k vectors; a *fully cached* 500-row file still cost 28.5 s). Writes: every pass
writes ~256 tiny files, giving the 44,587-file / 9.2 KiB-per-file warehouse above. **File count grows
with the number of passes, not the volume of data.** This corrects an earlier characterisation of the
scheme as a design choice; it is a mistake.

**S4 — ~2 s fixed cost per materialization**, dominated by Iceberg commits to object storage. At
100K tables this dominates the dedup win.

**S5 — Metadata bloat.** 467 metadata versions and 1,400 metadata files at demo scale. No compaction,
no `expireSnapshots`.

---

## 5. Proposed target architecture

Iceberg stays authoritative. VectorSync owns derived artifacts and their coverage.

```
Apache Iceberg (source of truth)
        │  manifest diff / incremental append scan
        ▼
Control Plane (Postgres) ── materializations · work_items · embedded_content · index_versions
        │  leased, self-contained work items
        ▼
Workers ──► Embedding Service
        │
        ├─► Tier 1  embedding_store   (content_hash, model_version, config_id) → vector
        │           content_map       row/chunk @ sequence → content_hash
        │           (canonical, rebuild source for everything below)
        │
        ├─► Tier 2  one projection table, partitioned, incrementally maintained
        │           (what engines scan; optional per workload)
        │
        └─► Tier 3  ANN index versions: base + deltas + tombstones, sharded
                    (candidate generation only)
                            │
                            ▼
                    Serving / ANN coordinator → content_hash candidates
                            │
                            ▼
                    content_map → source keys → Iceberg (authoritative rows)
```

The shape in the brief is broadly right. Three corrections fall out of the investigation:
Tier 2 becomes **one** table rather than per-materialization; Tier 3 indexes **canonical vectors**
keyed by `content_hash` (as the brief says) which is the right call and has a consequence the brief
does not draw out (§8); and the durable dedup set belongs in **Postgres**, not in Iceberg.

---

## 6. Tier-1 recommendation — keep the two-table split

**Problem.** Is `embedding_store` + `content_map` the correct normalized model, or is it complexity
that could collapse into one table?

**Options.** (a) Keep two tables. (b) Collapse into one row-keyed table with the vector inlined.
(c) Keep two but move `content_map` into Postgres.

**Trade-offs.** (b) destroys the entire thesis: vectors become row-scoped, duplicate text is embedded
per row, and cross-table reuse is impossible. (c) makes lineage non-time-travellable and puts
unbounded append-only history in the transactional store; `content_map` is large and append-only,
which is exactly what Iceberg is for and exactly what Postgres is not.

**Recommendation.** Keep the split, unchanged. Two specific fixes: **drop `hash_prefix`
partitioning** (partition `embedding_store` by `model_version` alone, §4/S3) and add a **sort order on
`content_hash`** so manifest min/max prunes at file granularity without multiplying files. Schedule
compaction and `expireSnapshots`.

**Why.** Embedding identity and source-row relationship are genuinely different cardinalities and
lifetimes — one is immutable and shared, the other is append-only history per row. The measurement
already validates the split; only the physical layout is wrong.

---

## 7. Tier-2 recommendation — one partitioned table, partition-scoped overwrite

**Problem.** Full rebuild per publish is O(all history) and OOMs at scale (S1). It cannot be fixed by
calling it less often, because each call rewrites everything.

**Options.**
1. Streaming/spilling full rebuild — removes the OOM, keeps O(live rows) per publish.
2. Append + **equality deletes** on `(source_table, source_row_id, chunk_ordinal)`.
3. Append + **deletion vectors** (Iceberg v3).
4. **Partition-scoped overwrite**: partition by `(config_id, source_table, bucket(source_row_id, N))`
   and rewrite only buckets the batch touched.
5. Delete Tier 2; serve a view over Tier 1.

**Trade-offs.** Option 2 was the prior recommendation and **research retires it**: equality-delete
removal was re-proposed for V4 in July 2026, Snowflake does not support equality delete files at all,
Trino marks v3 row-level deletes unsupported, and row lineage explicitly does not track rows updated
via equality deletes. Option 3 is the format-forward answer but **unavailable here**: this repo is on
Iceberg **1.5.0** and v3 only reached production stability in 1.11.0 (May 2026); it also needs
per-row positional bookkeeping against our own files. Option 5 pushes an O(history) window function
onto every customer query. Option 1 leaves freshness proportional to dataset size.

**Recommendation.** Option 4. One table, `(config_id, source_table, bucket(source_row_id, N))`,
`overwriteByRowFilter` scoped to touched buckets, written forward from the derive batch — which
already holds the completed join in memory. Demote the full rebuild to an explicit admin/reproducibility
operation, off the scheduler. Revisit deletion vectors **after** an Iceberg upgrade.

**Why.** It is the only option that is incremental, uses no delete files (so every engine including
Snowflake and Athena can read it), works on 1.5.0, and bounds write amplification to `1/N` of the
table per touched bucket. It trades row-granular for bucket-granular amplification — the right trade
when the alternative is a format feature slated for retirement.

---

## 8. ANN / index architecture

Index **canonical unique vectors** keyed by `content_hash`, as the brief proposes. This is correct and
preserves the dedup advantage: an ANN over Tier 1 is smaller than one over Tier 2 by exactly the
dedup factor (measured up to 20× on the high-duplication corpus).

**Consequence the brief does not draw out:** top-k over *content* is not top-k over *rows*. One
content hash can map to thousands of rows, so a single ANN hit can fan out to thousands of source
rows, and `k` content hashes may yield far more or far fewer than `k` rows after `content_map`
resolution and predicate filtering. The serving layer must therefore **over-fetch and iterate**
(retrieve `k'` > `k` candidates, resolve, filter, top-k, repeat if short) rather than assume one hit
is one result. Row-count-based `LIMIT` semantics cannot be satisfied by a single ANN probe.

**Index metadata.** Reuse the materialization state-machine pattern rather than writing a second one:
`index_id, materialization_id, config_id, model_version, index_version, shard_id, backend,
covered_sequence_number, vector_count, status, created_at, updated_at` with
`BUILDING → READY → {STALE, INCOMPLETE} → COMPACTING → RETIRED` and `FAILED`. Store it in **Postgres**
alongside `materializations`, not in Iceberg: it is small, mutable, frequently transitioned, and needs
to be transactional with work completion. (The existing Iceberg-based `vector_index_manifest` /
`vector_index_alias` are append-only audit logs for the legacy path; keep them as audit, not as state.)

**Incremental maintenance (base + delta).** Appropriate, with a caveat. Base + delta + tombstone
matches Iceberg's manifest-diff primitive well and is what the Puffin paper does (Vamana greedy
insert for additions, lazy tombstoning for removals). The caveat is **recall drift**: query recall
degrades as delta count grows because each delta is an independently-built graph with no edges to the
base. So delta count must be bounded by policy (compact at N deltas or X% of base), and
`index_recall@k` — which the repo already measures label-free against an exact scan — should gate
compaction. Do not adopt base+delta without that gate; it is the difference between a controlled
trade and unmeasured degradation.

---

## 9. ANN sharding recommendation

**Problem.** Shard strategy determines query fanout, rebuild cost, and rebalancing pain.

**Options and trade-offs.**

| Strategy | Fanout | Rebuild/rebalance | Skew | Locality |
|---|---|---|---|---|
| Hash of `content_hash` | **all shards, every query** | trivial, uniform | none | none |
| Iceberg partition / data file | low if predicates align | aligned with source | **high** | source-aligned |
| Semantic clustering (IVF-style) | **low** — probe few clusters | expensive; drifts as data grows | moderate | vector-aligned |
| Hybrid: partition prune → cluster route | low | moderate | moderate | both |

**Recommendation.** Start with **hash sharding**, and treat the fanout as a deliberate, temporary
cost. Add a **coordinator-side centroid index** (semantic routing) as a second phase once there is a
real corpus to cluster, keeping hash shards underneath. Do not start with semantic sharding.

**Why.** Hash sharding is the only strategy whose correctness is independent of data distribution, and
correctness must be established before fanout is optimised. Its weakness is real but bounded: with S
shards every query probes S shards, which is acceptable to roughly tens of shards and is a latency
problem, not a wrong-answer problem. Semantic sharding inverts that: it reduces fanout but makes
rebalancing a clustering job whose drift silently degrades recall — an unmeasurable failure in a
system that does not yet have index-recall gating wired into the serving path. Partition-based
sharding is wrong here specifically because Tier 1 is content-addressed: a content hash has no
partition, so aligning shards to source partitions reintroduces the row-scoping the architecture
exists to avoid.

---

## 10. Durable dedup recommendation — Postgres

**Problem.** A cache hit must prove a durable vector exists (C1).

**Options.** Postgres table · RocksDB sidecar · Bloom + KV · bucketed Iceberg lookup table · Redis.

**Trade-offs.** RocksDB is per-node — the same flaw at a larger constant. Bloom + KV adds a
false-positive fallback path that is the per-batch Iceberg probe this design exists to eliminate
(measured 28.2 s/table at 20k vectors). A bucketed Iceberg lookup table has no point-lookup index:
every probe is a scan plan and the quadratic returns. Redis is a second stateful system and, decisively,
offers no transaction shared with work-item completion.

**Recommendation.** `embedded_content(model_version, config_id, content_hash)` with
`PRIMARY KEY (model_version, config_id, content_hash)` in the control plane. Probe with
`content_hash = ANY(?)`. **Write the row in the same transaction that marks the work item complete**,
so "vector exists" and "work done" cannot disagree. Keep the heap set as a read-through cache only.
At 10M vectors this is ~1 GB with index.

**Why.** Postgres is already the coordination substrate and is the only option that is simultaneously
exact, shared, durable, and *transactional with completion* — the last property is what makes it safe
rather than merely faster. Retires C1 and the per-scope 5M ceiling in one move. Needs a real migration
tool first: `ddl-auto: update` is not acceptable for a table on the correctness path.

---

## 11. Backfill execution recommendation

**Recommendation.** Keep **native pull-based workers**; introduce the `BackfillExecutor` seam but
implement only `NativeWorkerExecutor` now. Do **not** add Spark yet.

Three changes make native execution actually work:
1. **Bounded work units.** Split a data file into row-group / row-range tasks so a unit is bounded by
   *our* policy, not by the source writer's `write.target-file-size-bytes` (S2).
2. **Self-contained work items.** Serialize the `FileScanTask` into the item
   (`FileScanTaskParser.toJson/fromJson` is public in iceberg-core 1.5.0) and rehydrate it. This kills
   the per-file snapshot re-plan and makes an item independent of planner state.
3. **`planned_through` column** so planning is not re-run every cycle.

**Why.** The workload is inference-bound, not shuffle-bound: there is no join, no aggregation, no
sort. Spark's value is distributed shuffle and resource management, and the dominant cost here is a
GPU round trip that Spark does not make cheaper. Adding it imports a cluster dependency and a second
execution semantics for no measured gain. Revisit when a single materialization exceeds what a worker
pool can chew through, and keep the seam so that decision stays cheap.

---

## 12. Delete / update strategy

Be explicit rather than silent. Classify and publish the matrix:

| Source operation | Support | Mechanism |
|---|---|---|
| `INSERT` / append | **supported** | incremental append scan |
| `DELETE` (positional/DV) | **supported (phase 1)** | read delete files for touched partitions; tombstone `content_map` |
| `UPDATE` / `MERGE` (copy-on-write) | **supported (phase 1)** | overwrite rewrites data files; reconcile touched partitions |
| `UPDATE` / `MERGE` (merge-on-read, equality deletes) | **requires reconciliation** | equality deletes have no positional anchor; re-derive affected partitions |
| Schema evolution on an embedded column | **requires re-admission** | `config_id` changes; new materialization side-by-side |
| Partition-spec evolution | **requires rebuild** | |

**Phase 1 mechanism.** Replace blanket refusal with **partition-scoped reconcile**: when
`assess()` reports deletes or overwrites between snapshots, compute the affected partitions (already
implemented — `collectAffectedPartitions`, corrected during review to include copy-on-write *added*
files) and re-derive only those. A tombstoned source row appends a `content_map` tombstone; the
canonical vector is **never deleted**, because it is shared.

**Why partition-scoped rather than row-level.** Iceberg 1.5.0 has no deletion vectors, and equality
deletes are being retired. Partition reconcile is coarse but correct, needs no delete files, and the
repair primitives already exist and are unit-testable.

---

## 13. Snapshot consistency model

Three coverage watermarks, all **sequence numbers**, never snapshot ids:

```
source table          sequence S_src   (Iceberg current snapshot)
Tier 1 derivation     sequence S_1     (materializations.incremental_watermark)
Tier 2 projection     sequence S_2     (projection snapshot summary property)
Tier 3 index version  sequence S_3     (index_versions.covered_sequence_number)
```

Invariant: `S_3 ≤ S_2 ≤ S_1 ≤ S_src`, each advancing only when its input is complete. A query gets
`S_3` back with its results, so a caller can reason about staleness — and a caller needing
read-your-writes can compare against `S_src` and fall back to an exact scan over Tier 2.

`S_1` advances only on a pass with zero failures (already enforced). `S_2` must be capped at
`oldestUnresolvedSequenceNumber - 1` (already implemented) so an unresolved mapping cannot be
published as covered. `S_3` advances only after the index commit is durable.

**Reproducibility.** Because `config_id` pins every input to the derivation function and
`content_map` records the source sequence, `(source_snapshot, config_id)` is sufficient to reproduce a
vector set exactly. That is worth a formal test (§21).

---

## 14. Model migration strategy

Already half-solved: a different model or revision yields a different `config_id`, so versions coexist
in Tier 1 with no interference. What is missing is the promotion flow for Tiers 2–3.

```
v1 materialization LIVE, index ACTIVE
        │
        ├── admit v2 (new config_id) → backfill → Tier 1 (embeds only content novel *to v2*)
        ├── build v2 Tier 2 + index version → BUILDING → READY
        ├── validate: index_recall@k gate (label-free, exists) + precision vs fixture if supplied
        ├── promote: move the serving alias
        └── retire v1 after a grace period; GC by reference count
```

**Cost property worth stating:** migrating to a new model does *not* benefit from cross-model dedup —
a vector under `model_v2` is a different key, so every distinct content must be re-embedded once.
Dedup reduces a migration from "every row" to "every distinct content", which on the measured corpora
is a 2–20× reduction, not a free migration. Earlier phrasing risked implying the latter.

---

## 15. Serving / API architecture

Three modes by latency class; VectorSync is in the query path for none of the first two:

| Latency | Mode | State held by |
|---|---|---|
| Analytical / batch | brute-force scan over pruned Tier 2; exact; zero install | nobody |
| Interactive <100 ms | ANN coordinator over Tier 3 shards, or export to a vector DB | that layer |
| Embedded | mmap a Tier-3 artifact as a library | the app |

**Delete the current serving half.** `search-service` reads *only* the legacy `VectorTableSchema`,
whose sole writer is reachable from the now-disabled legacy scheduler — so in a default deployment
index build, evaluation, promotion, provenance and search are **dead on arrival**. Reduce the module
to an offline Tier-3 builder; delete `SearchController`, `SearchService`, `HnswIndexCache` and the
query-time embedders. Keep `EvaluationService` and `ProvenanceService`, repointed at Tier 2.

**Query-side embedding.** Measured 20 ms (MiniLM) / 68 ms (mpnet) warm, HTTP included — fine at query
time, fatal if the model is loaded per call. The API must take a **materialization**, never a model
name, so the query cannot be embedded in one space and scored against another.

---

## 16. Presto / Trino integration strategy

**Do not build a connector first.** Three layers, cheapest first:

1. **Zero-install SQL.** Vectors are an ordinary Iceberg table with `array<float>`; cosine is
   expressible in stock SQL. `SqlViewGenerator` already emits version-annotated Trino and Spark views
   (query vector hoisted into a CTE, `NULLIF` guard because Trino aborts on a zero denominator,
   `cardinality` check because `zip_with` pads with NULL). It is currently **dead code with no caller
   or endpoint** — exposing it is nearly free and is the single highest-leverage integration step.
2. **Engine adapters.** A Spark UDF package is straightforward. A Trino connector is an SPI plugin
   requiring cluster-wide install against an unstable API — defer.
3. **ANN pushdown.** Only after Tier 3 exists and the over-fetch semantics of §8 are settled.

**Blocking prerequisite:** Trino's Iceberg connector has **no `hadoop` catalog type**. REST is the one
catalog that unlocks Trino, Databricks Unity and Snowflake at once. ~3 days, and the cheapest
high-leverage item in this document.

**Hybrid filtering.** Predicate placement, in order of preference: Iceberg partition/file pruning
(cheapest, engine-native) → attribute filtering in the engine after candidate resolution → ANN-side
filtering last. Do **not** push structured predicates into the ANN backend in phase 1: filtered ANN
degrades recall in ways that require per-backend tuning, and the over-fetch loop of §8 already handles
post-filter shortfall correctly. Pruning belongs to Iceberg; ANN provides candidates only.

---

## 17. External vector backend strategy

```java
interface VectorIndexBackend {
    IndexVersion build(IndexSpec spec, Iterator<CanonicalVector> vectors);
    void        addDelta(IndexVersion base, Iterator<CanonicalVector> added);
    void        tombstone(IndexVersion version, Collection<String> contentHashes);
    SearchResult search(IndexVersion version, float[] query, int k, SearchOptions options);
    IndexVersion compact(IndexVersion base, List<IndexVersion> deltas);
    Capabilities capabilities();
}
```

`Capabilities()` is the load-bearing method and the answer to the brief's warning about erasing
capabilities: backends differ on filtered search, incremental insert, tombstone support, and
quantization, and the coordinator must **plan against declared capabilities** rather than assume a
lowest common denominator. A backend that cannot tombstone forces compaction-on-delete; one that
cannot do filtered search forces post-filtering with over-fetch.

**Order:** Lucene/HNSW first (already a dependency, embeddable, no new service, and the artifact is a
file — which fits "derived, rebuildable"). Then **one** external backend, Qdrant for its explicit
snapshot API. Milvus later. The exporter to a real vector DB is worth more adoption than any amount of
in-house ANN work, because it makes VectorSync complementary to the incumbents rather than competitive.

---

## 18. Puffin / Iceberg-native long-term strategy

**Research findings, not assumptions.** Iceberg's own project classifies vector indexing as
**early-stage discussion** (issue #12636); Puffin's proven index use is limited to Bloom filters and,
in v3, deletion vectors. A June 2026 paper proposes exactly the Puffin-backed ANN design — three
candidate blob types (`ann-routing-v1`, `ann-vamana-graph-v1`, `ann-centroid-index-v1`), sharded
Vamana graphs in Puffin bound through the snapshot summary, manifest-diff incremental refresh,
implemented in FlockDB — but it is an author-driven proposal, not an accepted spec. A separate 2026
paper instead embeds per-file IVF indexes in Parquet footers.

**Recommendation.** Treat Puffin-native as **V5 and as standards participation, not as a moat.** Design
`VectorIndexBackend` so a Puffin backend is addable without disturbing callers, and prefer the proposed
blob-type names if they gain traction. Do not build it now, and do not position it as the differentiator
— it is published prior art with an active committee discussion, and this repo is on Iceberg 1.5.0,
which predates even v3 stability (1.11.0, May 2026).

---

## 19. What is actually novel — corrected

I previously told you the content-addressed two-table design was something "nobody else offers."
**Research does not support that and I was wrong.**

Not novel:
- **Content hashing to avoid re-embedding** — recommended practice in the embedding-pipeline
  literature ("hash content and re-embed only modified chunks").
- **Content-addressed rows + a binding table** — described as the canonical design, including the
  composite key covering model and config, dedupe-before-embed, and the cross-worker coherence hazard
  this repo exhibits.
- **Lakehouse as source of truth, vector index as derived artifact** — the dominant recommendation.
- **Manifest-diff incremental refresh** — what the Puffin paper implements.
- **Puffin-backed ANN** — published June 2026.

Plausibly differentiated, in descending confidence:
1. **Reproducibility as a testable property** — `(source_snapshot, config_id)` reproducing a
   byte-identical vector set, with an incremental rebuild provably equal to a full one. Not found
   claimed anywhere; the repo is already most of the way there.
2. **Cross-materialization canonical store** — the literature describes content hashing *within* a
   pipeline; a shared store deduplicating across many tables and models, with measured exactness
   (inference calls == distinct content at four duplication levels), is a narrower but real
   distinction.
3. **Snapshot/sequence-anchored lifecycle with gated promotion** — label-free `index_recall@k` as a
   promotion gate, fixture-attributed precision, refusal to record unattributable scores.
4. **Engine-neutral open-table output** rather than a service API.

**Honest positioning.** The contribution is *rigor and integration*, not a new primitive. "Snapshot-aware
lifecycle management and incremental synchronization of embeddings and ANN indexes derived from Iceberg
tables" is defensible; "we invented content-addressed embeddings" is not. Lead with the reproducibility
proof and the measured cost curve, because those are the claims competitors do not make.

---

## 20. Phased implementation plan

**V1 — correct, scalable derivation (~8–10 weeks).** No ANN work until this is done.
- P0 Durable dedup in Postgres, transactional with completion (§10) + a real migration tool — 6 d
- P0 Tier 2: one table, partition-scoped incremental write (§7) + format bump — 12 d
- P0 Bounded work units; self-contained work items; `planned_through` (§11) — 6 d
- P0 REST catalog default; namespace creation; drop `hive`/`nessie` from advertised set — 3 d
- P0 JVM heap flags, `ExitOnOutOfMemoryError`, memory limits, actuator healthchecks — 1 d
- P0 Auth on all endpoints; Micrometer metrics (dedup rate, backlog, watermark lag, spend) — 5 d
- P0 Tests for control-plane and the derive path (currently 0 and 1 file) — 8 d
- P1 Drop `hash_prefix`; add sort order; compaction + `expireSnapshots` (§4/S3) — 4 d
- P1 Partition-scoped delete/overwrite reconcile (§12) — 8 d
- P1 Delete the serving half of `search-service` (§15) — 8 d, mostly deletion

**V2 — ANN (~6 weeks).** Backend abstraction with `Capabilities`; Lucene/HNSW backend;
`index_versions` in Postgres; hash sharding; coordinator with over-fetch/iterate resolution through
`content_map`; search API taking a materialization.

**V3 — incremental index lifecycle (~6 weeks).** Base + delta + tombstones; compaction gated on
`index_recall@k`; `STALE`/`INCOMPLETE` states; model migration promotion flow; one external backend
(Qdrant).

**V4 — engine integration (~4 weeks).** Expose `SqlViewGenerator`; Spark UDFs; measured hybrid
filtering; evaluate ANN pushdown.

**V5 — Puffin.** Standards participation; a Puffin backend behind the existing interface.

---

## 21. Tests and benchmarks required per phase

**V1**
- **Reproducibility:** full rebuild vs incremental rebuild at a pinned snapshot produce
  byte-identical vector sets. *The flagship test — it is the differentiator.*
- **Multi-worker dedup exactness:** N workers over a corpus with known distinct content; assert
  inference calls == distinct content (currently only true for N=1).
- **Crash injection:** kill a worker mid-batch; assert no `content_map` row points at a missing vector
  and the watermark does not advance.
- **Commit contention:** N concurrent writers to Tier 1; measure conflict and retry rates.
- **Memory bound:** a single 512 MB source data file must complete within a declared heap.
- **Tier-2 incrementality:** publish cost proportional to changed buckets, not dataset size.
- Benchmarks: dedup sweep (existing), per-materialization fixed cost, file-count growth per pass
  before/after dropping `hash_prefix`.

**V2** — index recall vs exact scan per shard count; fanout latency vs shard count; resolution
amplification (rows per content hash) on a skewed corpus; over-fetch iteration count to satisfy
`LIMIT k`.

**V3** — recall drift as delta count grows (this sets the compaction policy); compaction cost;
tombstone correctness after source deletes; promotion gate blocking a deliberately degraded index.

**V4** — SQL view correctness across Trino and Spark versions; pruning effectiveness with selective
predicates; end-to-end latency by mode.

---

## 22. Risks and unresolved questions

**Risks.**
1. **Tier-2 partition-scoped overwrite may still amplify too much** if updates are uniformly scattered
   across buckets — worst case approaches a full rewrite. Needs measurement on a realistic update
   distribution before committing the format bump.
2. **Postgres as the dedup authority becomes the scaling ceiling** somewhere past ~10⁸ content hashes.
   Acceptable for V1–V3; needs a sharded or tiered plan before billions.
3. **Base+delta recall drift** is a real quality regression if the compaction gate is not wired before
   deltas ship.
4. **Equality-delete retirement in V4** may change the delete story again; partition reconcile is
   chosen partly because it is insulated from that.
5. **Iceberg 1.5.0 is old.** Upgrading unlocks v3 deletion vectors and row lineage but is its own
   compatibility project, and row lineage notably does not track rows updated via equality deletes.

**Unresolved questions — these need a decision or an experiment, not an opinion.**
1. Is Tier 2 needed at all once Tier 3 exists, or does ANN + `content_map` + Iceberg cover the
   analytical case too? Keeping both is the expensive default.
2. What is the real distribution of rows-per-content-hash in production data? The entire ANN fanout
   analysis (§8) depends on it and I have only synthetic numbers.
3. Do customers actually want `LIMIT k` rows, or `k` distinct contents? These are different products
   and the API cannot straddle both.
4. Is anyone's corpus duplicated enough to care? The measured win ranges from 0% to 95% purely as a
   function of the corpus; the 0% case is the honest one to plan against.
5. Multi-tenant dedup: sharing a vector across tenants is a correctness win and possibly a privacy
   problem. Needs a policy before it is a feature.

---

## Bottom line

The content-addressed core is correct, measured, and worth keeping — but it is an implementation of a
known pattern, executed unusually rigorously, not a new primitive. The system is now assembled and
runs unattended, and assembly made three latent O(n²)-class defects live. **Do not start ANN work
until V1 lands:** Tier-2 incrementality, durable dedup, bounded work units, and a real catalog are
prerequisites, and building a derived acceleration layer on top of a derivation layer that cannot
scale would compound the problem rather than showcase it.

---

## Sources

- [Puffin-Backed Vector Indexes (arXiv:2606.04196)](https://arxiv.org/abs/2606.04196)
- [Support build full-text and vector index for iceberg — apache/iceberg#12636](https://github.com/apache/iceberg/issues/12636)
- [Filtered Vector Search in a Disaggregated Lakehouse](https://pith.science/paper/2608.05441)
- [How Hard Is It to Add an Index to an Open Format?](https://dev.to/morningman/how-hard-is-it-to-add-an-index-to-an-open-format-lessons-from-the-apache-iceberg-community-5gcf)
- [Why Embedding Pipelines Break at Scale](https://dzone.com/articles/why-embedding-pipelines-break-at-scale)
- [The Future of Open Source Table Formats: Iceberg and Lance](https://www.lancedb.com/blog/the-future-of-open-source-table-formats-iceberg-and-lance)
- [Apache Iceberg Spec — Deletion Vectors](https://iceberg.apache.org/spec/?h=deletion+vec)
- [Apache Iceberg v3: Moving the Ecosystem Towards Unification (Databricks)](https://www.databricks.com/blog/apache-icebergtm-v3-moving-ecosystem-towards-unification)
- [Why Iceberg V4 Wants to Retire Equality Deletes](https://datalakehousehub.com/blog/equality-deletes-iceberg-v4/)
- [Iceberg Delete Formats for CDC](https://olake.io/blog/iceberg-delete-formats-cdc-equality-vs-positional-vs-deletion-vectors/)
- [The vector embedding cache bug that costs nothing and corrupts everything](https://bh3r1th.medium.com/the-vector-embedding-cache-bug-that-costs-nothing-and-corrupts-everything-157be6c575e8)
- [Caching Pre-Computed Embeddings: TTL, Versioning, Cold Start](https://dev.to/gabrielanhaia/caching-pre-computed-embeddings-ttl-versioning-and-the-cold-start-problem-628)
- [How to run similarity search on Apache Iceberg (Oracle)](https://blogs.oracle.com/database/how-to-run-similarity-search-on-apache-iceberg)
