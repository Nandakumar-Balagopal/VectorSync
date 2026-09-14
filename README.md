# VectorSync

**Incremental, content-addressed embedding derivation over Apache Iceberg.**

Keeps derived embeddings correct and cheap as your Iceberg tables change. Vectors are an open
table, not a service — query them with Trino, Spark, or anything else that reads Iceberg.

---

## The thesis

An embedding is a pure function of `(content, model, configuration)`.

So a vector should be keyed by **content**, not by the row it arrived on. That one decision is what
separates this from a sync pipeline:

- A row whose `price` changed but whose description did not costs **nothing** to re-embed.
- The same text appearing in 500 rows, or in 50 different tables, is **one** inference call.
- A model migration re-embeds **novel content**, not your corpus.

Everything else in this repo exists to avoid calling the model.

## What it is not

Not a vector database. It does not answer `top-k` and does not compete on ANN latency — purpose-built
vector databases will win there, and this feeds them rather than replacing them.

Not an ML lifecycle tool. It does not track experiments or serve models.

It occupies the gap both leave: **an open, engine-neutral, reproducible derivation layer** that knows
which source snapshot and which model revision produced every vector it holds.

---

## Storage model

Three tiers, and the separation is the design.

```
TIER 1  canonical, deduplicated, never recomputed
  embedding_store   (content_hash, model_version, config_id) -> vector
                    immutable; partitioned by (model_version, hash_prefix)
  content_map       (source_table, source_row_id, chunk_ordinal, config_id) @ sequence -> content_hash
                    append-only history with tombstones; partitioned by (source_table, config_id)

TIER 2  serving projection — derived, disposable, rebuildable
  vectors_<table>_<config>   flat denormalized join, one row per live chunk, vector inlined
                             this is what a query engine brute-force scans

TIER 3  optional acceleration
  exported vector DB collection  |  mmap-able index artifact
```

`embedding_store` is normalized for dedup; `content_map` carries lineage. Tier 2 is the materialized
join, because a similarity scan should not pay for a join. Tier 2 can always be dropped and rebuilt
from Tier 1, which is precisely why it is safe to drop.

**`config_id` deliberately excludes the source table and key columns.** It identifies the derivation
*function* — embedded columns, separator, chunker, model, revision, version, normalization. Folding
the table name in would give every table a private key space and destroy cross-table dedup. (An early
version did exactly that; a 2-table corpus with 5 distinct texts cost 10 inference calls instead of 5.)

## Reproducibility

Every vector answers: *which exact source data and configuration produced this?*

| Recorded | Where |
|---|---|
| source table, row id, chunk ordinal | `content_map` |
| source snapshot id **and sequence number** | `content_map` |
| content hash of the exact embedded text | both tiers |
| model, revision, embedding version | `model_version` + `config_id` |
| embedded columns, separator, chunker, normalization | `config_id` |
| evaluation metrics and the fixture behind them | index manifest |

Ordering is always by **sequence number**, never snapshot id — Iceberg snapshot ids are random longs.
Sorting history by them shuffles it, and this bug has been fixed three times in this repo.

---

## Change detection

Uses Iceberg's incremental append scan to enumerate *added data files*, then makes each file a unit
of work. Cost tracks change, not table size.

The honest limitation: **an append scan sees added files only.** Deletes and row-level updates
expressed as Iceberg delete files are invisible to it. `IncrementalChangeDetector.assess()` detects
that case and refuses the pass rather than advancing the watermark past changes it never
materialized — a silent, permanent failure mode. A reconcile or re-anchor is required, and the
system says so instead of pretending.

A watermark advances only when **every** file in a pass succeeded.

---

## Benchmark

100 Iceberg tables x 100 rows = 10,000 rows per corpus, four corpora, on a single laptop with real
`all-MiniLM-L6-v2` embeddings. Duplication is an explicit parameter, so the result is a sweep rather
than one flattering number — and the 0% point is where the honesty lives, because there dedup can
win nothing and only its overhead shows.

Ground-truth content hashes are computed independently in `bench/corpus.py`, so agreement between
the expected and measured rates is evidence rather than self-report.

### Dedup sweep — 100 tables, 10,000 rows per corpus

`all-MiniLM-L6-v2`, MinIO-backed Iceberg warehouse, single laptop. Distinct content is computed
independently in `bench/corpus.py`; inference calls are the service's own counter.

| corpus | rows | distinct content | inference calls | avoided | derive time |
|---|---|---|---|---|---|
| dup00 | 10,000 | 10,000 | **10,000** | 0.0% | 502.6s |
| dup50 | 10,000 | 4,335 | **4,335** | 56.6% | 56.0s |
| dup80 | 10,000 | 1,979 | **1,979** | 80.2% | 39.4s |
| dup95 | 10,000 | 500 | **500** | 95.0% | 22.7s |

Inference calls equal distinct content **exactly** in all four runs. Cost tracks content, not rows.
At 95% duplication the same 10,000 rows derive 22x faster than at 0%.

In `dup95`, inference plateaus at exactly 500 by table 50 — tables 51 through 100 cost **zero**
inference, because their content was already embedded by earlier tables. Cross-table dedup is the
property a row-keyed design and a vector database both lack.

The `dup00` row is the control: every row distinct, nothing to reuse, so the design wins nothing and
pays full price. Reporting only `dup95` would be advocacy.

### Incremental passes

One table, appends only, measured by `bench/incremental.py`:

| case | rows in pass | inference | result |
|---|---|---|---|
| backfill, 500 rows over 50 distinct texts | 500 | 50 | baseline |
| append 300 rows, 50 novel texts | 300 | 50 | the 500 existing rows are not revisited |
| append 300 rows, content already embedded | 300 | **0** | 300 cache hits, 0.2s |
| append nothing | 0 | 0 | no work, watermark holds |

The third row is the decisive one. Three hundred brand-new rows cost zero inference because their
*content* was already known. A design keyed by row identity re-embeds all 300.

### What the benchmark does not show

- **A fixed per-materialization cost of roughly 2s** dominates small tables, spent on Iceberg
  commits to object storage rather than on inference. At 100K tables this matters more than the
  dedup win and argues for a batching committer.
- Every table here holds **one data file**, so multi-file parallelism and the O(N^2) metadata cost
  in `readFile` (which re-plans the snapshot per file) are untested.
- Deletes and updates are untested because the incremental path **refuses** them by design.
- Wall-clock timings are noisy on a loaded laptop; a sustained 40-minute run drove the embedding
  service from 62ms to 4,900ms per 64-text batch through thermal and memory pressure, which is why
  the claim rests on **inference call counts** (exact and reproducible) rather than on seconds.

### The bug this benchmark caught

The first implementation partitioned the embedding store by a 2-character `hash_prefix` to prune the
dedup probe. Content hashes are uniformly distributed, so a batch of 500 hashes touches essentially
all 256 buckets: pruning did nothing and every probe scanned the whole store. Backfill cost went
quadratic in store size — 9.3s of Iceberg time per table at 5,000 stored vectors, 28.2s at 20,000,
with inference flat at 2s. A 500-row file whose content was **entirely cached** still cost 28.5s.

`ContentHashIndex` replaces the per-batch scan with one seed scan per process and an exact in-memory
set. The same measurement afterwards: flat at ~7.6s per table across 70 tables, and a fully-cached
500-row file drops from 28.5s to **0.2s**. The ceiling is memory, is enforced, and is documented —
past it the index falls back to probing Iceberg, which is slow but correct.

Reproduce:

```bash
python3 bench/corpus.py --tables 100 --rows 100 --out /tmp/vsbench
python3 bench/run.py --corpus /tmp/vsbench --tables 100 --version run1
python3 bench/incremental.py run1
```

`--version` scopes a run to its own `(model_version, config_id)`, so a rerun starts from an empty
store instead of inheriting the previous run's cache and reporting a win it did not earn.

---

## Serving

There is no VectorSync process in the query path. Three modes, chosen by latency class:

| Latency class | Mode | Holds state |
|---|---|---|
| Analytical / batch | brute-force scan over a pruned Tier-2 partition — exact, zero install | nobody |
| Interactive, <100ms | export to a vector DB built for it | that system |
| Embedded | mmap a Tier-3 artifact as a library | the app |

A single query embedding measures **20ms** (MiniLM) / **68ms** (mpnet) warm, HTTP included — fine at
query time, fatal if the model is loaded per call. So the embedding service stays resident and
stateless, and `SqlViewGenerator` emits a view per dialect that wraps the cosine arithmetic, so an
analyst installs nothing and never writes `zip_with`/`reduce` by hand.

**An engine user never names a model.** The query API takes a *materialization*, and the active model
revision is resolved from it. Naming a model per query is how you embed a query in one space and
score it against another.

---

## Modules

| Module | Role |
|---|---|
| `common` | DTOs, constants, cosine similarity. No Spring. |
| `vectorsync-format` | Iceberg format layer: Tier-1 stores, content hashing, spec identity, projection, SQL views. No Spring. |
| `control-plane` | Postgres. Validated admission with cost estimates, materialization state machine, leased work queue. |
| `worker` | Change detection, chunking, dedup-aware derivation, Tier-1 writes. |
| `search-service` | Evaluation, provenance, index manifest. **Not** the serving tier. |
| `embedding-service` | FastAPI + sentence-transformers. Stateless, resident models. |
| `dashboard` | React + Carbon. Demo and ops only. |

## Admission

Registration validates instead of accepting. It checks that the table resolves, that every key and
embedded column exists and is the right type, that the chunker and model are known — accumulating
*all* problems rather than failing on the first — and returns a **cost estimate** computed from
Iceberg manifest metadata without scanning the table:

```bash
POST /api/materializations   {"dryRun": true, ...}
  -> estimatedRows, estimatedChunks, estimatedFiles, validation problems
```

The predecessor validated nothing: it generated a UUID and saved, so a nonexistent table with
nonexistent columns returned 200 and then failed forever in the scheduler.

## Retirement

Retiring a materialization **never** deletes from `embedding_store`. Those vectors are shared by
construction with every materialization that deduplicated against them, so deleting by source table
would silently corrupt other datasets. Retire removes the active tag, drops the Tier-2 projection,
and tombstones `content_map` rows. Reclaim is a separate reference-aware sweep with a grace period.

Because inference is already paid for and content-addressed, **retire and re-register is nearly free.**

---

## Promotion

Gated. An index cannot serve unless it was evaluated, clears a configurable index-recall floor
(default 0.95), and covers the newest source sequence number. Overridable with `force`, which
requires a note and writes the override plus the blockers it bypassed into the alias log — a gate
with no escape hatch gets bypassed by editing the alias table, which destroys the audit trail
instead of recording the decision.

Two metrics, kept apart because they have different authority:

- **`index_recall@k`** — HNSW versus an exhaustive scan. Ground truth is the exact scan, so it needs
  no labels and gates promotion.
- **`precision@k`** — against a judged fixture. Measures the *model*, not the index. Recorded only
  when a `fixtureRef` names its ground truth; a score whose ground truth cannot be identified is not
  evidence.

Consumers are query engines, so there is no click-through to fall back on: whatever is checked
before promotion is the only check that ever happens.

---

## Quick start

```bash
docker compose up -d --build
python3 bench/corpus.py --tables 5 --rows 100 --out /tmp/demo
python3 bench/run.py --corpus /tmp/demo --tables 5 --modes dup80
```

Dashboard at http://localhost:3000.

## Status and known gaps

Stated plainly rather than implied:

- **Delete/update detection is incomplete.** Append scans see added files. Delete-file handling is
  detected and refused, not implemented.
- **Content-hash lookup needs a real index.** `ContentHashIndex` holds an exact in-memory set,
  seeded once per process per scope. Memory is the ceiling: ~120 bytes per hash, so 10M vectors is
  around 1.2GB and the billions this architecture talks about need a shared KV store or a Bloom
  filter in front of one. Going straight to Iceberg does not work — measured, not assumed: prefix
  partitioning does not prune a multi-hundred-hash batch, so the probe degrades to a full store scan
  and backfill becomes quadratic.
- **Commit contention** on shared Tier-1 tables under many concurrent writers needs a batching
  committer. Not hit at benchmark scale.
- **Compaction and `expireSnapshots`** are obligations, not yet scheduled. Vector ids exclude data
  file paths, so compaction is safe when it arrives.
- **Branch/tag publish** is designed but not built; promotion still uses the alias table.
- **No exporter yet.** The highest-value next integration.
