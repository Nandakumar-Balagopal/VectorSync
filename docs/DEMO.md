# VectorSync — prototype walkthrough

A prototype for architectural review. The goal is to show that one idea works, that it is built on
Iceberg properties rather than around them, and that its limits are known rather than discovered
during the meeting.

```bash
docker compose up -d --build        # first time, ~3 min
python3 bench/demo.py --step        # --step pauses between beats so you can talk
```

Every run uses a fresh embedding version, so it starts from an empty vector store and tells the same
story twice. Nothing is dropped and no previous run is disturbed.

---

## The one-sentence claim

> An embedding is a pure function of `(content, model, configuration)`, so a vector should be keyed
> by content — not by the row it arrived on.

Everything else follows from that, including the parts that look like unusual design choices.

## Why an architect should care

Three consequences, all demonstrated live:

| Consequence | Beat |
|---|---|
| A row whose unembedded columns change costs nothing | 6 |
| The same text in 500 rows, or in a different table, is one inference call | 4, 5 |
| A model migration re-embeds novel content, not the corpus | implied by 5 |

And one property that is harder to get and easier to sell: **every vector records the source snapshot
and the exact configuration that produced it**, so `(snapshot, config_id)` reproduces a vector set
exactly. That makes an incremental rebuild checkable against a full one.

---

## The seven beats

**1 — A plain Iceberg table.** 400 rows over 10 distinct texts. Iceberg is the source of truth;
nothing has been derived yet.

**2 — Admission prices the work before committing to it.** A dry run validates against the real
catalog and returns an estimate — rows, files, chunks — computed from Iceberg manifest metadata
**without reading a data file**. Nothing is persisted. Talk to this beat: it is the cheapest thing in
the system and the one most systems skip.

**3 — Commit it, then stop touching it.** The materialization is admitted as `VALIDATED` and a
scheduler takes over: plan → enqueue → lease → derive → publish → `LIVE`. The queue counters are
printed as it moves, so the state machine is visible rather than asserted. **No further command is
issued by hand.**

**4 — The claim, measured.** 400 rows, 10 distinct texts, **10 inference calls**, 97.5% avoided. Say
the number out loud; it is the whole pitch.

**5 — A different table with the same text costs nothing.** `catalog_b`, 400 rows, **0 inference
calls, 400 cache hits**, and the durable vector count does not move. `config_id` deliberately
excludes the source table, which is what permits this. The reuse is served from shared state in
Postgres, so it holds across workers — not from a process-local cache.

**6 — Incremental.** 150 rows appended, mostly text already embedded plus 2 genuinely new texts →
**2 new vectors**. Change detection uses Iceberg's incremental append scan, so the pass opens only
the data files the commit added.

**7 — What an engine sees.** The serving projection is an ordinary Iceberg table,
`vectors_<table>_<configId>`, with no VectorSync process in the query path. The sample shows several
different source rows sharing **one** content hash — the deduplication, visible in the serving table
— each row carrying content hash, model revision, source snapshot and sequence number.

---

## Architecture in one diagram

```
Apache Iceberg  (source of truth)
      │  incremental append scan → added data files
      ▼
Control plane (Postgres)
      │  materializations · work_items · embedded_content
      │  leased work, FOR UPDATE SKIP LOCKED
      ▼
Workers ──► Embedding service (stateless, resident models)
      │
      ├─ Tier 1  embedding_store  (content_hash, model_version, config_id) → vector   [canonical]
      │          content_map      row/chunk @ sequence → content_hash                 [lineage]
      │
      └─ Tier 2  vectors_<table>_<config>   flat, one row per live chunk              [derived]
                        │
                        ▼
                 Spark / Trino read it directly
```

Tier 1 is normalized so a vector is stored once; Tier 2 materializes the join so a similarity scan
does not pay for it. Tier 2 is always rebuildable from Tier 1, which is exactly why it is safe to
drop and rebuild.

## Three design decisions worth defending

**`config_id` excludes the source table and key columns.** It identifies the *derivation function* —
embedded columns, separator, chunker, model, revision, version, normalization. Folding the table name
in would give each table a private key space and destroy cross-table reuse. An early version did
exactly that, and a 2-table corpus with 5 distinct texts cost 10 inference calls instead of 5.

**Ordering is always by Iceberg sequence number, never snapshot id.** Snapshot ids are random longs;
sorting history by them shuffles it. This bug recurred four times in this codebase, which is why it
is now stated in the schema comments.

**The dedup record is written in the same transaction that completes the work item.** "This content
has a durable vector" and "the work that wrote it finished" must not be separately observable.
Because the Iceberg appends precede completion, the error is one-directional: a lost completion costs
a redundant, byte-identical embedding; a record without a vector cannot happen.

---

## What this is not, and what breaks

Say this before you are asked.

- **Not a vector database.** It does not answer top-k and does not compete on ANN latency. It feeds
  those systems rather than replacing them.
- **The serving projection is rebuilt whole on each publish.** Fine at demo scale; it is the first
  thing that must change for a large table, because freshness cannot be improved by running more
  often when every run rewrites everything.
- **One unit of work is a whole Iceberg data file.** A 512 MB source file is ~1.5–2M rows in memory.
  Row-level batching is required before production.
- **Deletes and `MERGE` are detected and refused**, not silently skipped. Append scans cannot see
  delete files, so the materialization parks `DEGRADED` with a reason. The refusal is deliberate; the
  handling is not built.
- **Only Spark can attach today.** Trino's Iceberg connector has no `hadoop` catalog type. REST
  catalog support is written but untested against a real REST catalog.
- **No authentication**, and metrics exist only for derivation counters.
- **Single-worker validated.** The shared dedup store makes multi-worker *correct by design* and it
  is unit-tested, but not load-tested.

## Honest positioning

The content-addressed two-table design is **not novel** — it is the documented pattern for embedding
caches, and content hashing to avoid re-embedding is recommended practice. Puffin-backed ANN indexes
were published as a paper in June 2026. What is plausibly differentiated here is the combination and
the rigor: snapshot-anchored lifecycle, a cross-materialization canonical store, and reproducibility
as a testable property. Lead with the cost curve and the reproducibility claim; do not claim the
primitive.

Full analysis, including the gap table and a phased plan, is in `docs/ARCHITECTURE-REVIEW.md`.

## If something goes wrong live

The demo fails loudly with the failing URL and response body rather than printing a half-story.

| Symptom | Cause |
|---|---|
| `demo stopped: ... Connection refused` | stack not up — `docker compose ps` |
| stuck at `VALIDATED` | scheduler interval is 15s; give it one cycle |
| `DEGRADED` | a delete/overwrite landed on the source table; use a fresh table name |
| beat 4 shows zeros | worker restarted after deriving; counters are in-memory per process |

---

## Measured on real data

The synthetic figures in `bench/cluster.py` said not to quote them. `bench/real.py` re-runs the same
measurement on BEIR — real documents, the benchmark's own test queries, and its relevance
judgements — so retrieval quality is measured rather than index agreement.

```bash
mkdir -p /tmp/beir && cd /tmp/beir
for d in nfcorpus fiqa; do curl -O https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/$d.zip && unzip -o $d.zip; done
python3 bench/real.py --dataset nfcorpus --clusters 64
python3 bench/real.py --dataset fiqa --clusters 256 --limit 20000
```

### FiQA — 20,000 financial forum posts, 256 clusters, 40 judged test queries

| probes | recall@10 | nDCG@10 | rows read | fraction | quality retained |
|---|---|---|---|---|---|
| 1 | 0.465 | 0.063 | 94 | 0.5% | 47% |
| 2 | 0.630 | 0.123 | 183 | 0.9% | 91% |
| 8 | **0.847** | **0.136** | 668 | **3.3%** | **100.6%** |
| 64 | 0.985 | 0.135 | 5,175 | 25.9% | 100% |
| 256 | 1.000 | 0.135 | 20,000 | 100% | ceiling |

**8 of 256 clusters gives the same retrieval quality as a full scan while reading 3.3% of the table
— a 30x I/O reduction.**

### NFCorpus — 3,593 biomedical abstracts, 64 clusters

| probes | recall@10 | nDCG@10 | fraction | quality retained |
|---|---|---|---|---|
| 2 | 0.737 | 0.329 | 3.8% | 94.5% |
| 8 | 0.907 | 0.334 | 14.0% | 96.0% |
| 64 | 1.000 | 0.348 | 100% | ceiling |

### The finding that matters

**Recall against the exact neighbour set badly understates quality retention.** On FiQA, recall
0.847 delivered 100% of exact nDCG: the neighbours pruning missed were not the relevant ones. Anyone
tuning this on recall alone would over-provision probes by 8x.

### Caveats, all of which cut against the result

- **The absolute nDCG ceiling is low** (0.135 on FiQA) because `all-MiniLM-L6-v2` is weak on
  financial text and titles were excluded. Hitting "100% of a low ceiling" is easier than hitting
  100% of a strong one. A better model may prove more sensitive to pruning — re-measure before
  claiming this generalises.
- **Cluster count needs tuning, and badly.** NFCorpus at 16 clusters needed 53% of the table for
  comparable quality; at 64 clusters it needed 3.8%. This is the standard IVF `nlist`/`nprobe` trade
  and there is no auto-tuning here.
- **Clusters are skewed** (FiQA: 14 to 295 members), so fraction-read varies per query and the
  average hides the worst case.
- **Deduplication is worth almost nothing on these corpora.** NFCorpus deduplicated 1.1%
  (3,633 documents to 3,593 vectors); FiQA 0.00%. The measured 95% savings came from synthetic
  duplication. Real IR corpora are near-unique by construction, so the content-addressed cost
  argument applies to catalogs, logs, support macros and document revisions — not to search corpora.
  Say this before being asked.
- **Derivation is slow**: 569s for 20,000 vectors, roughly 28ms each, where embedding itself is about
  1ms. Over 95% is per-batch overhead, not inference.

---

## Engine verification (Trino)

Everything above measured pruning with VectorSync's own scan counters, which is not evidence that a
query engine prunes anything. This section is the same claim checked by Trino's own counters.

```bash
docker compose -f docker-compose.yml -f docker-compose.engine.yml --profile engine up -d
python3 bench/cluster.py --clusters 32 --top-k 10 --per-category 400
docker compose --profile engine exec trino trino
```

The override exists because **Trino's Iceberg connector has no `hadoop` catalog type**, so the
default stack's warehouse is unreachable from it. Both attach to a JDBC catalog over the Postgres
already in the stack. Trino does not create that catalog's tables; VectorSync must run first.

### Trino sees every derived table

```
trino> SHOW TABLES FROM iceberg.vectorsync;
 clustered_demo_cluster_corpus          -- Tier 3, partitioned by cluster_id
 content_map                            -- Tier 1 lineage
 embedding_store                         -- Tier 1 canonical vectors
 vector_centroids                        -- routing table
 vectors_demo_cluster_corpus_c3b74338b3cfb584   -- Tier 2 projection
```

### Exact search, zero install

`cosine_similarity(array(double), array(double))` is built into Trino from release 465:

```sql
WITH q AS (
  SELECT CAST(embedding AS array(double)) AS qv
  FROM iceberg.vectorsync.vectors_demo_cluster_corpus_c3b74338b3cfb584
  WHERE source_row_id = 'c-00007' LIMIT 1
)
SELECT v.source_row_id, v.text,
       cosine_similarity(CAST(v.embedding AS array(double)), q.qv) AS similarity
FROM iceberg.vectorsync.vectors_demo_cluster_corpus_c3b74338b3cfb584 v
CROSS JOIN q ORDER BY similarity DESC LIMIT 5;
```

Returned the query row at 1.0 and its semantic neighbours at 0.944, 0.944, 0.942, 0.935.

### Pruning, confirmed by Trino

`EXPLAIN ANALYZE` reports physical input:

| query | partitions read | rows read | fraction |
|---|---|---|---|
| `SELECT count(*)` (full) | 32 | 2,000 | 100% |
| `WHERE cluster_id IN (3, 7)` | 2 | 82 | 4.1% |
| two-step search, literal ids | 2 | 135 | 6.75% |

The two-step search returned **the identical top-5 to a full scan** — same rows, same scores — for
6.75% of the I/O.

### The finding that changes how you write the query

**Cluster ids must be literals.** Ranking centroids in a subquery inside the same statement does not
prune:

| predicate form | rows read |
|---|---|
| `cluster_id IN (3, 2)` | **135** |
| `cluster_id IN (SELECT ... FROM vector_centroids ORDER BY ... LIMIT 2)` | **1,699** |

Dynamic filtering trimmed 2,000 to 1,699 and no further — 85% of the table. Partition pruning happens
during planning, and the planner cannot know a subquery's result. So the flow is two statements, or
one cheap centroid query plus one search. A single self-contained SQL statement looks more elegant
and costs 12x the I/O.
