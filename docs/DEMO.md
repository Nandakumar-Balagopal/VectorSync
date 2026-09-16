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
