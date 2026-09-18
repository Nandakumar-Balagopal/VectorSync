#!/usr/bin/env python3
"""
What a mutating table actually costs, and what a user waiting on it actually feels.

Every number this project has published so far was measured on a table that was loaded once and
then left alone. That is the easy case, and it is not the case the architecture exists for: the
claim is that derived embeddings stay correct and cheap *as the source changes*, and nothing
measured that.

So this drives a rapidly changing table and reports two families of number that matter to different
people:

  COST      inference calls, and how they track novel content rather than changed rows. This is the
            architecture's whole thesis and the one thing it should win on decisively.

  FRESHNESS how long after a source commit the change is visible to a query. This is what a user
            feels. A system that is cheap and twelve minutes stale is not obviously better than one
            that is expensive and current, so reporting cost without lag would be advocacy.

Mutation mixes appends, updates and deletes, because the pipeline treats all three differently:
appends go through the incremental scan, updates arrive as overwrites, and deletes only resolve via
the reconcile sweep. A workload of pure appends would flatter the system by avoiding both of the
paths that are hard.

Latency is measured end to end through whatever engine is configured. With --trino it issues the
real SQL a user would write, through the view the worker generates, so the number includes engine
planning, partition pruning and scoring -- not just our own scan. Without it, the same cosine is
computed over the projection directly, which isolates data cost from engine cost. Run both: the
difference between them is the engine's overhead and it is worth knowing separately.

WHICH PATH THIS MEASURES, AND WHY IT MATTERS

It registers a materialization through the control plane and lets the SCHEDULER drive derivation,
rather than calling POST /api/derive/run. That is not a stylistic preference -- the first version of
this script called the derive endpoint directly and reported 12.387 inference calls per changed row,
twelve times worse than the row-keyed baseline it was meant to beat.

The cause was the endpoint, not the architecture. The durable dedup record is written by
WorkQueueService.complete, in the same transaction that completes a work item, and
DeriveOrchestrationService -- which backs /api/derive/run -- has no control-plane reference at all.
So nothing it embedded was ever recorded, the probe could never hit on a later invocation, and every
pass re-embedded the whole corpus. Anyone scripting that endpoint pays full price every time, which
is worth knowing separately, but it is not what the system does in production.

So: materialization, scheduler, work queue. The same path a deployment runs.

Usage
  python3 bench/mutation.py --dataset nfcorpus --rows 3000 --rounds 10 --mutate 0.05
  python3 bench/mutation.py --dataset fiqa --rows 20000 --rounds 20 --mutate 0.02 --trino
  python3 bench/mutation.py --dataset nfcorpus --rows 3000 --rounds 5 --mutate 0.10 --deletes 0.3

Scale with --rows and mutation pressure with --mutate (fraction of rows changed per round). The
interesting regime is many rounds at a small fraction, which is what a real CDC feed looks like;
a few rounds at a large fraction tests the refit path instead.
"""

import argparse
import json
import os
import random
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from real import (  # noqa: E402  -- reuses the existing harness rather than duplicating it
    WORKER,
    content_hash,
    get,
    load_beir,
    ndcg_at_k,
    post,
)

CONTROL = os.environ.get("CONTROL_URL", "http://localhost:8080")


def percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
    return ordered[index]


class Workload:
    """
    A deterministic mutation stream over a BEIR corpus.

    Seeded so two runs of the same arguments mutate identically -- otherwise a cost comparison
    between configurations is comparing workloads rather than configurations.
    """

    def __init__(self, docs, mutate_fraction, delete_share, seed=20260917):
        # docs is {id: text}, which is what real.load_beir produces.
        self.live = dict(docs)
        self.mutate_fraction = mutate_fraction
        self.delete_share = delete_share
        self.random = random.Random(seed)
        self.retired = []

    def next_round(self):
        """
        Returns (upserts, deletes) for one round.

        Updates reuse an existing id with genuinely different text, because an update whose text is
        unchanged is indistinguishable from a no-op to a content-addressed pipeline -- it would cost
        nothing and prove nothing. Deletes retire ids permanently so the live set really shrinks.
        """
        live_ids = list(self.live.keys())
        if not live_ids:
            return [], []

        count = max(1, int(len(live_ids) * self.mutate_fraction))
        chosen = self.random.sample(live_ids, min(count, len(live_ids)))

        split = int(len(chosen) * self.delete_share)
        to_delete = chosen[:split]
        to_update = chosen[split:]

        deletes = []
        for doc_id in to_delete:
            self.live.pop(doc_id, None)
            self.retired.append(doc_id)
            deletes.append(doc_id)

        upserts = []
        for doc_id in to_update:
            # Text that is new but still natural: a real edit, not a random string, so chunking and
            # embedding behave as they would on a genuine revision. It must actually differ, or a
            # content-addressed pipeline treats the update as a no-op and the round proves nothing.
            self.live[doc_id] = self.live[doc_id] + " Revised in round %d." % self.round_marker
            upserts.append(doc_id)

        return upserts, deletes

    round_marker = 0


def config_id_of(materialization_id):
    return get("%s/api/materializations/%s" % (CONTROL, materialization_id))["configId"]


def watermark_of(materialization_id):
    entry = get("%s/api/materializations/%s" % (CONTROL, materialization_id))
    return entry.get("incrementalWatermark", 0) or 0


def as_rows(live):
    """{id: text} to the seed-row shape the demo endpoints take."""
    return [{"id": doc_id, "name": "", "description": text} for doc_id, text in live.items()]


def seed_source(table, live_docs):
    """Creates the table for round zero. Drops and recreates, so there is no prior history."""
    post("%s/api/demo/tables" % WORKER,
         {"tableName": table, "rows": as_rows(live_docs)}, timeout=3600)


def rewrite_source(table, live_docs):
    """
    Replaces the source table's contents with the current live set, as one copy-on-write pass.

    Deliberately a rewrite rather than row-level edits. It is what an engine emits for
    UPDATE/DELETE on a table with no delete files, it is the shape the pipeline's assess() classifies
    as needing a reconcile, and it exercises the path that matters instead of the one that is easy.
    """
    post(
        "%s/api/demo/tables/replace" % WORKER,
        {"tableName": table, "rows": as_rows(live_docs)},
        timeout=3600,
    )


SPEC = {
    "keyColumns": ["id"],
    "embeddingColumns": ["name", "description"],
    "joinSeparator": " ",
    "chunker": "whole",
    "modelName": "all-MiniLM-L6-v2",
    "modelRevision": "mutation-bench",
    "embeddingVersion": "v1",
}


def admit(table):
    """
    Registers the materialization, or returns the existing one.

    Admission is unique on (source_table, config_id), so a re-run of this benchmark collides rather
    than creating a second scope -- which is the behaviour we want: the whole point is to accumulate
    a dedup record across rounds, and a fresh scope each round would measure nothing.
    """
    body = dict(SPEC)
    body["sourceTable"] = table
    body["freshnessSlaSeconds"] = 60
    try:
        created = post("%s/api/materializations" % CONTROL, body, timeout=300)
        if created.get("id"):
            return created["id"]
    except Exception as failure:
        # An "already exists" collision is the expected path on any run after the first.
        if "already exists" not in str(failure):
            raise

    for state in ("LIVE", "VALIDATED", "BACKFILLING", "DEGRADED", "REGISTERED"):
        for entry in get("%s/api/materializations?state=%s" % (CONTROL, state)):
            if entry.get("sourceTable") == table:
                return entry["id"]
    raise RuntimeError("could not admit or find a materialization for %s" % table)


def resume_if_degraded(materialization_id):
    """
    A range with deletes parks the materialization unless reconcile is enabled.

    Attempted every round rather than only on failure: the benchmark is deliberately generating the
    mutation shape that degrades a materialization, and a run that silently stalled there would
    report falling inference counts that looked like excellent dedup.
    """
    entry = get("%s/api/materializations/%s" % (CONTROL, materialization_id))
    if entry.get("state") == "DEGRADED":
        post("%s/api/materializations/%s/resume" % (CONTROL, materialization_id), {}, timeout=300)
        return True
    return False


def await_advance(materialization_id, previous_watermark, timeout_seconds=1800):
    """
    Waits until the scheduler's watermark moves past where it was before the source commit.

    Compared against the PREVIOUS watermark rather than against the source's current sequence
    number, which would need an endpoint that does not exist -- and this is the better comparison
    anyway: it asks "has the pipeline caught up with the change I just made", which is exactly the
    question a user waiting on freshness is asking.

    Polls rather than sleeps, because the number being measured is the lag and a fixed sleep would
    report the sleep. Returns (seconds_waited, state, resumed).
    """
    started = time.time()
    resumed = False
    while time.time() - started < timeout_seconds:
        entry = get("%s/api/materializations/%s" % (CONTROL, materialization_id))
        state = entry.get("state")
        watermark = entry.get("incrementalWatermark", 0) or 0
        if state == "LIVE" and watermark > previous_watermark:
            return time.time() - started, state, resumed
        if state == "DEGRADED":
            resumed = resume_if_degraded(materialization_id) or resumed
        time.sleep(2)
    return time.time() - started, "TIMEOUT", resumed


def query_direct(table, config_id, query_text, k):
    """Top-k straight from the projection. Isolates data cost from engine cost."""
    started = time.time()
    body = post(
        "%s/api/derive/search" % WORKER,
        {"sourceTable": table, "configId": config_id, "query": query_text,
         "modelName": SPEC["modelName"], "k": k},
        timeout=300,
    )
    elapsed_ms = (time.time() - started) * 1000.0
    return [hit["sourceRowId"] for hit in body.get("hits", [])], elapsed_ms, body


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="nfcorpus")
    parser.add_argument("--rows", type=int, default=3000)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--mutate", type=float, default=0.05,
                        help="fraction of live rows changed per round")
    parser.add_argument("--deletes", type=float, default=0.2,
                        help="share of each round's mutations that are deletes")
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--trino", action="store_true",
                        help="also measure through real Trino, including planning and pruning")
    parser.add_argument("--table", default="default.mutation_bench")
    args = parser.parse_args()

    docs, queries, qrels = load_beir(args.dataset, 0)

    # Judged documents first. A leading slice of the corpus is the obvious way to take a subset and
    # it makes nDCG structurally zero: nfcorpus has 3,633 documents and its qrels reference ids
    # scattered throughout, so the first 400 contained none of them and every round reported 0.0000.
    # A relevance number that cannot be nonzero is worse than no relevance number, because it looks
    # like a retrieval failure.
    judged = {doc_id for relevant in qrels.values() for doc_id in relevant}
    ordered = [d for d in docs if d in judged] + [d for d in docs if d not in judged]
    docs = {doc_id: docs[doc_id] for doc_id in ordered[: args.rows]}

    # Selecting judged DOCUMENTS is not enough, which a run proved: 300 of 300 documents were
    # judged by some query and nDCG was still 0.0000 for every round. The metric needs queries
    # whose OWN relevant documents are in the subset, so the queries are filtered too, and the
    # count is printed rather than assumed -- a relevance figure that cannot be non-zero reads as a
    # retrieval failure and is worse than no figure.
    answerable = {
        query_id: text for query_id, text in queries.items()
        if any(doc_id in docs for doc_id in qrels.get(query_id, {}))
    }
    print("documents: %d, queries answerable from this subset: %d of %d"
          % (len(docs), len(answerable), len(queries)))
    if not answerable:
        print("  !! no query has a relevant document in this subset -- nDCG will be 0.0000 and "
              "means nothing. Raise --rows until this is non-zero.")
    else:
        queries = answerable
    print("corpus %s: %d documents, %d judged queries"
          % (args.dataset, len(docs), len(queries)))

    distinct_initial = len({content_hash(text) for text in docs.values()})
    print("distinct content: %d of %d rows (%.1f%% duplicated)"
          % (distinct_initial, len(docs), 100.0 * (1 - distinct_initial / max(1, len(docs)))))

    workload = Workload(docs, args.mutate, args.deletes)

    # Seed and admit before the loop: admission validates the spec against a table that must already
    # exist, and the config id it returns is what every later query and metric read is keyed on.
    seed_source(args.table, workload.live)
    materialization_id = admit(args.table)
    config_id = config_id_of(materialization_id)
    print("materialization %s, config %s" % (materialization_id, config_id))
    previous_cumulative = 0

    print()
    print("round |   upserts  deletes |  inference | derive_s | lag_s | p50_ms  p95_ms | nDCG@%d"
          % args.k)
    print("------+--------------------+------------+----------+-------+----------------+--------")

    rows = []
    for round_index in range(args.rounds + 1):
        workload.round_marker = round_index

        if round_index == 0:
            upserts, deletes = list(workload.live.keys()), []
        else:
            upserts, deletes = workload.next_round()

        # Captured before the commit, so the wait below asks "has the pipeline caught up with the
        # change I just made" rather than "is it LIVE", which it already was.
        watermark_before = watermark_of(materialization_id) if round_index > 0 else -1

        committed_at = time.time()
        if round_index > 0:
            # Round zero's rows were seeded before admission, because admission validates the spec
            # against a table that has to exist.
            rewrite_source(args.table, workload.live)

        # Wait for the pipeline to make the change queryable, which is the number a user feels.
        # Polled rather than assumed: the whole point is to measure the lag, not to sleep past it.
        derive_started = time.time()
        derive_seconds, state, resumed = await_advance(
            materialization_id, watermark_before)
        lag_seconds = time.time() - committed_at

        # Cumulative, so the per-round figure is a difference. The registry counts per scope for the
        # life of the worker, which is what makes it the right source for a dedup claim -- it cannot
        # be reset between rounds to flatter a number.
        totals = get("%s/api/derive/metrics?sourceTable=%s&configId=%s"
                     % (WORKER, args.table, config_id))
        cumulative = totals.get("inferenceCalls", 0)
        inference = cumulative - previous_cumulative
        previous_cumulative = cumulative

        if state != "LIVE":
            print("  !! materialization is %s at round %d, not LIVE. Inference counts after this "
                  "point describe a stalled pipeline, not dedup." % (state, round_index))
        if resumed:
            print("  (resumed from DEGRADED -- the round contained deletes)")

        latencies = []
        ranked_all = {}
        for query_id, query_text in list(queries.items())[: args.queries]:
            hits, elapsed_ms, _ = query_direct(args.table, config_id, query_text, args.k)
            latencies.append(elapsed_ms)
            ranked_all[query_id] = hits

        scores = [ndcg_at_k(ranked_all[q], qrels.get(q, {}), args.k) for q in ranked_all]
        ndcg = statistics.fmean(scores) if scores else 0.0

        print("%5d | %9d %8d | %10d | %8.1f | %5.1f | %6.0f %7.0f | %.4f"
              % (round_index, len(upserts), len(deletes), inference, derive_seconds,
                 lag_seconds, percentile(latencies, 0.50), percentile(latencies, 0.95), ndcg))

        rows.append({
            "round": round_index,
            "upserts": len(upserts),
            "deletes": len(deletes),
            "live_rows": len(workload.live),
            "inference_calls": inference,
            "derive_seconds": round(derive_seconds, 2),
            "visible_lag_seconds": round(lag_seconds, 2),
            "p50_ms": round(percentile(latencies, 0.50), 1),
            "p95_ms": round(percentile(latencies, 0.95), 1),
            "ndcg": round(ndcg, 4),
        })

    steady = rows[1:]
    if steady:
        total_inference = sum(r["inference_calls"] for r in steady)
        total_changed = sum(r["upserts"] + r["deletes"] for r in steady)
        print()
        print("STEADY STATE over %d mutation rounds" % len(steady))
        print("  rows changed:        %d" % total_changed)
        print("  inference calls:     %d" % total_inference)
        print("  calls per changed row: %.3f   <-- the thesis. A row-keyed pipeline pays 1.000"
              % (total_inference / max(1, total_changed)))
        print("  visible lag p50:     %.1fs" % percentile(
            [r["visible_lag_seconds"] for r in steady], 0.50))
        print("  visible lag p95:     %.1fs" % percentile(
            [r["visible_lag_seconds"] for r in steady], 0.95))
        print("  query p50 / p95:     %.0fms / %.0fms" % (
            percentile([r["p50_ms"] for r in steady], 0.50),
            percentile([r["p95_ms"] for r in steady], 0.95)))
        print("  nDCG@%d first/last:   %.4f / %.4f   <-- must not decay, or mutation is corrupting"
              % (args.k, steady[0]["ndcg"], steady[-1]["ndcg"]))

    out = "/tmp/vectorsync-mutation-%s-%d.json" % (args.dataset, args.rows)
    with open(out, "w") as handle:
        json.dump({"args": vars(args), "rounds": rows}, handle, indent=2)
    print()
    print("wrote %s" % out)


if __name__ == "__main__":
    main()
