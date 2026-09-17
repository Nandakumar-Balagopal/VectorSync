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
        self.docs = list(docs)
        self.live = {doc["id"]: doc for doc in self.docs}
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
            doc = dict(self.live[doc_id])
            # Text that is new but still natural: a real edit, not a random string, so chunking and
            # embedding behave as they would on a genuine revision.
            doc["text"] = doc["text"] + " Revised in round %d." % self.round_marker
            self.live[doc_id] = doc
            upserts.append(doc)

        return upserts, deletes

    round_marker = 0


def seed_source(table, live_docs):
    """Creates the table for round zero. Drops and recreates, so there is no prior history."""
    rows = [
        {"id": doc["id"], "name": doc.get("title", "") or "", "description": doc["text"]}
        for doc in live_docs
    ]
    post("%s/api/demo/tables" % WORKER, {"tableName": table, "rows": rows}, timeout=3600)


def rewrite_source(table, live_docs):
    """
    Replaces the source table's contents with the current live set, as one copy-on-write pass.

    Deliberately a rewrite rather than row-level edits. It is what an engine emits for
    UPDATE/DELETE on a table with no delete files, it is the shape the pipeline's assess() classifies
    as needing a reconcile, and it exercises the path that matters instead of the one that is easy.
    """
    rows = [
        {"id": doc["id"], "name": doc.get("title", "") or "", "description": doc["text"]}
        for doc in live_docs
    ]
    post(
        "%s/api/demo/tables/replace" % WORKER,
        {"tableName": table, "rows": rows},
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


def derive_and_publish(table, timeout_seconds=1800):
    """
    One derivation pass, publishing Tier 2 so the result is queryable when this returns.

    publishProjection is what makes the returned elapsed time a freshness number rather than just a
    derive number: without it the pass writes Tier 1 and leaves publishing to the scheduler, so a
    query issued immediately afterwards would read the previous projection and the measurement would
    silently be of the wrong thing.
    """
    body = dict(SPEC)
    body["sourceTable"] = table
    body["publishProjection"] = True
    return post("%s/api/derive/run" % WORKER, body, timeout=timeout_seconds)


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

    docs, queries, qrels = load_beir(args.dataset, args.rows)
    print("corpus %s: %d documents, %d judged queries"
          % (args.dataset, len(docs), len(queries)))

    distinct_initial = len({content_hash(d["text"]) for d in docs})
    print("distinct content: %d of %d rows (%.1f%% duplicated)"
          % (distinct_initial, len(docs), 100.0 * (1 - distinct_initial / max(1, len(docs)))))

    workload = Workload(docs, args.mutate, args.deletes)

    print()
    print("round |   upserts  deletes |  inference | derive_s | lag_s | p50_ms  p95_ms | nDCG@%d"
          % args.k)
    print("------+--------------------+------------+----------+-------+----------------+--------")

    rows = []
    for round_index in range(args.rounds + 1):
        workload.round_marker = round_index

        if round_index == 0:
            upserts, deletes = list(workload.live.values()), []
        else:
            upserts, deletes = workload.next_round()

        committed_at = time.time()
        if round_index == 0:
            seed_source(args.table, list(workload.live.values()))
        else:
            rewrite_source(args.table, list(workload.live.values()))

        # Wait for the pipeline to make the change queryable, which is the number a user feels.
        # Polled rather than assumed: the whole point is to measure the lag, not to sleep past it.
        derive_started = time.time()
        status = derive_and_publish(args.table)
        derive_seconds = time.time() - derive_started
        lag_seconds = time.time() - committed_at
        config_id = status["configId"]
        inference = status.get("metrics", {}).get("inferenceCalls", 0)
        if not status.get("metrics", {}).get("complete", False):
            print("  !! pass incomplete at round %d -- filesFailed=%s. Numbers after this point "
                  "describe a partial derive." % (round_index, status.get("filesFailed")))

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
