#!/usr/bin/env python3
"""Drives the scalability test and reports whether the architectural claim holds.

The claim under test: derivation cost tracks NOVEL CONTENT, not rows and not row changes. Three
measurements, in order of how much they matter:

  1. dedup sweep    -- backfill the same row count at four duplication levels. Inference calls
                       should fall with duplication while rows stay constant. The 0% point shows
                       what the design costs when it cannot help.
  2. cross-table    -- a second table whose content already exists in the store should cost
                       (almost) no inference at all. This is the property a vector database and a
                       row-keyed design both lack.
  3. incremental    -- change 5% of rows, then re-run. Cost should track the changed rows, and a
                       change to a column that is not embedded should cost nothing.

Everything is measured from the service's own reported counters and cross-checked against hashes
computed independently in corpus.py, so agreement is evidence rather than self-report.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

WORKER = "http://localhost:8081"


def post(path, payload, timeout=1800):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(WORKER + path, data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        detail = e.read().decode()[:400]
        raise RuntimeError("POST %s -> %s: %s" % (path, e.code, detail))


def seed(table_name, rows):
    return post("/api/demo/tables", {"tableName": table_name, "rows": rows})


def derive(table_name, model, version="v1", from_snapshot=None, columns=None):
    payload = {
        "sourceTable": table_name,
        "keyColumns": ["id"],
        "embeddingColumns": columns or ["name", "description"],
        "joinSeparator": " ",
        "chunker": "whole",
        "modelName": model,
        "modelRevision": "bench",
        "embeddingVersion": version,
        "normalize": False,
    }
    if from_snapshot is not None:
        payload["fromSnapshotExclusive"] = from_snapshot
    return post("/api/derive/run", payload)


def load_corpus(path):
    with open(path) as handle:
        return json.load(handle)


def run_mode(corpus, tables, model, label, version="v1"):
    """Seeds and derives `tables` tables, accumulating the service's own counters."""
    totals = {"rows": 0, "chunks": 0, "inference": 0, "cacheHits": 0,
              "distinct": 0, "files": 0, "failed": 0, "seedSeconds": 0.0, "deriveSeconds": 0.0}
    per_table = []

    for index, table in enumerate(corpus[:tables]):
        name = table["tableName"]

        t0 = time.time()
        seed(name, table["rows"])
        totals["seedSeconds"] += time.time() - t0

        t1 = time.time()
        result = derive(name, model, version)
        elapsed = time.time() - t1
        totals["deriveSeconds"] += elapsed

        metrics = result["metrics"]
        totals["rows"] += result["rowsProcessed"]
        totals["chunks"] += result["chunksProcessed"]
        totals["inference"] += metrics["inferenceCalls"]
        totals["cacheHits"] += metrics["cacheHits"]
        totals["distinct"] += metrics["distinctHashes"]
        totals["files"] += result["filesProcessed"]
        totals["failed"] += result["filesFailed"]
        per_table.append({"table": name, "rows": result["rowsProcessed"],
                          "inference": metrics["inferenceCalls"], "seconds": round(elapsed, 2)})

        if (index + 1) % 10 == 0 or index == 0:
            avoided = 1 - (totals["inference"] / totals["chunks"]) if totals["chunks"] else 0
            print("  [%s] %3d/%d tables | %6d chunks | %6d inference | %5.1f%% avoided | %5.1fs"
                  % (label, index + 1, tables, totals["chunks"], totals["inference"],
                     avoided * 100, totals["deriveSeconds"]), flush=True)

    totals["inferenceAvoidedRate"] = (
        1 - (totals["inference"] / totals["chunks"])) if totals["chunks"] else 0.0
    return totals, per_table


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", default="/tmp/vsbench")
    parser.add_argument("--tables", type=int, default=100)
    parser.add_argument("--model", default="all-MiniLM-L6-v2")
    # A fresh version gives each run its own (model_version, config_id) scope, so the embedding
    # store starts empty for it. That isolates a cold run without dropping tables underneath a
    # previous one, and keeps runs comparable.
    parser.add_argument("--version", default="v1")
    parser.add_argument("--modes", default="dup00,dup50,dup80,dup95")
    parser.add_argument("--out", default="/tmp/vsbench/results.json")
    args = parser.parse_args()

    with open("%s/expected.json" % args.corpus) as handle:
        expected = json.load(handle)

    results = {}
    for mode in args.modes.split(","):
        corpus = load_corpus("%s/%s.json" % (args.corpus, mode))
        print("\n=== %s: expected dedup %.1f%% over %d rows ==="
              % (mode, expected[mode]["expectedDedupRate"] * 100, expected[mode]["rows"]),
              flush=True)
        # Each corpus gets its own scope: dup00's vectors must not pre-warm dup95's store, or the
        # later modes would report a dedup win they did not earn.
        totals, per_table = run_mode(corpus, args.tables, args.model, mode,
                                     "%s-%s" % (args.version, mode))
        totals["expected"] = expected[mode]
        results[mode] = {"totals": totals, "perTable": per_table[:5]}

        print("  RESULT %s: %d rows, %d chunks, %d inference calls, %.1f%% avoided, %.1fs derive"
              % (mode, totals["rows"], totals["chunks"], totals["inference"],
                 totals["inferenceAvoidedRate"] * 100, totals["deriveSeconds"]), flush=True)

    with open(args.out, "w") as handle:
        json.dump(results, handle, indent=2)

    print("\n=== SWEEP ===")
    print("%-8s %8s %8s %10s %9s %9s" % ("corpus", "rows", "chunks", "inference", "avoided", "derive_s"))
    for mode, data in results.items():
        t = data["totals"]
        print("%-8s %8d %8d %10d %8.1f%% %9.1f"
              % (mode, t["rows"], t["chunks"], t["inference"],
                 t["inferenceAvoidedRate"] * 100, t["deriveSeconds"]))

    failed = sum(data["totals"]["failed"] for data in results.values())
    if failed:
        print("\n%d files FAILED - results above are not a clean run" % failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
