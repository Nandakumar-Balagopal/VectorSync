"""
Scale run: what happens to the derived tables as a source table grows.

Every other benchmark in this directory measures a claim about inference cost. This one measures
the things inference cost was hiding. With the real embedding provider the model is the bottleneck
-- roughly 10 rows/sec end to end, which puts ten million rows near 278 hours -- so a run long
enough to say anything about Iceberg behaviour has to stop paying for the model. The
inference-avoidance claim is already measured separately against real embeddings; what is
unmeasured is:

  * Throughput as a function of table size. If a pass is O(history) rather than O(change), rows/sec
    decays and the decay is the finding.
  * Fragmentation. Each pass commits, each commit writes files, so data files per record falls
    steadily -- and compaction either arrests that or does not.
  * Metadata growth. metadata.json is re-parsed on every catalog load because caching is
    deliberately off, so its size is on the critical path of every pass.
  * Tier 3 on a timer against a table that never stops changing, which is the case the coverage
    digest exists for: it decides reuse, append, or refit on every tick.
  * Query latency as the projection grows, which is what a user actually feels.

Writes a CSV sample per poll plus a summary, so the shape over time is recoverable rather than only
the endpoint. Interruptible: the CSV is flushed per sample, so a run stopped early is still data.

  python3 bench/scale.py --target-rows 400000 --minutes 120 --batch 2000
"""

import argparse
import csv
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from real import (  # noqa: E402  -- reuses the existing harness rather than duplicating it
    CONTROL,
    WORKER,
    get,
    post,
)

# Text with enough shape that chunking and hashing behave as they would on prose, and enough
# variation that no two rows collapse to the same content hash -- a corpus that deduplicated to one
# vector would measure nothing about scale.
TOPICS = [
    "wireless headphones with active noise cancellation and a long battery life",
    "a stainless steel insulated bottle that keeps liquids cold for a full day",
    "mechanical keyboard with hot swappable switches and per key backlighting",
    "a lightweight running shoe with a carbon plate for road racing",
    "espresso machine with a built in grinder and adjustable pressure profiling",
    "an ultrawide monitor with factory colour calibration for photo editing",
    "hiking backpack with a ventilated frame and a rain cover in the lid",
    "a robot vacuum that maps rooms with lidar and empties itself",
    "noise isolating earbuds for swimming with onboard music storage",
    "a standing desk with programmable height presets and cable management",
]

CATEGORIES = ["audio", "outdoors", "computing", "kitchen", "fitness"]


def row(index):
    topic = TOPICS[index % len(TOPICS)]
    return {
        "id": "row-%08d" % index,
        "name": "Product %d" % index,
        # The index inside the text is what guarantees distinct content per row.
        "description": "%s (unit %d, revision 0)" % (topic, index),
        "category": CATEGORIES[index % len(CATEGORIES)],
        "price": 10.0 + (index % 900),
    }


SPEC = {
    "keyColumns": ["id"],
    "embeddingColumns": ["name", "description"],
    "joinSeparator": " ",
    "chunker": "whole",
    "modelName": "all-MiniLM-L6-v2",
    "modelRevision": "scale-run",
    "embeddingVersion": "v1",
}


def admit(table):
    """
    Registers the table, or finds the existing materialization for it.

    Admission is unique on (source_table, config_id), so re-running this against a table that was
    already admitted collides rather than creating a second scope -- which is what we want, since a
    resumed run should keep appending to the same projection.
    """
    body = dict(SPEC)
    body["sourceTable"] = table
    body["freshnessSlaSeconds"] = 300
    try:
        created = post("%s/api/materializations" % CONTROL, body, timeout=300)
        if created.get("id"):
            return created["id"], created["configId"]
    except Exception as failure:
        if "already exists" not in str(failure):
            raise

    for state in ("LIVE", "VALIDATED", "BACKFILLING", "DEGRADED", "REGISTERED"):
        for entry in get("%s/api/materializations?state=%s" % (CONTROL, state)):
            if entry.get("sourceTable") == table:
                return entry["id"], entry["configId"]
    raise RuntimeError("could not admit or find a materialization for %s" % table)


def maintenance_status():
    try:
        return get("%s/api/maintenance/status" % WORKER, timeout=300)
    except Exception as e:
        return {"error": str(e)}


def materialization(materialization_id):
    try:
        return get("%s/api/materializations/%s" % (CONTROL, materialization_id), timeout=120)
    except Exception as e:
        return {"error": str(e)}


def metrics(table, config_id):
    try:
        return get("%s/api/derive/metrics?sourceTable=%s&configId=%s"
                   % (WORKER, table, config_id), timeout=120)
    except Exception:
        return {}


def cluster_status(table, config_id):
    """The one scope this run creates, flattened out of the per-source-table scope list."""
    try:
        body = get("%s/api/cluster/status?sourceTable=%s&configId=%s"
                   % (WORKER, table, config_id), timeout=300)
        scopes = body.get("scopes") or []
        return scopes[0] if scopes else {}
    except Exception:
        return {}


def query_latency(table, config_id, samples=3):
    """
    Milliseconds per exact top-k over the projection.

    Deliberately the in-process endpoint rather than Trino. It scans the whole projection with no
    pruning, which makes it the worst case and therefore the one that shows growth: an engine-side
    query would be faster and would also mix in engine cost, and this run is about what the data
    layer does as it grows.
    """
    timings = []
    for i in range(samples):
        started = time.time()
        try:
            post("%s/api/derive/search" % WORKER,
                 {"sourceTable": table, "configId": config_id,
                  "query": TOPICS[i % len(TOPICS)], "k": 10},
                 timeout=300)
            timings.append((time.time() - started) * 1000.0)
        except Exception:
            # A failed query is a real observation, but recording it as a latency would corrupt the
            # percentiles. Counted by the caller via the returned length.
            pass
    return timings


def derived_view(status, table, config_id):
    """Pulls the projection, content map and embedding store rows out of a maintenance status."""
    wanted = {
        "projection": "vectors_%s_%s" % (table.replace(".", "_"), config_id),
        "content_map": "content_map",
        "embedding_store": "embedding_store",
    }
    out = {}
    for label, name in wanted.items():
        for entry in status.get("tables", []):
            if entry.get("table") == name:
                out[label] = entry
                break
        out.setdefault(label, {})
    return out


FIELDS = [
    "elapsed_s", "rows_seeded", "state", "watermark",
    "chunks_processed", "inference_calls", "inference_avoided_rate",
    "proj_records", "proj_files", "proj_bytes", "proj_avg_records_per_file", "proj_snapshots",
    "cmap_records", "cmap_files", "cmap_avg_records_per_file",
    "store_records", "store_files",
    "cluster_vectors", "cluster_centroids", "cluster_freshness",
    "query_p50_ms", "query_ok",
    "seed_rate_rows_per_s", "derive_rate_rows_per_s",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="default.scale_run")
    parser.add_argument("--target-rows", type=int, default=400000)
    parser.add_argument("--minutes", type=float, default=120.0)
    parser.add_argument("--batch", type=int, default=2000,
                        help="rows per append; also files per seed commit, so it sets the "
                             "fragmentation the run starts from")
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    parser.add_argument("--out", default="/tmp/vectorsync-scale")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    csv_path = os.path.join(args.out, "samples.csv")
    deadline = time.time() + args.minutes * 60.0
    started = time.time()

    print("scale run: table=%s target=%d rows budget=%.0f min batch=%d"
          % (args.table, args.target_rows, args.minutes, args.batch), flush=True)

    # Round zero creates the table, so the run starts from no history rather than from whatever a
    # previous run left behind.
    print("seeding first batch and admitting...", flush=True)
    post("%s/api/demo/tables" % WORKER,
         {"tableName": args.table, "rows": [row(i) for i in range(args.batch)]}, timeout=3600)
    materialization_id, config_id = admit(args.table)
    print("admitted id=%s config=%s" % (materialization_id, config_id), flush=True)

    rows_seeded = args.batch
    last_poll = 0.0
    last_records = 0
    last_records_at = started
    samples = []

    with open(csv_path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()

        while True:
            now = time.time()
            if now >= deadline:
                print("time budget reached", flush=True)
                break

            # Keep feeding until the target is met, then stop writing and let the pipeline catch
            # up -- otherwise the run ends mid-backlog and the final numbers describe a table that
            # is still being written rather than a steady state.
            if rows_seeded < args.target_rows:
                batch = min(args.batch, args.target_rows - rows_seeded)
                seed_started = time.time()
                try:
                    post("%s/api/demo/tables/append" % WORKER,
                         {"tableName": args.table,
                          "rows": [row(i) for i in range(rows_seeded, rows_seeded + batch)]},
                         timeout=3600)
                    rows_seeded += batch
                    seed_rate = batch / max(0.001, time.time() - seed_started)
                except Exception as e:
                    print("append failed at %d rows: %s" % (rows_seeded, e), flush=True)
                    seed_rate = 0.0
            else:
                seed_rate = 0.0
                time.sleep(5)

            if now - last_poll < args.poll_seconds:
                continue
            last_poll = now

            status = maintenance_status()
            view = derived_view(status, args.table, config_id)
            entry = materialization(materialization_id)
            counters = metrics(args.table, config_id)
            clusters = cluster_status(args.table, config_id)
            timings = query_latency(args.table, config_id)

            proj = view.get("projection", {})
            records = proj.get("records", 0) or 0
            # Derived rows per second over the interval, which is the number that decays if a pass
            # is O(history) instead of O(change).
            interval = max(0.001, now - last_records_at)
            derive_rate = max(0, records - last_records) / interval
            last_records, last_records_at = records, now

            sample = {
                "elapsed_s": round(now - started, 1),
                "rows_seeded": rows_seeded,
                "state": entry.get("state", entry.get("error", "?")),
                "watermark": entry.get("incrementalWatermark", 0) or 0,
                "chunks_processed": counters.get("chunksProcessed", 0),
                "inference_calls": counters.get("inferenceCalls", 0),
                "inference_avoided_rate": round(counters.get("inferenceAvoidedRate", 0.0), 4),
                "proj_records": records,
                "proj_files": proj.get("dataFiles", 0),
                "proj_bytes": proj.get("bytes", 0),
                "proj_avg_records_per_file": proj.get("avgRecordsPerFile", 0),
                "proj_snapshots": proj.get("snapshots", 0),
                "cmap_records": view.get("content_map", {}).get("records", 0),
                "cmap_files": view.get("content_map", {}).get("dataFiles", 0),
                "cmap_avg_records_per_file":
                    view.get("content_map", {}).get("avgRecordsPerFile", 0),
                "store_records": view.get("embedding_store", {}).get("records", 0),
                "store_files": view.get("embedding_store", {}).get("dataFiles", 0),
                "cluster_vectors": clusters.get("indexedVectors", 0),
                "cluster_centroids": clusters.get("centroids", 0),
                "cluster_freshness": clusters.get("freshness", "-"),
                "query_p50_ms": round(statistics.median(timings), 1) if timings else 0.0,
                "query_ok": len(timings),
                "seed_rate_rows_per_s": round(seed_rate, 1),
                "derive_rate_rows_per_s": round(derive_rate, 1),
            }
            samples.append(sample)
            writer.writerow(sample)
            handle.flush()

            print("t=%5.0fs seeded=%7d derived=%7d files=%4d avg/file=%6d "
                  "derive=%6.0f rows/s q50=%6.1fms state=%s clusters=%s/%s"
                  % (sample["elapsed_s"], rows_seeded, records, sample["proj_files"],
                     sample["proj_avg_records_per_file"], derive_rate, sample["query_p50_ms"],
                     sample["state"], sample["cluster_vectors"], sample["cluster_centroids"]),
                  flush=True)

            # Stop once the pipeline has caught up with everything written, which is the point at
            # which the numbers describe a settled table.
            if rows_seeded >= args.target_rows and records >= rows_seeded:
                print("pipeline caught up with %d rows" % rows_seeded, flush=True)
                break

    summary = {
        "table": args.table,
        "rowsSeeded": rows_seeded,
        "elapsedSeconds": round(time.time() - started, 1),
        "samples": len(samples),
        "finalDerivedRecords": samples[-1]["proj_records"] if samples else 0,
        "finalDataFiles": samples[-1]["proj_files"] if samples else 0,
        "finalAvgRecordsPerFile": samples[-1]["proj_avg_records_per_file"] if samples else 0,
        "caughtUp": bool(samples) and samples[-1]["proj_records"] >= rows_seeded,
        "meanDeriveRateRowsPerSecond": round(statistics.mean(
            [s["derive_rate_rows_per_s"] for s in samples[1:]] or [0.0]), 1),
        "queryP50MsFirstSample": samples[0]["query_p50_ms"] if samples else 0.0,
        "queryP50MsLastSample": samples[-1]["query_p50_ms"] if samples else 0.0,
        "csv": csv_path,
    }
    summary_path = os.path.join(args.out, "summary.json")
    with open(summary_path, "w") as handle:
        json.dump(summary, handle, indent=2)

    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
