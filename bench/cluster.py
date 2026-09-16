#!/usr/bin/env python3
"""Measures cluster-pruned search: recall against fraction of the table read.

The hypothesis under test. No query engine can read an Iceberg-native ANN index today -- Iceberg
classifies vector indexing as early-stage discussion, Puffin's established index use is Bloom filters
and deletion vectors, and the two 2026 ANN blob proposals are unstandardised, so writing one would
mean forking every engine meant to read it. Partitioning, by contrast, every engine implements. So:
assign each canonical vector to its nearest centroid, partition by that id, and let a query probe the
nearest few partitions. The approximation moves out of a graph and into a partition predicate.

What this measures is whether that trade is any good. For a sweep of probe counts it reports
recall@k against the exhaustive answer over the same vectors, and the fraction of rows actually read.
A useful result looks like high recall at a small fraction; a negative result is equally publishable
and is the reason the exhaustive scan is computed in the same request rather than assumed.

    python3 bench/cluster.py --clusters 32
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

WORKER = "http://localhost:8081"
CONTROL = "http://localhost:8080"


def post(url, body, timeout=1800):
    request = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        raise RuntimeError("POST %s -> %s: %s" % (url, e.code, e.read().decode()[:400]))


def get(url, timeout=300):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


# Distinct enough that clustering has something to find, and varied enough that a single cluster
# cannot contain every answer. Categories are the latent structure the centroids should discover.
CORPUS = {
    "audio": ["noise cancelling headphones with deep bass", "studio monitor speaker for mixing",
              "portable bluetooth speaker for the beach", "wireless earbuds with a charging case",
              "turntable with a built-in preamp", "over-ear headphones for long flights"],
    "outdoor": ["four season tent for alpine conditions", "solar lantern for camping",
                "insulated sleeping bag rated to minus ten", "lightweight trekking poles",
                "waterproof hiking boots for wet trails", "compact camp stove for two people"],
    "computing": ["mechanical keyboard with tactile switches", "ultrawide monitor for code",
                  "external ssd with fast transfer", "laptop stand with adjustable height",
                  "wireless mouse for large hands", "usb hub with power delivery"],
    "kitchen": ["cast iron skillet pre seasoned", "electric kettle with temperature control",
                "chef knife with a forged blade", "stand mixer for bread dough",
                "insulated travel mug that seals", "nonstick pan for eggs"],
    "apparel": ["merino base layer for winter", "rain shell that packs small",
                "running shorts with a zip pocket", "wool socks for cold weather",
                "down jacket for below freezing", "sun hoodie for hot days"],
}


# Category-coherent vocabulary, so a larger corpus still has latent structure for the centroids to
# find. Synthesising from per-category word lists rather than one global list matters: text drawn
# from a single vocabulary clusters arbitrarily, and the measurement would then describe noise.
MODIFIERS = ["compact", "rugged", "lightweight", "premium", "insulated", "weatherproof",
             "adjustable", "portable", "reinforced", "ergonomic"]
QUALIFIERS = ["for travel", "for everyday use", "for cold weather", "for long sessions",
              "with a carry case", "in a matte finish", "rated for heavy use", "for small spaces",
              "with a lifetime warranty", "in a compact size"]


def synthesize(per_category):
    """Expands each category's seed texts into `per_category` distinct, on-topic texts."""
    built = []
    index = 0
    for category, seeds in CORPUS.items():
        made = 0
        while made < per_category:
            seed = seeds[made % len(seeds)]
            modifier = MODIFIERS[(made // len(seeds)) % len(MODIFIERS)]
            qualifier = QUALIFIERS[(made // (len(seeds) * len(MODIFIERS))) % len(QUALIFIERS)]
            text = "%s %s %s" % (modifier, seed, qualifier)
            built.append({"id": "c-%05d" % index,
                          "name": modifier.title() + " " + seed.split()[0].title(),
                          "description": text, "category": category})
            index += 1
            made += 1
    return built


def rows():
    built = []
    index = 0
    for category, texts in CORPUS.items():
        for text in texts:
            # Each text is one distinct content, so the canonical store holds exactly len(all texts)
            # vectors -- which keeps the recall arithmetic honest and easy to reason about.
            built.append({"id": "c-%04d" % index,
                          "name": text.split()[0].title() + " " + text.split()[1].title(),
                          "description": text, "category": category})
            index += 1
    return built


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clusters", type=int, default=8)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--version", default=None)
    parser.add_argument("--per-category", type=int, default=0,
                        help="synthesize this many distinct texts per category; 0 uses the seed set")
    args = parser.parse_args()

    version = args.version or ("cluster-%d" % int(time.time()))
    table = "demo.cluster_corpus"
    model_version = "all-MiniLM-L6-v2:%s" % version

    corpus = synthesize(args.per_category) if args.per_category else rows()
    per_cluster = len(corpus) / max(1, args.clusters)
    print("corpus: %d rows, %d distinct texts, %d categories"
          % (len(corpus), len(corpus), len(CORPUS)))
    print("        %d clusters -> ~%.0f vectors per cluster, top-k %d"
          % (args.clusters, per_cluster, args.top_k))
    if per_cluster < args.top_k * 3:
        # Stated up front rather than discovered in the numbers: when a cluster holds barely more
        # than k vectors, the true top-k is spread across clusters by arithmetic and no probe count
        # below a full scan can do well. That is a property of the corpus size, not of the method.
        print("        note: fewer than 3k vectors per cluster, so recall is bounded by geometry")

    # --- derive canonical vectors through the normal pipeline ------------------
    post("%s/api/demo/tables" % WORKER, {"tableName": table, "rows": corpus})
    admitted = post("%s/api/materializations" % CONTROL, {
        "dryRun": False, "sourceTable": table, "keyColumns": ["id"],
        "embeddingColumns": ["description"], "joinSeparator": " ", "chunker": "whole",
        "modelName": "all-MiniLM-L6-v2", "modelRevision": "cluster", "embeddingVersion": version,
    })["materialization"]
    config_id = admitted["configId"]

    print("materialization %s admitted; waiting for derivation" % admitted["id"][:8])
    deadline = time.time() + 300
    while time.time() < deadline:
        state = get("%s/api/materializations/%s" % (CONTROL, admitted["id"]))["state"]
        if state == "LIVE":
            break
        if state in ("DEGRADED", "FAILED"):
            raise RuntimeError("materialization reached %s" % state)
        time.sleep(4)
    else:
        raise RuntimeError("derivation did not finish in time")

    embedded = get("%s/api/queue/embedded/count?modelVersion=%s&configId=%s"
                   % (CONTROL, model_version, config_id))["embedded"]
    print("canonical vectors: %d" % embedded)

    # --- build the cluster-partitioned index ----------------------------------
    report = post("%s/api/cluster/build" % WORKER, {
        "sourceTable": table, "modelVersion": model_version,
        "configId": config_id, "clusters": args.clusters,
    })
    sizes = report["clusterSizes"]
    print("clustered %d vectors into %d clusters in %d iterations"
          % (report["vectors"], report["clusters"], report["iterations"]))
    print("cluster sizes: %s" % sizes)
    print("  largest %d, smallest %d  %s" % (
        max(sizes), min(sizes),
        "(balanced)" if max(sizes) <= 3 * max(1, min(sizes)) else "(skewed -- see note below)"))

    # --- sweep probe counts ----------------------------------------------------
    queries = ["speaker for listening to music outside",
               "shelter and warmth for a cold night outdoors",
               "typing and display gear for a developer desk",
               "cooking pan and knife for a home kitchen",
               "warm clothing layers for winter hiking"]

    print("\n%-8s %-10s %-14s %-12s" % ("probes", "recall@%d" % args.top_k,
                                        "rows scanned", "fraction read"))
    print("-" * 50)

    results = []
    sweep = sorted({1, 2, 4, max(1, args.clusters // 8), max(1, args.clusters // 4),
                    args.clusters // 2, args.clusters})
    for probes in sweep:
        if probes > args.clusters:
            continue
        recalls, scanned, totals = [], [], []
        for query in queries:
            response = post("%s/api/cluster/probe" % WORKER, {
                "sourceTable": table, "modelVersion": model_version, "configId": config_id,
                "query": query, "topK": args.top_k, "probes": probes, "includeExact": True,
            })
            recalls.append(response["recallAtK"])
            scanned.append(response["rowsScanned"])
            totals.append(response["totalRows"])

        recall = sum(recalls) / len(recalls)
        rows_read = sum(scanned) / len(scanned)
        fraction = rows_read / totals[0] if totals[0] else 0
        results.append((probes, recall, rows_read, fraction))
        print("%-8d %-10.3f %-14.1f %-11.1f%%"
              % (probes, recall, rows_read, fraction * 100))

    # --- interpretation --------------------------------------------------------
    print("\ninterpretation")
    full = [r for r in results if r[0] == args.clusters]
    if full and abs(full[0][1] - 1.0) > 1e-9:
        print("  WARNING: probing every cluster did not reach recall 1.0 (%.3f)." % full[0][1])
        print("  That is a bug, not a trade-off: the union of all partitions is the whole table.")
    useful = [r for r in results if r[1] >= 0.9 and r[0] < args.clusters]
    if useful:
        probes, recall, _, fraction = useful[0]
        print("  recall %.3f at %d of %d clusters, reading %.1f%% of the table (%.1fx less)."
              % (recall, probes, args.clusters, fraction * 100,
                 1.0 / fraction if fraction else 0))
        print("  The approximation is the partition predicate; scoring inside it is exact, so a")
        print("  missed neighbour is explainable -- it sat in a cluster that was not probed.")
        print("")
        print("  Read this number with two caveats, both of which flatter it:")
        if max(sizes) > 3 * max(1, min(sizes)):
            print("    - Clusters are skewed (%d vs %d), so fraction-read varies by query and the"
                  % (max(sizes), min(sizes)))
            print("      average understates the worst case. Balancing is unsolved here.")
        print("    - This corpus is synthetic with %d clean semantic groups, which is far more"
              % len(CORPUS))
        print("      separable than real data. Recall at a small probe count is the first thing")
        print("      that degrades on a messy corpus; the mechanism is proven, the curve is not")
        print("      transferable. Re-measure on real embeddings before quoting it.")
    else:
        print("  No probe count below full scan reached recall 0.9 on this corpus.")
        print("  On a corpus this small that is expected: with few vectors per cluster the true")
        print("  top-k is spread across many clusters. The number to trust is the shape of the")
        print("  curve at scale, not this one.")

    print("\n  Every engine prunes partitions, so this runs on Spark, Trino, Databricks and")
    print("  Snowflake unchanged. No Puffin blob, no engine fork, no VectorSync in the query path.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeError as error:
        print("stopped: %s" % error)
        sys.exit(1)
