#!/usr/bin/env python3
"""Generates the scalability corpus: many Iceberg source tables with controlled content duplication.

The whole architectural claim is that re-embedding costs *novel content* rather than *changed rows*.
That claim is only interesting if the duplication rate is stated honestly, because a synthetic corpus
can be rigged to produce any dedup number you like. So duplication is an explicit parameter and the
result is a sweep, not a single point:

  dup00 -- every row's text distinct. Dedup cannot help, so this measures the OVERHEAD the design
           costs (hashing plus a store probe) in the case where it wins nothing. The most important
           point on the curve.
  dup50 / dup80 / dup95 -- drawn from progressively smaller content pools, the way real product
           catalogs, log lines, support macros and document revisions repeat themselves.

Reporting only the high-duplication point would be advocacy. The curve is the actual result, and the
zero point is where the honesty lives.
"""

import argparse
import hashlib
import json
import os
import random

CATEGORIES = [
    "audio", "computing", "outdoor", "kitchen", "apparel",
    "photography", "fitness", "garden", "office", "footwear",
]

# Fragment vocabulary. Pool SIZE controls duplication (see content_pool); these just make the text
# look like product copy rather than random bytes, so token counts are realistic for the embedder.
ADJECTIVES = ["Wireless", "Compact", "Rugged", "Premium", "Lightweight", "Weatherproof",
              "Ergonomic", "Portable", "Insulated", "Adjustable"]
NOUNS = ["Headphones", "Speaker", "Backpack", "Kettle", "Jacket", "Tripod", "Mat",
         "Pruner", "Lamp", "Boots", "Monitor", "Keyboard", "Bottle", "Tent", "Camera"]
CLAUSES = [
    "Rich low-end response and active noise suppression.",
    "Long battery life in a lightweight body.",
    "Weather-sealed seams and a reinforced base.",
    "Fast transfer speeds over a single cable.",
    "Packs down small for travel.",
    "Built for alpine conditions.",
    "Reliable in low light.",
    "Dishwasher safe and stain resistant.",
    "Machine washable with a durable finish.",
    "Tool-free assembly in under a minute.",
]


def content_pool(size, seed):
    """A pool of `size` distinct texts, drawn from to create a controlled duplication rate.

    Duplication is a parameter, not an emergent property of how many template fragments happened to
    be in the file. The first version of this generator had 1500 possible combinations and therefore
    reported 97% dedup on 50k rows, which says more about the generator than about the system.
    """
    rng = random.Random(seed)
    pool = []
    for i in range(size):
        pool.append("%s %s %d | %s %s" % (
            rng.choice(ADJECTIVES), rng.choice(NOUNS), i,
            rng.choice(CLAUSES), rng.choice(CLAUSES)))
    return pool


def unique_text(table_index, row_index):
    """Guaranteed distinct: a per-row nonce makes every hash novel."""
    nonce = hashlib.sha256(("%d:%d" % (table_index, row_index)).encode()).hexdigest()
    return "Item %d-%d with unique specification %s" % (table_index, row_index, nonce)


def build(mode, tables, rows_per_table, seed, pool_size=None):
    rng = random.Random(seed)
    total_rows = tables * rows_per_table
    pool = content_pool(pool_size, seed) if pool_size else None
    corpus = []
    for t in range(tables):
        name = "bench.%s_%03d" % (mode, t)
        table_rows = []
        for r in range(rows_per_table):
            if pool is not None:
                text = pool[rng.randrange(len(pool))]
            else:
                text = unique_text(t, r)
            table_rows.append({
                "id": "r-%05d" % r,
                "name": text.split(" | ")[0],
                "description": text.split(" | ")[1] if " | " in text else text,
                "category": CATEGORIES[r % len(CATEGORIES)],
            })
        corpus.append({"tableName": name, "rows": table_rows})
    return corpus


def content_stats(corpus, join_separator=" "):
    """The ground truth the system's own dedup number must be checked against.

    Computed here independently of the Java implementation, with the same assembly rule
    (name + separator + description), so an agreement between the two is meaningful rather
    than circular.
    """
    seen = set()
    total = 0
    for table in corpus:
        for row in table["rows"]:
            assembled = row["name"] + join_separator + row["description"]
            seen.add(hashlib.sha256(assembled.encode()).hexdigest())
            total += 1
    return {"rows": total, "distinctContent": len(seen),
            "uniqueFraction": round(len(seen) / total, 4) if total else 0.0,
            "expectedDedupRate": round(1 - len(seen) / total, 4) if total else 0.0}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", type=int, default=100)
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--seed", type=int, default=20260914)
    parser.add_argument("--out", default="/tmp/vsbench")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    total = args.tables * args.rows
    # A sweep rather than a single flattering number. The dedup win is a function of how much
    # duplicate content the corpus actually holds, so the honest result is the curve, and the
    # zero-duplication point is the one that shows the overhead this design costs when it cannot help.
    modes = [
        ("dup00", None),                    # every row distinct: dedup cannot help
        ("dup50", total // 2),
        ("dup80", total // 5),
        ("dup95", total // 20),
    ]
    summary = {}
    for mode, pool_size in modes:
        corpus = build(mode, args.tables, args.rows, args.seed, pool_size)
        path = os.path.join(args.out, "%s.json" % mode)
        with open(path, "w") as handle:
            json.dump(corpus, handle)
        stats = content_stats(corpus)
        summary[mode] = stats
        print("%-8s %3d tables x %4d rows = %6d rows | distinct %6d | dedup %5.1f%%"
              % (mode, args.tables, args.rows, stats["rows"], stats["distinctContent"],
                 stats["expectedDedupRate"] * 100))

    with open(os.path.join(args.out, "expected.json"), "w") as handle:
        json.dump(summary, handle, indent=2)


if __name__ == "__main__":
    main()
