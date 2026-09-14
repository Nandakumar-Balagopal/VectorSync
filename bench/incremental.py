#!/usr/bin/env python3
"""Tests that an incremental pass costs the change, not the table.

Four cases, each isolating one claim:

  A. backfill            establishes the baseline for a table.
  B. append novel        new rows with content not in the store -> inference for the novel content
                         only; the pre-existing rows are not revisited.
  C. append duplicate    new rows whose content is already embedded -> ZERO inference. This is the
                         case a row-keyed design cannot get right: the rows are new, so it would
                         re-embed all of them.
  D. no change           an incremental pass with nothing appended -> zero work, watermark holds.

Case C is the one that matters. If it reports any inference calls, content addressing is not
actually doing anything and the whole architecture is decoration.
"""

import json
import sys
import time
import urllib.error
import urllib.request

WORKER = "http://localhost:8081"


def post(path, payload, timeout=1800):
    req = urllib.request.Request(WORKER + path, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        raise RuntimeError("POST %s -> %s: %s" % (path, e.code, e.read().decode()[:300]))


def spec(table, version):
    return {"sourceTable": table, "keyColumns": ["id"],
            "embeddingColumns": ["name", "description"], "joinSeparator": " ",
            "chunker": "whole", "modelName": "all-MiniLM-L6-v2",
            "modelRevision": "bench", "embeddingVersion": version, "normalize": False}


def derive(table, version, from_snapshot=None):
    payload = spec(table, version)
    if from_snapshot is not None:
        payload["fromSnapshotExclusive"] = from_snapshot
    t0 = time.time()
    result = post("/api/derive/run", payload)
    result["_elapsed"] = time.time() - t0
    return result


def show(label, result, expect_inference=None):
    m = result["metrics"]
    ok = "" if expect_inference is None else (
        " OK" if m["inferenceCalls"] == expect_inference else
        " MISMATCH (expected %d)" % expect_inference)
    print("  %-22s rows=%4d chunks=%4d inference=%4d cacheHits=%4d files=%d complete=%s %.1fs%s"
          % (label, result["rowsProcessed"], result["chunksProcessed"], m["inferenceCalls"],
             m["cacheHits"], result["filesProcessed"], m["complete"], result["_elapsed"], ok),
          flush=True)
    return m["inferenceCalls"]


def rows(prefix, count, texts):
    """`texts` cycles, so duplication is controlled by how many distinct entries it holds."""
    out = []
    for i in range(count):
        text = texts[i % len(texts)]
        out.append({"id": "%s-%05d" % (prefix, i), "name": text.split("|")[0].strip(),
                    "description": text.split("|")[1].strip(), "category": "bench"})
    return out


def main():
    version = sys.argv[1] if len(sys.argv) > 1 else "inc1"
    table = "bench.incremental_%s" % version

    base_texts = ["Widget %d | Description number %d for the widget." % (i, i) for i in range(50)]
    novel_texts = ["Gadget %d | Fresh description number %d." % (i, i) for i in range(50)]

    failures = []

    print("A. backfill 500 rows over 50 distinct texts")
    post("/api/demo/tables", {"tableName": table, "rows": rows("base", 500, base_texts)})
    a = derive(table, version)
    show("backfill", a, expect_inference=50)
    if a["metrics"]["inferenceCalls"] != 50:
        failures.append("A: expected 50 inference calls, got %d" % a["metrics"]["inferenceCalls"])
    snapshot = a["snapshotId"]

    print("\nB. append 300 rows carrying 50 NOVEL texts")
    post("/api/demo/tables/append", {"tableName": table, "rows": rows("new", 300, novel_texts)})
    b = derive(table, version, snapshot)
    show("incremental novel", b, expect_inference=50)
    if b["rowsProcessed"] > 300:
        failures.append("B: processed %d rows; an incremental pass must not revisit the "
                        "500 already-materialized rows" % b["rowsProcessed"])
    if b["metrics"]["inferenceCalls"] != 50:
        failures.append("B: expected 50 novel embeddings, got %d" % b["metrics"]["inferenceCalls"])
    snapshot = b["snapshotId"]

    print("\nC. append 300 MORE rows whose content is already embedded")
    post("/api/demo/tables/append", {"tableName": table, "rows": rows("dup", 300, base_texts)})
    c = derive(table, version, snapshot)
    show("incremental duplicate", c, expect_inference=0)
    if c["metrics"]["inferenceCalls"] != 0:
        failures.append("C: 300 new rows of already-embedded content cost %d inference calls; "
                        "content addressing is not working"
                        % c["metrics"]["inferenceCalls"])
    snapshot = c["snapshotId"]

    print("\nD. incremental pass with nothing appended")
    d = derive(table, version, snapshot)
    show("no-op", d, expect_inference=0)
    if d["chunksProcessed"] != 0 or not d["metrics"]["complete"]:
        failures.append("D: a no-op pass did work (%d chunks) or did not complete"
                        % d["chunksProcessed"])

    print("\n" + ("FAILURES:" if failures else "All incremental claims hold."))
    for f in failures:
        print("  - " + f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
