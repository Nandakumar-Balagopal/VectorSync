#!/usr/bin/env python3
"""The VectorSync architecture demo, in the order the story should be told.

One command, seven beats, real numbers printed as they happen. Designed to be run live in front of
an architect, so it is repeatable: every run uses a fresh embedding version, which gives it a private
(model_version, config_id) scope and therefore an empty store. Nothing is dropped and no previous run
is disturbed, so a second run in the same session tells the same story rather than reporting
suspiciously perfect cache hits inherited from the first.

    python3 bench/demo.py

Beats:
    1  A plain Iceberg table. Nothing has been derived yet.
    2  Admission: validated, with a cost estimate taken from Iceberg metadata without reading data.
    3  Nothing else is done by hand. A scheduler picks it up and drives it to LIVE.
    4  The claim: inference tracks distinct content, not rows.
    5  Cross-table reuse: a different table holding the same text costs nothing.
    6  Incremental: appending rows costs only the novel content among them.
    7  Lineage: which source snapshot, model revision and content produced this exact vector.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

WORKER = "http://localhost:8081"
CONTROL = "http://localhost:8080"
SEARCH = "http://localhost:8083"


# ---------------------------------------------------------------- presentation helpers

class Style:
    BOLD = "\033[1m"
    DIM = "\033[2m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    RESET = "\033[0m"


def beat(number, title):
    print("\n%s%s  %d. %s %s" % (Style.BOLD, Style.CYAN, number, title, Style.RESET), flush=True)
    print(Style.DIM + "   " + "-" * 68 + Style.RESET, flush=True)


def say(text):
    print("   " + text, flush=True)


def figure(label, value, note=""):
    suffix = (Style.DIM + "  " + note + Style.RESET) if note else ""
    print("   %-34s %s%s%s%s" % (label, Style.BOLD, Style.GREEN, value, Style.RESET) + suffix,
          flush=True)


def pause(enabled, prompt="   [enter to continue]"):
    """Lets a presenter talk between beats instead of racing the output."""
    if enabled:
        try:
            input(Style.DIM + prompt + Style.RESET)
        except EOFError:
            pass


# ---------------------------------------------------------------- http

def post(url, body, timeout=1800):
    request = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        raise RuntimeError("POST %s -> %s: %s" % (url, e.code, e.read().decode()[:300]))


def get(url, timeout=300):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        raise RuntimeError("GET %s -> %s: %s" % (url, e.code, e.read().decode()[:300]))


# ---------------------------------------------------------------- corpus

# A small pool drawn from repeatedly, which is what product catalogs, support macros and log lines
# actually look like. Duplication is the property under demonstration, so it is explicit here
# rather than incidental.
POOL = [
    "Solar Camping Lantern|Collapsible solar lantern with a 40-hour runtime.",
    "Trail Running Shoe|Grippy outsole with a breathable upper.",
    "Insulated Bottle|Keeps liquids cold for 24 hours.",
    "Weatherproof Backpack|Sealed seams and a padded laptop sleeve.",
    "Compact Tripod|Folds to 30cm and holds a full-frame body.",
    "Noise Cancelling Headphones|Active suppression with a 30-hour battery.",
    "Ergonomic Keyboard|Split layout with a cushioned wrist rest.",
    "Cast Iron Skillet|Pre-seasoned and oven safe to 260C.",
    "Merino Base Layer|Odour resistant and machine washable.",
    "Folding Camp Chair|Packs into an included shoulder bag.",
]


def rows(prefix, count, pool=POOL):
    built = []
    for i in range(count):
        text = pool[i % len(pool)]
        name, description = text.split("|")
        built.append({"id": "%s-%04d" % (prefix, i), "name": name,
                      "description": description, "category": "demo"})
    return built


def spec(table, version):
    return {
        "sourceTable": table,
        "keyColumns": ["id"],
        "embeddingColumns": ["name", "description"],
        "joinSeparator": " ",
        "chunker": "whole",
        "modelName": "all-MiniLM-L6-v2",
        "modelRevision": "demo",
        "embeddingVersion": version,
        "normalize": False,
    }


def wait_for_live(materialization_id, label, timeout_seconds=300):
    """Polls until LIVE. Prints each transition so the state machine is visible, not implied."""
    started = time.time()
    seen = None
    while time.time() - started < timeout_seconds:
        state = get("%s/api/materializations/%s" % (CONTROL, materialization_id))
        queue = get("%s/api/queue/stats?materializationId=%s" % (CONTROL, materialization_id))
        current = state.get("state")
        if current != seen:
            say("%-12s %-10s files: pending=%s leased=%s done=%s failed=%s"
                % (label, current, queue.get("pending"), queue.get("leased"),
                   queue.get("done"), queue.get("failed")))
            seen = current
        if current == "LIVE":
            return state
        if current in ("DEGRADED", "FAILED"):
            raise RuntimeError("%s reached %s: %s" % (label, current, state.get("lastError")))
        time.sleep(4)
    raise RuntimeError("%s did not reach LIVE within %ds" % (label, timeout_seconds))


def derive_metrics(table, config_id):
    """Reads the worker's own counters for the most recent pass over this table."""
    return get("%s/api/derive/metrics?sourceTable=%s&configId=%s" % (WORKER, table, config_id))


# ---------------------------------------------------------------- the demo

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=400)
    parser.add_argument("--step", action="store_true",
                        help="pause between beats so a presenter can talk")
    parser.add_argument("--version", default=None,
                        help="embedding version; defaults to a fresh one so the run is repeatable")
    args = parser.parse_args()

    # A fresh scope per run. Without this a second run inherits the first run's vectors and reports
    # 100%% cache hits everywhere, which looks like a rigged demo because it effectively is one.
    version = args.version or ("demo-%d" % int(time.time()))
    table_a = "demo.catalog_a"
    table_b = "demo.catalog_b"

    print("%s%sVectorSync — architecture demo%s" % (Style.BOLD, Style.CYAN, Style.RESET))
    print(Style.DIM + "embedding scope for this run: all-MiniLM-L6-v2:%s" % version + Style.RESET)

    # ---------------------------------------------------------------- 1
    beat(1, "A plain Iceberg table")
    post("%s/api/demo/tables" % WORKER, {"tableName": table_a, "rows": rows("a", args.rows)})
    distinct = len(POOL)
    figure("rows written", args.rows)
    figure("distinct texts among them", distinct,
           "each text repeats ~%d times" % (args.rows // distinct))
    say("Iceberg is the source of truth. Nothing has been derived from it yet.")
    pause(args.step)

    # ---------------------------------------------------------------- 2
    beat(2, "Admission: validate, and price it before committing")
    dry = post("%s/api/materializations" % CONTROL,
               dict(spec(table_a, version), dryRun=True))
    estimate = dry.get("estimate", {})
    figure("validation problems", len(dry.get("problems", [])) or "none")
    figure("estimated rows", estimate.get("estimatedRows"))
    figure("estimated files", estimate.get("estimatedFiles"))
    figure("config id", dry.get("configId"), "hash of the derivation function")
    say("The estimate comes from Iceberg manifest metadata. No data file was read.")
    say("Nothing was persisted: this was a dry run.")
    pause(args.step)

    # ---------------------------------------------------------------- 3
    beat(3, "Commit it, then stop touching it")
    admitted = post("%s/api/materializations" % CONTROL,
                    dict(spec(table_a, version), dryRun=False))
    materialization = admitted["materialization"]
    config_id = materialization["configId"]
    say("admitted as %s" % materialization["state"])
    say("")
    say("From here nothing is invoked by hand. A scheduler plans the work, a worker leases it.")
    wait_for_live(materialization["id"], table_a)
    pause(args.step)

    # ---------------------------------------------------------------- 4
    beat(4, "The claim: cost tracks distinct content, not rows")
    a = derive_metrics(table_a, config_id)
    figure("rows processed", a["rowsProcessed"])
    figure("distinct content", a["distinctHashes"])
    figure("inference calls", a["inferenceCalls"],
           "equals distinct content, not rows")
    figure("avoided", "%.1f%%" % (a["inferenceAvoidedRate"] * 100))
    embedded = get("%s/api/queue/embedded/count?modelVersion=all-MiniLM-L6-v2:%s&configId=%s"
                   % (CONTROL, version, config_id))
    figure("vectors recorded durably", embedded["embedded"])
    say("A vector is keyed by (content_hash, model_version, config_id) -- not by the row it")
    say("arrived on. So %d rows carrying %d distinct texts is %d units of work."
        % (a["rowsProcessed"], a["distinctHashes"], a["inferenceCalls"]))
    pause(args.step)

    # ---------------------------------------------------------------- 5
    beat(5, "A different table, the same text, no inference at all")
    post("%s/api/demo/tables" % WORKER, {"tableName": table_b, "rows": rows("b", args.rows)})
    second = post("%s/api/materializations" % CONTROL,
                  dict(spec(table_b, version), dryRun=False))["materialization"]
    say("same config id: %s" % (second["configId"] == config_id))
    wait_for_live(second["id"], table_b)

    b = derive_metrics(table_b, config_id)
    figure("rows processed", b["rowsProcessed"])
    figure("inference calls", b["inferenceCalls"], "the store already had this content")
    figure("cache hits", b["cacheHits"])
    after = get("%s/api/queue/embedded/count?modelVersion=all-MiniLM-L6-v2:%s&configId=%s"
                % (CONTROL, version, config_id))
    figure("vectors recorded durably", after["embedded"],
           "unchanged: nothing new was embedded")
    say("config_id deliberately excludes the source table, which is what lets reuse cross tables.")
    say("The reuse is served from shared durable state, so it holds across workers too.")
    pause(args.step)

    # ---------------------------------------------------------------- 6
    beat(6, "Incremental: appending rows costs only what is new")
    novel = ["Alpine Ice Axe|Forged head with a leash point.",
             "Down Sleeping Bag|Rated to minus 10C."]
    post("%s/api/demo/tables/append" % WORKER,
         {"tableName": table_a, "rows": rows("late", 150, POOL + novel)})
    say("appended 150 rows: mostly text already embedded, plus %d new texts" % len(novel))

    before_count = after["embedded"]
    deadline = time.time() + 180
    while time.time() < deadline:
        current = get("%s/api/queue/embedded/count?modelVersion=all-MiniLM-L6-v2:%s&configId=%s"
                      % (CONTROL, version, config_id))["embedded"]
        if current > before_count:
            figure("new vectors from the append", current - before_count,
                   "the %d novel texts only" % len(novel))
            break
        time.sleep(4)
    else:
        say(Style.YELLOW + "append not yet picked up; the scheduler runs every 15s" + Style.RESET)

    say("Change detection uses Iceberg's incremental append scan, so the pass reads only the")
    say("data files the commit added -- not the table.")
    pause(args.step)

    # ---------------------------------------------------------------- 7
    beat(7, "What a query engine sees, and where each vector came from")
    sample = get("%s/api/derive/projection/sample?sourceTable=%s&configId=%s&limit=4"
                 % (WORKER, table_a, config_id))
    say("projection table: vectors_%s_%s" % (table_a.replace(".", "_"), config_id))
    say("an ordinary Iceberg table; Spark or Trino reads it with no VectorSync in the path")
    say("")
    for row in sample.get("rows", []):
        say("%srow %-10s chunk %-2s dim %-4s%s" % (
            Style.DIM, row.get("source_row_id"), row.get("chunk_ordinal"),
            row.get("embedding_dim"), Style.RESET))
        say("   content  %s..." % (row.get("content_hash") or "")[:24])
        say("   model    %s" % row.get("model_version"))
        say("   from     snapshot %s, sequence %s"
            % (row.get("source_snapshot_id"), row.get("source_sequence_number")))
        say("   text     %s" % (row.get("text") or "")[:44])
    say("")
    sampled = sample.get("rows", [])
    hashes = {row.get("content_hash") for row in sampled}
    if sampled and len(hashes) < len(sampled):
        figure("rows shown", len(sampled))
        figure("distinct vectors behind them", len(hashes),
               "the deduplication, visible in the serving table")
        say("Different source rows, one content hash, one vector. The projection resolves the")
        say("join so an engine still sees one row per source row.")
        say("")
    say("Every row carries the content hash, the model revision and the source snapshot, so")
    say("(snapshot, config_id) reproduces this vector set exactly -- and an incremental rebuild")
    say("is checkable against a full one.")

    # ---------------------------------------------------------------- close
    print("\n%s%sWhat this is not%s" % (Style.BOLD, Style.YELLOW, Style.RESET))
    print(Style.DIM + """   Not a vector database: it does not answer top-k and does not compete on ANN latency.
   The serving projection is rebuilt whole on each publish, which is fine at demo scale and
   is the first thing that has to change for a large table. One unit of work is a whole
   Iceberg data file, so a 512MB source file needs row-level batching before production.
   Deletes and MERGE are detected and refused rather than silently skipped.""" + Style.RESET)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeError as error:
        print("\n%sdemo stopped: %s%s" % (Style.YELLOW, error, Style.RESET))
        sys.exit(1)
