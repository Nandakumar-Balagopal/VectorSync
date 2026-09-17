# Contributing to VectorSync

The project is Apache-2.0 (see `LICENSE`). Contributing means agreeing that your contribution is
licensed under those terms; there is no CLA.

This document is specific to this repository. If something here is wrong, fixing it is a welcome
patch — a stale contributor guide costs more than no guide.

---

## Module layout

| Module | What lives there |
|---|---|
| `common` | DTOs, constants, cosine similarity. Pure Java, no framework. |
| `vectorsync-format` | The on-disk contract: Iceberg catalog construction, Tier-1 schemas and codecs, content hashing, spec identity (`config_id`), the serving projection, SQL view generation. |
| `control-plane` | Spring Boot + Postgres. Validated admission, cost estimation, the materialization state machine, the leased work queue, the durable dedup record. |
| `worker` | Spring Boot. Change detection over Iceberg snapshots, chunking, dedup-aware derivation, Tier-1 writes. |
| `search-service` | Spring Boot + Lucene. Index build, evaluation, promotion, provenance. Not the serving tier. |
| `embedding-service` | Python FastAPI + sentence-transformers. Stateless, resident models. |
| `dashboard` | React + Carbon. Demo and ops only; nothing depends on it. |
| `bench` | Python measurement harness. Every performance claim in the README comes from here. |
| `deployment` | Dockerfiles, compose overlays, E2E scripts. |

Layering: `common` <- `vectorsync-format` <- `control-plane` / `worker` / `search-service`. Format
semantics — a schema, an id derivation, a resolution rule — belong in `vectorsync-format`. Three
services previously carried their own copy of the version-resolution rule and they had drifted,
which for a project whose claim is "an open, engine-neutral format" is the one bug that cannot be
tolerated: the resolution semantics *are* the product.

### `common` and `vectorsync-format` must not depend on Spring

Their consumers include the Spring services, Spark batch jobs and eventually a Trino plugin, none of
which can share a framework container. The Spring services hold thin `@Value`-binding adapters over
the format layer and nothing more.

This is now mechanically enforced: both modules configure `maven-enforcer-plugin` with a
`bannedDependencies` rule that rejects any `org.springframework*` artifact, direct or transitive.
The rule is bound to `validate`, so it fires on every `mvn compile`, `mvn test-compile` and
`mvn test` — a Spring dependency is rejected when it is added to a pom, before any import exists.
Version and rule configuration live in each module's own `pom.xml`; the root `pom.xml` is untouched
by it and still contributes only its `<modules>`, `<dependencyManagement>` and `<build>`
plugin-version sections.

If you genuinely need framework behaviour in the format layer, the answer is an adapter in the
service, not a relaxed rule.

---

## Build and test

The authoritative gate, in this order:

```bash
mvn -o clean test-compile     # ~9.5s on a warm local repo
mvn -o clean test             # 117 tests when this was written
```

`clean` is not optional. `mvn -q compile` has twice masked real compilation errors on this project
through stale incremental compilation: the reactor reused class files whose sources no longer
compiled, reported success, and the error surfaced later on a machine with a cold `target/`. Treat a
green `-q compile` as no information.

`-o` (offline) is the local convention and keeps the gate from depending on network weather. It
requires a warm `~/.m2`; the first build on a fresh machine has to run online. CI runs
`mvn -B clean test` online.

Do not run two Maven builds against this tree concurrently. They race on `target/` and produce
failures that do not reproduce.

### Docker is required for the full suite

`control-plane/src/test/java/io/vectorsync/controlplane/service/DurableDedupTest.java` runs against
real Postgres via Testcontainers (`postgres:16-alpine`), and has to: both the enqueue path and the
dedup record use `INSERT ... ON CONFLICT DO NOTHING`, which H2 cannot parse even in PostgreSQL
compatibility mode. Substituting H2 would mean weakening production SQL to suit a test. Flyway runs
in that test too, so the migrations are exercised rather than assumed.

On macOS with Colima, Testcontainers needs two environment variables and will otherwise fail in ways
that look like a broken test:

```bash
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_RYUK_DISABLED=true
```

Ryuk (the resource reaper) cannot mount Colima's socket, so it must be disabled there. On Linux and
on GitHub runners, leave Ryuk **enabled** — the disable is a Colima-specific workaround, and turning
it off elsewhere leaks containers.

Docker Desktop on macOS needs neither variable.

---

## Comments

This is the repository's most unusual convention and the one most worth keeping.

**A comment explains why the code is the way it is, and cites the measurement, incident or
documented API behaviour that made it that way.** Two exemplars to read before writing your first
one:

- `vectorsync-format/src/main/java/io/vectorsync/format/catalog/IcebergCatalogConfig.java`, the
  `cache-enabled=false` comment. It does not say "disable caching". It says that
  `CatalogUtil.buildIcebergCatalog` wraps the catalog in a `CachingCatalog` whenever `cache-enabled`
  is absent, that its default is true, that a cached `Table` pins stale metadata, and that the
  observable failure was a worker pinning an anchor snapshot and then failing forever because that
  snapshot was "no longer in history". Then it states the price of the fix: one metadata read per
  `loadTable`.
- `vectorsync-format/src/main/java/io/vectorsync/format/derive/MaterializationSpec.java`, the
  `configId()` javadoc. It explains why `sourceTable` and `keyColumns` are excluded from the hash and
  names the number that proved it: a 2-table corpus holding 5 distinct texts cost 10 inference calls
  instead of 5 when they were included.

`ProjectionBuilder`'s `build`/`commitReplacement` comments are the same register applied to a commit
protocol — including why publishing the newest resolved sequence number rather than the newest
*fully covered* one silently strands a row.

Corollaries, and they are enforced in review:

- A comment that restates the code is noise. `// increment the counter` above `counter++` should be
  deleted, not improved.
- A claim in a comment must be true or removed. Comments here are load-bearing: they are the only
  record of why the obvious alternative is wrong, so an inaccurate one actively misleads. If you
  change behaviour, change the comment that justified the old behaviour.
- No marketing. "blazingly fast", "robust", "enterprise-grade" do not appear in this codebase and
  should not start.
- If you fix something whose failure mode is non-obvious, write down what the failure looked like.
  "Fixed ordering" is worthless; "sorted by snapshot id, which is a random long, so history came
  back shuffled" is the fix.

---

## Rules that protect on-disk state

**Field IDs in Iceberg schemas are an on-disk contract.** Never renumber an existing field.
Parquet files already written carry those ids, and readers resolve columns by id, not by name.
Renumbering silently reassigns data to the wrong column. Add new fields with new, higher ids.

**Iceberg 1.5.0 is the target.** Do not use an API added after it, even if your IDE offers it and
even if a newer Iceberg is in your local repository. The sources and jars under
`~/.m2/repository/org/apache/iceberg` are the reference — check there before using anything you are
unsure about. The format layer is meant to be readable by engines pinned to older Iceberg.

**`config_id` deliberately excludes `sourceTable` and `keyColumns`.** Tier 1 holds exactly one row
per `(content, model, config)` and dedup crosses tables because of that exclusion. Adding a field to
the hash changes the identity of every already-derived vector.

**Refuse loudly rather than guess.** When the system cannot establish that an action is safe it
fails with an explanation, and that is deliberate — see `ProjectionBuilder`'s "Refusing to empty the
serving projection" and `MaterializationRunner`'s `markDegraded` calls. Silently doing something
plausible is how a corrupted derived dataset gets published.

---

## Tests

JUnit 5. Two conventions:

- `@DisplayName` is a sentence stating the property under test, not a restatement of the method
  name. "a hash is recorded only by the transaction that completed the work that wrote it" beats
  "testRecordHash".
- The class javadoc says what would be broken if the test did not exist. Read
  `worker/src/test/java/io/vectorsync/worker/service/derive/ReproducibilityTest.java` and
  `control-plane/src/test/java/io/vectorsync/controlplane/service/DurableDedupTest.java` before
  adding a test; both explain the failure they were written against and why the cheaper test setup
  would not have caught it.

Prefer asserting committed state over in-memory results where the committed state is what a reader
consumes — `ReproducibilityTest` compares the Iceberg embedding store and content map, not the
values returned by the call.

---

## Benchmarks

The harness in `bench/` drives the running stack over HTTP (`worker` on `:8081`, `control-plane` on
`:8080`), so bring the stack up first:

```bash
docker compose up -d --build

# dedup sweep: generate a corpus with a stated duplication rate, then derive it
python3 bench/corpus.py --tables 100 --rows 100 --out /tmp/vsbench
python3 bench/run.py --corpus /tmp/vsbench --tables 100 --version run1

# incremental passes: backfill, novel append, duplicate append, no-op
python3 bench/incremental.py run1

# cluster-pruned search, synthetic then real BEIR corpora with qrels
python3 bench/cluster.py --clusters 32 --top-k 10
python3 bench/real.py --dataset nfcorpus --clusters 32
```

`--version` scopes a run to its own `(model_version, config_id)`, so a rerun starts from an empty
store instead of inheriting the previous run's cache and reporting a win it did not earn. Ground
truth for the dedup numbers is computed independently in `bench/corpus.py`, which is why agreement
with the service's own counters counts as evidence.

**A performance claim in a pull request comes with the command that produced it**, the hardware it
ran on, and the control case. Inference-call counts are exact and reproducible; wall-clock numbers on
a laptop are not — a sustained run here drove the embedding service from 62ms to 4,900ms per 64-text
batch through thermal pressure. Report both, and say which one the claim rests on. A negative result
is publishable: `bench/corpus.py`'s `dup00` control exists precisely so the design's zero-win case is
reported alongside its best one.

---

## Pull requests

- One concern per PR. A format change and a service change in the same commit cannot be reviewed.
- Run `mvn -o clean test-compile` then `mvn -o clean test` before pushing. CI runs the same gate.
- Update the docs your change makes false. `docs/REPOSITORY_STRUCTURE.md`, `docs/architecture.md`,
  `docs/LIFECYCLE.md` and the README's "Status and known gaps" section are part of the change, not
  follow-up work.
- Security-relevant reports go through the process in `SECURITY.md`, not a public PR.
