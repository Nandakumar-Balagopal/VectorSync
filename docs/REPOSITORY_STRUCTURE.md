# Repository structure

```text
vectorsync-format/   Canonical, engine-neutral format contract. NO framework dependencies.
common/              Pure DTOs, constants, cosine utility
control-plane/       Validated admission with cost estimates, materialization state machine,
                     leased work queue, durable dedup record (PostgreSQL) :8080
worker/              Embedding materializer: snapshot diff -> chunk -> dedup-aware embed ->
                     Tier-1 write :8081
                     (not the serving tier; engines read Tier 2 directly)
embedding-service/   Python FastAPI: sentence-transformers or a managed API :8000
dashboard/           React UI :3000
bench/               Python measurement harness; every performance claim in the README comes from here
deployment/          Dockerfiles, compose overlays, E2E scripts
scripts/             Host-local dev startup (start-local.sh)
docs/                Architecture, lifecycle, demo and review notes
.github/workflows/   CI: one job, JDK 17, `mvn -B clean test`
```

Project-level files:

```text
LICENSE              Apache License 2.0, verbatim
NOTICE               Apache-2.0 s.4(d) attribution. Scoped to the SOURCE tree: it says where
                     binary attribution actually lives (BOOT-INF/lib/ of the repackaged jar)
                     rather than listing a subset of dependencies
README.md            The design argument and the measurements behind it
CONTRIBUTING.md      Build gate, the Docker/Colima setup Testcontainers needs, the comment
                     convention, and the rules that protect on-disk state
SECURITY.md          Disclosure process, and the trusted-network-only posture stated plainly:
                     no HTTP endpoint has authentication today
pom.xml              Reactor root: <modules>, <dependencyManagement>, <build> plugin versions
```

## Why `vectorsync-format` exists

The vector table's schema, record codec, and version-resolution rule were previously duplicated
claim is "an open, engine-neutral format", that is fatal: the resolution semantics *are* the
product, so two components carrying their own copy means they can disagree about what the format
means.

```
io.vectorsync.format
├── catalog/   IcebergCatalogConfig, IcebergCatalogFactory, Namespaces
├── io/        IcebergAppender          partitioned Parquet append, one commit
├── vector/    VectorTableSchema, VectorRecordCodec, VectorResolution, VectorIds
├── derive/    MaterializationSpec (config_id), ContentHash, ContentMap, EmbeddingStore,
│              ProjectionBuilder, SqlViewGenerator, VectorClustering, ClusteredIndex
└── index/     IndexManifestStore, IndexAliasStore, IndexArtifactStore, IndexManifestEntry
```

**Hard constraint: no Spring, no framework.** Its consumers include the Spring services, Spark
batch jobs, and eventually a Trino plugin — none of which can share a framework container. The
Spring services hold only thin `@Value`-binding adapters over it.

This is no longer only a convention. `vectorsync-format/pom.xml` and `common/pom.xml` each add a
`maven-enforcer-plugin` execution (pinned version, bound to `validate`) whose `bannedDependencies`
rule rejects any `org.springframework*` artifact, direct or transitive. It fails the build on the
pom entry, before any import exists, and it runs in every `mvn test-compile` and `mvn test` rather
than only in CI. The root `pom.xml` is untouched by this: it still contributes only its `<modules>`,
`<dependencyManagement>` and `<build>` plugin-version sections, so a module that has no reason to
ban Spring does not inherit the rule.

## Layering

```
```

Nothing above `vectorsync-format` may define format semantics. If you find yourself writing a
schema, an id derivation, or a resolution rule inside a service, it belongs in the format module.

## Not planned

Do not reintroduce a multi-format CDC abstraction (Delta, Hive). It conflicts directly with
treating Iceberg as the system of record, and the previous design document for it has been removed.

Worker autoscaling is also off the roadmap. The materializer's steady-state load is small and its
heavy load is a known-size scheduled backfill, which wants provisioned parallelism rather than a
reactive autoscaler — and the embedding tier, not the worker, is the binding constraint. See the
scaling notes in `docs/architecture.md`.
