# Repository structure

```text
vectorsync-format/   Canonical, engine-neutral format contract. NO framework dependencies.
common/              Pure DTOs, constants, cosine utility
control-plane/       Table + embedding-version registry, sync watermark (PostgreSQL) :8080
worker/              Embedding materializer: snapshot diff -> batch embed -> versioned write :8081
search-service/      Index build, alias-based serving, evaluation, provenance :8083
embedding-service/   Python FastAPI: sentence-transformers or a managed API :8000
dashboard/           React UI :3000
deployment/          Dockerfiles, compose overlays, E2E scripts
scripts/             Host-local dev startup
docs/                Architecture and lifecycle guides
```

## Why `vectorsync-format` exists

The vector table's schema, record codec, and version-resolution rule were previously duplicated
across worker, search-service, and control-plane, and had drifted apart. Under a design whose
claim is "an open, engine-neutral format", that is fatal: the resolution semantics *are* the
product, so two components carrying their own copy means they can disagree about what the format
means.

```
io.vectorsync.format
├── catalog/   IcebergCatalogConfig, IcebergCatalogFactory
├── io/        IcebergAppender          partitioned Parquet append, one commit
├── vector/    VectorTableSchema, VectorRecordCodec, VectorResolution, VectorIds
└── index/     IndexManifestStore, IndexAliasStore, IndexArtifactStore, IndexManifestEntry
```

**Hard constraint: no Spring, no framework.** Its consumers include the Spring services, Spark
batch jobs, and eventually a Trino plugin — none of which can share a framework container. The
Spring services hold only thin `@Value`-binding adapters over it.

## Layering

```
common  <-  vectorsync-format  <-  control-plane / worker / search-service
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
