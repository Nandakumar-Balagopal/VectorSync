# Repository structure

VectorSync is organized around the Phase 1 runtime path and leaves room for
future distributed phases without keeping unused experimental modules in the
active build.

## Active services

```text
common/             Shared Java DTOs, constants, and utilities
control-plane/      Table configuration and sync-state API
worker/             Phase 1 CDC + embedding + vector write service
search-service/     Semantic query API and vector retrieval
embedding-service/  Optional Python embedding API for local/self-hosted models
dashboard/          React UI
deployment/         Dockerfiles and helper scripts for the active stack
docs/               Architecture and planning docs
```

## Future phases

Do not reintroduce separate CDC or embedding worker modules until the coordinator
task model exists. Phase 2 should add distributed pieces intentionally, for
example:

```text
coordinator/        Planned: sync jobs, file-level task planning, leases
worker/             Evolves into a stateless task executor
```

The durable contract should be task-based rather than module-name-based:

- table snapshot detected
- coordinator plans Iceberg file tasks
- workers claim tasks with leases
- workers generate embeddings and write idempotent vector/tombstone records
- coordinator advances table freshness only after task completion

This keeps the codebase small in Phase 1 while preserving a clean path to
autoscaled distributed ingestion.
