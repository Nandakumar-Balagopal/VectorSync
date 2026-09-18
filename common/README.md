# common

Shared Java library for the active VectorSync services.

This module intentionally contains only cross-service types and pure utilities:

- `TableConfig`
- `ChangeEvent`
- `VectorRecord`
- `SearchResult`
- shared constants
- cosine-similarity helper

Business logic belongs in the service modules:

- `control-plane/` owns admission, the materialization state machine and the dedup record.
- `worker/` owns change detection, derivation, the serving projection, clustering, retrieval and
  derived-table maintenance.

New shared DTOs may be added here, but this module stays free of orchestration, Iceberg I/O and
embedding-provider logic.
