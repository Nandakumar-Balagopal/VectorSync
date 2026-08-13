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

- `control-plane/` owns metadata and sync state.
- `worker/` owns CDC detection, embedding generation, and vector writes.
- `search-service/` owns query embedding and retrieval.

Future coordinator/worker phases can add new shared DTOs here, but this module
should stay free of orchestration, Iceberg I/O, embedding-provider, and search
indexing logic.
