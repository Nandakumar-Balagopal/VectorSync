# control-plane

Metadata and sync-state API for VectorSync.

In Phase 1 this service is deliberately small. It does not execute CDC work
itself; the `worker/` service polls the control plane for enabled table configs
and reports sync progress back here.

## Responsibilities

- Register/list/delete source table configurations.
- Store embedding-column and model configuration.
- Store the last processed Iceberg snapshot per table.
- Expose health and operational APIs for the local stack.

## Current endpoints

- `GET /api/tables`
- `POST /api/tables`
- `DELETE /api/tables/{tableId}`
- `GET /api/sync-state/{tableId}`
- `POST /api/sync-state/{tableId}`
- `GET /actuator/health`

## Future role

In Phase 2, this module is the natural home for the distributed coordinator:

- worker registration and heartbeats
- Iceberg snapshot/file task planning
- task leasing and retry
- backpressure and capacity-aware scheduling
- sync job history and freshness SLA tracking

Those APIs are planned, not part of the Phase 1 runtime yet.
