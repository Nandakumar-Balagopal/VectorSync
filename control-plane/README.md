# Control Plane

The control plane is the central coordination service for VectorSync, managing metadata, table registration, sync state, and worker coordination.

## Responsibilities

- Table registration and configuration APIs
- Metadata management (PostgreSQL)
- Sync state tracking
- Worker registration and heartbeat tracking
- **Scheduler** - assigns tables to workers and manages leases
- Lease management and rebalancing
- Health monitoring

## Architecture

```
control-plane/
├── api/              # REST API controllers
├── scheduler/        # Table assignment and lease management
├── worker/           # Worker registration and coordination
├── lease/            # Lease management logic
├── metadata/         # Metadata models and DTOs
├── repository/       # JPA repositories
├── service/          # Business logic services
└── config/           # Configuration
```

## Key Components

### API
- `TableController`: Table registration and management endpoints
- `SyncStateController`: Sync state queries and updates
- `WorkerController`: Worker registration and health endpoints

### Scheduler
- `TableScheduler`: Assigns tables to CDC workers
- Maintains table leases with TTL
- Detects failed workers and rebalances
- Manages polling intervals per table
- **Lives inside control-plane** (not a separate service)

### Worker Coordination
- Worker registration on startup
- Heartbeat tracking
- Failure detection
- Automatic rebalancing

### Metadata
- Table configurations
- Sync state (last snapshot ID, timestamp)
- Worker registrations
- Lease assignments

## Configuration

```yaml
server:
  port: 8080

spring:
  datasource:
    url: jdbc:postgresql://localhost:5433/vectorsync
    username: postgres
    password: postgres
  jpa:
    hibernate:
      ddl-auto: update

iceberg:
  catalog:
    warehouse: s3a://warehouse/iceberg
  vector:
    namespace: vector

aws:
  s3:
    endpoint: http://localhost:9000
    access-key: minioadmin
    secret-key: minioadmin
    region: us-east-1

scheduler:
  enabled: true
  lease-ttl: 60000        # 60 seconds
  rebalance-interval: 30000  # 30 seconds
```

## Running

```bash
# Build
mvn clean package

# Run
java -jar target/vectorsync-control-plane-0.1.0-SNAPSHOT.jar

# Or with Maven
mvn spring-boot:run
```

## Environment Variables

```bash
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5433/vectorsync
SPRING_DATASOURCE_USERNAME=postgres
SPRING_DATASOURCE_PASSWORD=postgres
ICEBERG_CATALOG_WAREHOUSE=s3a://warehouse/iceberg
AWS_S3_ENDPOINT=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
SCHEDULER_ENABLED=true
SCHEDULER_LEASE_TTL=60000
SCHEDULER_REBALANCE_INTERVAL=30000
```

## API Endpoints

### Table Management

#### Register Table
```http
POST /api/tables
Content-Type: application/json

{
  "catalogName": "iceberg_data",
  "schemaName": "default",
  "tableName": "products",
  "textColumns": ["name", "description"],
  "pollInterval": 30000
}
```

#### List Tables
```http
GET /api/tables
```

#### Get Table Details
```http
GET /api/tables/{id}
```

#### Delete Table
```http
DELETE /api/tables/{id}
```

### Sync State

#### Get Sync State
```http
GET /api/sync/state/{tableId}
```

#### Update Sync State
```http
PUT /api/sync/state/{tableId}
Content-Type: application/json

{
  "lastSnapshotId": 1234567890,
  "lastSyncTimestamp": "2024-01-01T00:00:00Z",
  "status": "SYNCED"
}
```

### Worker Management

#### Register Worker
```http
POST /api/workers/register
Content-Type: application/json

{
  "workerId": "cdc-worker-1",
  "workerType": "CDC",
  "host": "localhost",
  "port": 8081
}
```

#### Worker Heartbeat
```http
POST /api/workers/{workerId}/heartbeat
```

#### List Workers
```http
GET /api/workers
```

### Health Check
```http
GET /actuator/health
```

## Database Schema

### tables
```sql
CREATE TABLE tables (
  id BIGSERIAL PRIMARY KEY,
  catalog_name VARCHAR(255) NOT NULL,
  schema_name VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  text_columns TEXT[] NOT NULL,
  poll_interval INTEGER DEFAULT 30000,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(catalog_name, schema_name, table_name)
);
```

### sync_state
```sql
CREATE TABLE sync_state (
  id BIGSERIAL PRIMARY KEY,
  table_id BIGINT REFERENCES tables(id),
  last_snapshot_id BIGINT,
  last_sync_timestamp TIMESTAMP,
  status VARCHAR(50),
  error_message TEXT,
  updated_at TIMESTAMP DEFAULT NOW()
);
```

### workers
```sql
CREATE TABLE workers (
  id BIGSERIAL PRIMARY KEY,
  worker_id VARCHAR(255) UNIQUE NOT NULL,
  worker_type VARCHAR(50) NOT NULL,
  host VARCHAR(255),
  port INTEGER,
  status VARCHAR(50),
  last_heartbeat TIMESTAMP,
  registered_at TIMESTAMP DEFAULT NOW()
);
```

### table_leases
```sql
CREATE TABLE table_leases (
  id BIGSERIAL PRIMARY KEY,
  table_id BIGINT REFERENCES tables(id),
  worker_id VARCHAR(255) REFERENCES workers(worker_id),
  lease_expires_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(table_id)
);
```

## Scheduler Logic

### Table Assignment
1. Scheduler runs periodically (every 30s by default)
2. Queries all registered tables
3. Checks existing leases
4. Assigns unassigned tables to available workers
5. Balances load across workers

### Lease Management
1. Each table assignment has a lease with TTL (60s default)
2. Workers must renew leases via heartbeat
3. Expired leases are automatically released
4. Released tables are reassigned to other workers

### Worker Failure Detection
1. Workers send heartbeats every 15s
2. If no heartbeat for 60s, worker is marked as failed
3. All tables assigned to failed worker are released
4. Tables are rebalanced to healthy workers

### Rebalancing
1. Triggered when worker fails or new worker joins
2. Calculates optimal distribution
3. Releases leases from overloaded workers
4. Reassigns to underloaded workers
5. Minimizes disruption to running syncs

## Future Enhancements

### Kafka Integration (TODO)
Replace in-memory coordination with Kafka:
```yaml
kafka:
  bootstrap-servers: localhost:9092
  topics:
    worker-events: worker-events
    table-assignments: table-assignments
```

Benefits:
- Event-driven worker coordination
- Better fault tolerance
- Audit trail of all assignments
- Easier debugging and monitoring

### Advanced Scheduling (TODO)
- Priority-based scheduling
- Resource-aware scheduling (CPU, memory)
- Table affinity (prefer same worker for same table)
- Adaptive polling intervals based on change frequency
- Cost-based scheduling (optimize for S3 costs)

### Multi-Region Support (TODO)
- Region-aware worker assignment
- Cross-region replication
- Geo-distributed coordination
- Latency-based routing

### Monitoring & Observability (TODO)
- Prometheus metrics export
- Grafana dashboards
- Alert rules for failures
- Performance analytics
- Cost tracking

## Monitoring

Health endpoint: `http://localhost:8080/actuator/health`

Metrics to monitor:
- Registered tables count
- Active workers count
- Lease assignments
- Failed workers
- Rebalancing frequency
- API request latency
- Database connection pool

## Troubleshooting

### Tables not being assigned
- Check scheduler is enabled
- Verify workers are registered and healthy
- Check worker heartbeats
- Review lease expiration settings

### Frequent rebalancing
- Check worker stability
- Review heartbeat interval
- Verify network connectivity
- Check lease TTL settings

### Database connection issues
- Verify PostgreSQL is running
- Check connection pool settings
- Review database credentials
- Monitor connection count

### High API latency
- Check database query performance
- Review connection pool size
- Monitor JVM heap usage
- Consider caching frequently accessed data