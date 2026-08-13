# CDC Worker

The CDC (Change Data Capture) worker is responsible for detecting changes in Iceberg tables by polling snapshots and publishing change events.

## Responsibilities

- Poll assigned Iceberg tables for snapshot changes
- Detect new/modified records using incremental snapshot scanning
- Publish CDC events for downstream processing
- Update sync progress and state
- Handle recovery from failures

## Architecture

```
cdc-worker/
├── polling/          # Iceberg CDC polling logic
├── snapshot/         # Snapshot comparison and diff
├── scanner/          # Iceberg table scanning
├── publisher/        # Event publishing (TODO: Kafka)
├── recovery/         # Failure recovery logic
├── worker/           # Worker coordination
└── config/           # Configuration
```

## Key Components

### Polling
- `IcebergCdcService`: Detects changes between snapshots
- Supports full table scans and incremental scans
- Handles snapshot metadata and change tracking

### Scanner
- `IcebergTableService`: Loads and manages Iceberg tables
- `IcebergCatalogService`: Manages Iceberg catalog connections
- Supports S3/MinIO storage backends

### Publisher (TODO)
- Currently publishes events in-memory
- **Future**: Kafka producer for event-driven architecture
- Will enable horizontal scaling and better fault tolerance

## Configuration

```yaml
server:
  port: 8081

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

worker:
  id: cdc-worker-1
  poll-interval: 30000  # Poll every 30 seconds
```

## Running

```bash
# Build
mvn clean package

# Run
java -jar target/vectorsync-cdc-worker-0.1.0-SNAPSHOT.jar

# Or with Maven
mvn spring-boot:run
```

## Environment Variables

```bash
ICEBERG_CATALOG_WAREHOUSE=s3a://warehouse/iceberg
AWS_S3_ENDPOINT=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
WORKER_ID=cdc-worker-1
WORKER_POLL_INTERVAL=30000
```

## How It Works

1. **Registration**: Worker registers with control-plane on startup
2. **Assignment**: Control-plane assigns tables to worker
3. **Polling**: Worker polls assigned tables for snapshot changes
4. **Detection**: Compares current snapshot with last processed snapshot
5. **Scanning**: Reads new/modified records using incremental scan
6. **Publishing**: Publishes CDC events (currently in-memory, TODO: Kafka)
7. **State Update**: Updates sync state in control-plane

## Future Enhancements

### Kafka Integration (TODO)
Replace in-memory event publishing with Kafka:
```yaml
kafka:
  bootstrap-servers: localhost:9092
  producer:
    topic: cdc-events
    compression: snappy
```

Benefits:
- Horizontal scaling of CDC workers
- Better fault tolerance and replay capability
- Decoupling from embedding workers
- Event ordering guarantees

### Advanced Features (TODO)
- DELETE operation support (currently only APPEND)
- Schema evolution handling
- Partition-aware scanning
- Adaptive polling intervals based on change frequency
- Worker health monitoring and auto-recovery

## Monitoring

Health endpoint: `http://localhost:8081/actuator/health`

Metrics to monitor:
- Tables assigned to worker
- Snapshot polling frequency
- CDC events published
- Scan duration and throughput
- Error rates and retry attempts

## Troubleshooting

### Worker not detecting changes
- Check snapshot IDs in sync_state table
- Verify Iceberg table has new snapshots
- Check worker logs for polling errors

### S3/MinIO connection issues
- Verify AWS credentials and endpoint
- Check network connectivity
- Ensure bucket exists and is accessible

### High memory usage
- Reduce batch size for large tables
- Implement streaming for very large scans
- Monitor partition sizes