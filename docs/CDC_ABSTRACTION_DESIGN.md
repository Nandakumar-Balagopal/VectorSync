# CDC Abstraction Layer Design

## Overview

VectorSync needs to support multiple table formats (Iceberg, Delta Lake, Hive) with different Change Data Capture (CDC) capabilities. This document defines a unified abstraction layer that allows format-agnostic CDC processing.

## Problem Statement

Different table formats have varying CDC capabilities:
- **Iceberg**: Native snapshot diff with efficient incremental reads
- **Delta Lake**: Transaction log with versioned commits
- **Hive**: No native CDC support, requires alternative strategies

We need a unified interface that abstracts these differences while maintaining efficiency.

## Architecture

### Core Interface

```java
package io.vectorsync.common.cdc;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import java.util.List;

/**
 * Abstraction for Change Data Capture across different table formats.
 * Implementations provide format-specific CDC strategies.
 */
public interface CDCProvider {
    
    /**
     * Get changes since the last checkpoint.
     * 
     * @param config Table configuration
     * @param lastCheckpoint Last processed checkpoint (format-specific)
     * @return List of change events
     */
    List<ChangeEvent> getChangesSince(TableConfig config, CDCCheckpoint lastCheckpoint);
    
    /**
     * Get current checkpoint for the table.
     * 
     * @param config Table configuration
     * @return Current checkpoint
     */
    CDCCheckpoint getCurrentCheckpoint(TableConfig config);
    
    /**
     * Check if this provider supports incremental reads.
     * 
     * @return true if incremental CDC is supported
     */
    boolean supportsIncrementalRead();
    
    /**
     * Get the table format this provider handles.
     * 
     * @return Table format (ICEBERG, DELTA, HIVE)
     */
    TableFormat getTableFormat();
    
    /**
     * Validate if the table is compatible with this CDC provider.
     * 
     * @param config Table configuration
     * @return Validation result with error messages if any
     */
    CDCValidationResult validate(TableConfig config);
}
```

### Checkpoint Abstraction

```java
package io.vectorsync.common.cdc;

/**
 * Format-agnostic checkpoint representation.
 * Each format stores its specific checkpoint data.
 */
public class CDCCheckpoint {
    private final TableFormat format;
    private final String checkpointValue;  // Format-specific: snapshotId, version, timestamp
    private final long timestamp;
    
    // Iceberg: snapshotId
    public static CDCCheckpoint forIceberg(long snapshotId) {
        return new CDCCheckpoint(TableFormat.ICEBERG, String.valueOf(snapshotId), System.currentTimeMillis());
    }
    
    // Delta Lake: version number
    public static CDCCheckpoint forDelta(long version) {
        return new CDCCheckpoint(TableFormat.DELTA, String.valueOf(version), System.currentTimeMillis());
    }
    
    // Hive: timestamp
    public static CDCCheckpoint forHive(long timestamp) {
        return new CDCCheckpoint(TableFormat.HIVE, String.valueOf(timestamp), timestamp);
    }
    
    // Getters and serialization methods
}
```

### Provider Factory

```java
package io.vectorsync.common.cdc;

import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.HashMap;

@Component
public class CDCProviderFactory {
    
    private final Map<TableFormat, CDCProvider> providers = new HashMap<>();
    
    public CDCProviderFactory(
            IcebergCDCProvider icebergProvider,
            DeltaCDCProvider deltaProvider,
            HiveCDCProvider hiveProvider) {
        providers.put(TableFormat.ICEBERG, icebergProvider);
        providers.put(TableFormat.DELTA, deltaProvider);
        providers.put(TableFormat.HIVE, hiveProvider);
    }
    
    public CDCProvider getProvider(TableFormat format) {
        CDCProvider provider = providers.get(format);
        if (provider == null) {
            throw new UnsupportedTableFormatException("No CDC provider for format: " + format);
        }
        return provider;
    }
    
    public CDCProvider getProvider(TableConfig config) {
        return getProvider(config.getTableFormat());
    }
}
```

## Implementation Strategies

### 1. Iceberg CDC Provider (✅ Current Implementation)

```java
package io.vectorsync.worker.service.cdc;

import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.data.Record;

@Component
public class IcebergCDCProvider implements CDCProvider {
    
    @Override
    public List<ChangeEvent> getChangesSince(TableConfig config, CDCCheckpoint lastCheckpoint) {
        Table table = loadIcebergTable(config);
        long lastSnapshotId = Long.parseLong(lastCheckpoint.getCheckpointValue());
        long currentSnapshotId = table.currentSnapshot().snapshotId();
        
        // Use Iceberg's incremental read API
        CloseableIterable<Record> changes = table.newScan()
            .appendsBetween(lastSnapshotId, currentSnapshotId)
            .planFiles();
            
        return convertToChangeEvents(changes, config);
    }
    
    @Override
    public CDCCheckpoint getCurrentCheckpoint(TableConfig config) {
        Table table = loadIcebergTable(config);
        long snapshotId = table.currentSnapshot().snapshotId();
        return CDCCheckpoint.forIceberg(snapshotId);
    }
    
    @Override
    public boolean supportsIncrementalRead() {
        return true;  // Iceberg has native incremental read
    }
    
    @Override
    public TableFormat getTableFormat() {
        return TableFormat.ICEBERG;
    }
    
    @Override
    public CDCValidationResult validate(TableConfig config) {
        // Validate Iceberg table exists and is accessible
        return CDCValidationResult.valid();
    }
}
```

### 2. Delta Lake CDC Provider (🎯 To Implement)

```java
package io.vectorsync.worker.service.cdc;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

@Component
public class DeltaCDCProvider implements CDCProvider {
    
    @Override
    public List<ChangeEvent> getChangesSince(TableConfig config, CDCCheckpoint lastCheckpoint) {
        DeltaLog deltaLog = DeltaLog.forTable(config.getTablePath());
        long lastVersion = Long.parseLong(lastCheckpoint.getCheckpointValue());
        long currentVersion = deltaLog.snapshot().getVersion();
        
        List<ChangeEvent> events = new ArrayList<>();
        
        // Read transaction log between versions
        for (long version = lastVersion + 1; version <= currentVersion; version++) {
            VersionLog versionLog = deltaLog.getChanges(version, true).next();
            
            for (Action action : versionLog.getActions()) {
                if (action instanceof AddFile) {
                    // File added = rows inserted
                    events.addAll(readAddedRows((AddFile) action, config));
                } else if (action instanceof RemoveFile) {
                    // File removed = rows deleted
                    events.addAll(createDeleteEvents((RemoveFile) action, config));
                }
            }
        }
        
        return events;
    }
    
    @Override
    public CDCCheckpoint getCurrentCheckpoint(TableConfig config) {
        DeltaLog deltaLog = DeltaLog.forTable(config.getTablePath());
        long version = deltaLog.snapshot().getVersion();
        return CDCCheckpoint.forDelta(version);
    }
    
    @Override
    public boolean supportsIncrementalRead() {
        return true;  // Delta Lake has transaction log
    }
    
    @Override
    public TableFormat getTableFormat() {
        return TableFormat.DELTA;
    }
    
    @Override
    public CDCValidationResult validate(TableConfig config) {
        // Validate Delta table exists and _delta_log is accessible
        return CDCValidationResult.valid();
    }
    
    private List<ChangeEvent> readAddedRows(AddFile addFile, TableConfig config) {
        // Read parquet file and create INSERT events
        // Implementation details...
    }
}
```

### 3. Hive CDC Provider (⚠️ Limited Support)

```java
package io.vectorsync.worker.service.cdc;

@Component
public class HiveCDCProvider implements CDCProvider {
    
    private final HiveCDCStrategy strategy;
    
    public HiveCDCProvider() {
        // Default to timestamp-based strategy
        this.strategy = new TimestampBasedCDCStrategy();
    }
    
    @Override
    public List<ChangeEvent> getChangesSince(TableConfig config, CDCCheckpoint lastCheckpoint) {
        return strategy.getChanges(config, lastCheckpoint);
    }
    
    @Override
    public CDCCheckpoint getCurrentCheckpoint(TableConfig config) {
        return CDCCheckpoint.forHive(System.currentTimeMillis());
    }
    
    @Override
    public boolean supportsIncrementalRead() {
        return strategy.supportsIncremental();
    }
    
    @Override
    public TableFormat getTableFormat() {
        return TableFormat.HIVE;
    }
    
    @Override
    public CDCValidationResult validate(TableConfig config) {
        // Check if table has required timestamp column
        if (!hasTimestampColumn(config)) {
            return CDCValidationResult.invalid(
                "Hive table requires 'updated_at' or 'modified_at' timestamp column for CDC"
            );
        }
        return CDCValidationResult.valid();
    }
}

// Strategy pattern for different Hive CDC approaches
interface HiveCDCStrategy {
    List<ChangeEvent> getChanges(TableConfig config, CDCCheckpoint checkpoint);
    boolean supportsIncremental();
}

class TimestampBasedCDCStrategy implements HiveCDCStrategy {
    @Override
    public List<ChangeEvent> getChanges(TableConfig config, CDCCheckpoint checkpoint) {
        long lastTimestamp = Long.parseLong(checkpoint.getCheckpointValue());
        
        // Query: SELECT * FROM table WHERE updated_at > lastTimestamp
        String sql = String.format(
            "SELECT * FROM %s.%s WHERE %s > %d",
            config.getCatalog(),
            config.getTableName(),
            config.getTimestampColumn(),
            lastTimestamp
        );
        
        // Execute query and convert to ChangeEvents
        // Note: Can only detect inserts/updates, not deletes
        return executeAndConvert(sql, config);
    }
    
    @Override
    public boolean supportsIncremental() {
        return true;  // If timestamp column exists
    }
}
```

## Integration with Existing Code

### Update CDCService

```java
package io.vectorsync.worker.service;

@Service
public class CDCService {
    
    private final CDCProviderFactory providerFactory;
    private final SyncStateRepository syncStateRepository;
    
    public List<ChangeEvent> detectChanges(TableConfig config) {
        // Get format-specific provider
        CDCProvider provider = providerFactory.getProvider(config);
        
        // Validate table compatibility
        CDCValidationResult validation = provider.validate(config);
        if (!validation.isValid()) {
            throw new CDCException("Table validation failed: " + validation.getErrors());
        }
        
        // Get last checkpoint from database
        SyncStateEntity syncState = syncStateRepository.findByTableId(config.getTableId())
            .orElse(new SyncStateEntity());
        
        CDCCheckpoint lastCheckpoint = syncState.getLastCheckpoint() != null
            ? CDCCheckpoint.fromString(syncState.getLastCheckpoint())
            : provider.getCurrentCheckpoint(config);  // First sync
        
        // Get changes using provider
        List<ChangeEvent> changes = provider.getChangesSince(config, lastCheckpoint);
        
        // Update checkpoint
        CDCCheckpoint newCheckpoint = provider.getCurrentCheckpoint(config);
        syncState.setLastCheckpoint(newCheckpoint.toString());
        syncStateRepository.save(syncState);
        
        return changes;
    }
}
```

### Update TableConfig DTO

```java
package io.vectorsync.common.dto;

@Data
@Builder
public class TableConfig {
    private String tableId;
    private String catalog;
    private String tableName;
    private TableFormat tableFormat;  // NEW: ICEBERG, DELTA, HIVE
    private String timestampColumn;   // NEW: For Hive CDC (optional)
    
    // Existing fields...
    private List<String> embeddingColumns;
    private String vectorColumn;
    private String textColumn;
    private String modelName;
    private boolean enabled;
}

public enum TableFormat {
    ICEBERG,
    DELTA,
    HIVE
}
```

## Migration Path

### Phase 1: Refactor Existing Iceberg Code (Week 1)
- [ ] Create CDC abstraction interfaces
- [ ] Extract Iceberg logic into `IcebergCDCProvider`
- [ ] Update `CDCService` to use provider pattern
- [ ] Add `tableFormat` field to `TableConfig`
- [ ] Update database schema for checkpoint storage
- [ ] Write unit tests for abstraction layer

### Phase 2: Implement Delta Lake Support (Week 2)
- [ ] Add Delta Lake dependencies to `pom.xml`
- [ ] Implement `DeltaCDCProvider`
- [ ] Parse Delta transaction log
- [ ] Map Delta actions to `ChangeEvent`
- [ ] Write integration tests with Delta tables
- [ ] Update documentation

### Phase 3: Implement Hive Support (Week 3)
- [ ] Implement `HiveCDCProvider` with timestamp strategy
- [ ] Add table validation for timestamp column
- [ ] Document limitations (no delete detection)
- [ ] Provide configuration examples
- [ ] Consider external CDC tool integration guide

### Phase 4: Testing & Documentation (Week 4)
- [ ] End-to-end tests with all three formats
- [ ] Performance benchmarks
- [ ] Update API documentation
- [ ] Create migration guide for existing deployments
- [ ] Add format selection to dashboard

## Configuration Examples

### Iceberg Table
```yaml
tables:
  - tableId: "products-iceberg"
    catalog: "iceberg_data"
    tableName: "products"
    tableFormat: "ICEBERG"
    embeddingColumns: ["description"]
    vectorColumn: "description_vector"
```

### Delta Lake Table
```yaml
tables:
  - tableId: "products-delta"
    catalog: "delta_data"
    tableName: "products"
    tableFormat: "DELTA"
    embeddingColumns: ["description"]
    vectorColumn: "description_vector"
```

### Hive Table (with timestamp)
```yaml
tables:
  - tableId: "products-hive"
    catalog: "hive_data"
    tableName: "products"
    tableFormat: "HIVE"
    timestampColumn: "updated_at"  # Required for CDC
    embeddingColumns: ["description"]
    vectorColumn: "description_vector"
```

## Performance Considerations

| Format | CDC Method | Efficiency | Scalability | Limitations |
|--------|-----------|-----------|-------------|-------------|
| **Iceberg** | Snapshot diff | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐⭐⭐ Excellent | None |
| **Delta Lake** | Transaction log | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐⭐⭐ Excellent | None |
| **Hive (timestamp)** | SQL query | ⭐⭐⭐ Good | ⭐⭐⭐ Good | No delete detection, requires timestamp column |
| **Hive (full scan)** | Checksum comparison | ⭐ Poor | ⭐ Poor | Very expensive for large tables |

## Testing Strategy

### Unit Tests
- Test each provider independently
- Mock table access
- Verify checkpoint handling
- Test error scenarios

### Integration Tests
- Test with real Iceberg/Delta/Hive tables
- Verify change detection accuracy
- Test checkpoint persistence
- Measure performance

### End-to-End Tests
- Full pipeline: CDC → Embedding → Index
- Multi-format scenarios
- Failure recovery
- Concurrent table processing

## Open Questions

1. **Hive Delete Detection**: Should we support full table scan for delete detection, or document as limitation?
2. **External CDC Tools**: Should we provide integration guides for Debezium/Maxwell?
3. **Format Auto-Detection**: Should we auto-detect table format, or require explicit configuration?
4. **Checkpoint Migration**: How do we migrate existing Iceberg-only checkpoints to new format?

## References

- [Apache Iceberg Incremental Read](https://iceberg.apache.org/docs/latest/spark-queries/#incremental-read)
- [Delta Lake Transaction Log](https://docs.delta.io/latest/delta-batch.html#read-older-versions-of-data-using-time-travel)
- [Debezium CDC](https://debezium.io/)

---

**Document Status**: Draft  
**Last Updated**: 2026-04-27  
**Author**: VectorSync Team  
**Reviewers**: TBD