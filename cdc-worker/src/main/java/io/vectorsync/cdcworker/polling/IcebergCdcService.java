package io.vectorsync.cdcworker.polling;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.cdcworker.snapshot.CdcResult;
import io.vectorsync.cdcworker.scanner.IcebergTableService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service responsible for detecting changes in Iceberg tables.
 * Performs incremental snapshot scanning to identify new/modified records.
 * 
 * TODO: Add support for DELETE operations (currently only handles APPEND)
 * TODO: Consider Kafka for publishing CDC events instead of in-memory
 */
@Service
@Slf4j
public class IcebergCdcService {

    private final IcebergTableService icebergTableService;

    public IcebergCdcService(IcebergTableService icebergTableService) {
        this.icebergTableService = icebergTableService;
    }

    public CdcResult detectChanges(TableConfig tableConfig, Long lastSnapshotId) {
        Table table;
        try {
            table = icebergTableService.loadTable(tableConfig);
        } catch (Exception e) {
            log.warn("Source table {} not available: {}", tableConfig.getTableName(), e.getMessage());
            return CdcResult.builder()
                    .changeEvents(List.of())
                    .currentSnapshotId(null)
                    .previousSnapshotId(lastSnapshotId)
                    .build();
        }
        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            log.warn("Table {} has no current snapshot", tableConfig.getTableName());
            return CdcResult.builder()
                    .changeEvents(List.of())
                    .currentSnapshotId(null)
                    .previousSnapshotId(lastSnapshotId)
                    .build();
        }

        Long currentSnapshotId = currentSnapshot.snapshotId();
        log.info("CDC check for table {}: lastSnapshotId={}, currentSnapshotId={}",
                 tableConfig.getTableName(), lastSnapshotId, currentSnapshotId);
        
        List<ChangeEvent> events = new ArrayList<>();

        try {
            if (lastSnapshotId == null) {
                log.info("Performing full table scan for {} (lastSnapshotId is null)", tableConfig.getTableName());
                events.addAll(readFullTable(table, tableConfig, currentSnapshotId));
            } else if (!lastSnapshotId.equals(currentSnapshotId)) {
                log.info("Performing incremental scan for {} (snapshots differ)", tableConfig.getTableName());
                events.addAll(readIncremental(table, tableConfig, lastSnapshotId, currentSnapshotId));
            } else {
                log.info("No changes for {} (snapshots match: {})", tableConfig.getTableName(), currentSnapshotId);
            }
        } catch (Exception e) {
            log.error("Failed to read changes for {}: {}", tableConfig.getTableName(), e.getMessage(), e);
        }

        log.info("CDC result for {}: {} change events detected", tableConfig.getTableName(), events.size());
        
        return CdcResult.builder()
                .changeEvents(events)
                .currentSnapshotId(currentSnapshotId)
                .previousSnapshotId(lastSnapshotId)
                .build();
    }

    private List<ChangeEvent> readFullTable(Table table, TableConfig tableConfig, long snapshotId) {
        log.info("Performing initial full scan for table {}", tableConfig.getTableName());

        List<ChangeEvent> events = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                .useSnapshot(snapshotId)
                .where(Expressions.alwaysTrue())
                .build()) {
            for (Record record : records) {
                events.add(toChangeEvent(tableConfig, snapshotId, null, record, table.schema()));
            }
        } catch (Exception e) {
            log.error("Full scan failed for {}: {}", tableConfig.getTableName(), e.getMessage(), e);
        }
        return events;
    }

    private List<ChangeEvent> readIncremental(Table table, TableConfig tableConfig, long fromSnapshot, long toSnapshot) {
        log.info("Performing incremental scan for table {} from {} to {}",
                tableConfig.getTableName(), fromSnapshot, toSnapshot);

        List<ChangeEvent> events = new ArrayList<>();
        IncrementalAppendScan scan = table.newIncrementalAppendScan()
            .fromSnapshotExclusive(fromSnapshot)
            .toSnapshot(toSnapshot);

        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            if (!tasks.iterator().hasNext()) {
                return events;
            }
        } catch (Exception e) {
            log.warn("Failed to plan incremental tasks for {}: {}", tableConfig.getTableName(), e.getMessage());
        }

        try (CloseableIterable<Record> records = IcebergGenerics.read(table)
            .appendsBetween(fromSnapshot, toSnapshot)
                .build()) {
            for (Record record : records) {
                events.add(toChangeEvent(tableConfig, toSnapshot, fromSnapshot, record, table.schema()));
            }
        } catch (Exception e) {
            log.error("Incremental scan failed for {}: {}", tableConfig.getTableName(), e.getMessage(), e);
        }

        return events;
    }

    private ChangeEvent toChangeEvent(TableConfig tableConfig,
                                      long snapshotId,
                                      Long previousSnapshotId,
                                      Record record,
                                      org.apache.iceberg.Schema schema) {
        Map<String, Object> rowData = new HashMap<>();
        for (int i = 0; i < record.size(); i++) {
            String fieldName = schema.columns().get(i).name();
            rowData.put(fieldName, record.get(i));
        }

        return ChangeEvent.builder()
                .tableId(tableConfig.getTableId())
                .snapshotId(snapshotId)
                .previousSnapshotId(previousSnapshotId == null ? -1L : previousSnapshotId)
                .rowData(rowData)
                .detectedAt(Instant.now())
                .build();
    }
}

// Made with Bob
