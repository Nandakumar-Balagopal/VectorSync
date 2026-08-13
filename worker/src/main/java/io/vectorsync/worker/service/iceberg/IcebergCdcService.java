package io.vectorsync.worker.service.iceberg;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
            log.warn("Incremental CDC failed for {} from snapshot {} to {}: {}. Falling back to full scan.",
                    tableConfig.getTableName(), lastSnapshotId, currentSnapshotId, e.getMessage());
            events.clear();
            events.addAll(readFullTable(table, tableConfig, currentSnapshotId));
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
                Map<String, Object> rowData = toRowData(record, table.schema());
                events.add(toChangeEvent(
                        tableConfig,
                        snapshotId,
                        null,
                        ChangeEvent.OPERATION_INSERT,
                        rowData,
                        null
                ));
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

        Map<String, Map<String, Object>> previousRows = readSnapshotByRowId(table, tableConfig, fromSnapshot);
        Map<String, Map<String, Object>> currentRows = readSnapshotByRowId(table, tableConfig, toSnapshot);

        for (Map.Entry<String, Map<String, Object>> currentEntry : currentRows.entrySet()) {
            String rowId = currentEntry.getKey();
            Map<String, Object> currentRow = currentEntry.getValue();
            Map<String, Object> previousRow = previousRows.get(rowId);

            if (previousRow == null) {
                events.add(toChangeEvent(
                        tableConfig,
                        toSnapshot,
                        fromSnapshot,
                        ChangeEvent.OPERATION_INSERT,
                        currentRow,
                        null
                ));
            } else if (!Objects.equals(previousRow, currentRow)) {
                events.add(toChangeEvent(
                        tableConfig,
                        toSnapshot,
                        fromSnapshot,
                        ChangeEvent.OPERATION_UPDATE,
                        currentRow,
                        previousRow
                ));
            }
        }

        for (Map.Entry<String, Map<String, Object>> previousEntry : previousRows.entrySet()) {
            if (!currentRows.containsKey(previousEntry.getKey())) {
                events.add(toChangeEvent(
                        tableConfig,
                        toSnapshot,
                        fromSnapshot,
                        ChangeEvent.OPERATION_DELETE,
                        previousEntry.getValue(),
                        previousEntry.getValue()
                ));
            }
        }

        return events;
    }

    private Map<String, Map<String, Object>> readSnapshotByRowId(Table table,
                                                                 TableConfig tableConfig,
                                                                 long snapshotId) {
        Map<String, Map<String, Object>> rows = new LinkedHashMap<>();

        try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                .useSnapshot(snapshotId)
                .where(Expressions.alwaysTrue())
                .build()) {
            for (Record record : records) {
                Map<String, Object> rowData = toRowData(record, table.schema());
                rows.put(extractRowId(rowData), rowData);
            }
        } catch (Exception e) {
            log.error("Snapshot scan failed for {} at snapshot {}: {}",
                    tableConfig.getTableName(), snapshotId, e.getMessage(), e);
            throw new IllegalStateException("Failed to read source snapshot " + snapshotId, e);
        }

        return rows;
    }

    private ChangeEvent toChangeEvent(TableConfig tableConfig,
                                      long snapshotId,
                                      Long previousSnapshotId,
                                      String operation,
                                      Map<String, Object> rowData,
                                      Map<String, Object> previousRowData) {
        return ChangeEvent.builder()
                .tableId(tableConfig.getTableId())
                .snapshotId(snapshotId)
                .previousSnapshotId(previousSnapshotId == null ? -1L : previousSnapshotId)
                .operation(operation)
                .rowData(rowData)
                .previousRowData(previousRowData)
                .detectedAt(Instant.now())
                .build();
    }

    private Map<String, Object> toRowData(Record record, org.apache.iceberg.Schema schema) {
        Map<String, Object> rowData = new HashMap<>();
        for (int i = 0; i < record.size(); i++) {
            String fieldName = schema.columns().get(i).name();
            rowData.put(fieldName, record.get(i));
        }

        return rowData;
    }

    private String extractRowId(Map<String, Object> rowData) {
        Object rowId = rowData.get("id");
        if (rowId == null) {
            rowId = rowData.get("ID");
        }
        if (rowId == null) {
            throw new IllegalArgumentException("Source rows must contain an id column for CDC diffing");
        }
        return rowId.toString();
    }
}
