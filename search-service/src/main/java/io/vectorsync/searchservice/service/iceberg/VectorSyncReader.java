package io.vectorsync.searchservice.service.iceberg;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.VectorRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@Slf4j
public class VectorSyncReader {

    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public VectorSyncReader(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public List<VectorRecord> readAllVectors() {
        Table table = loadVectorTable();
        if (table == null) {
            return List.of();
        }
        List<VectorRecord> records = new ArrayList<>();

        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                try {
                    records.add(fromIcebergRecord(row));
                } catch (Exception e) {
                    log.warn("Skipping unreadable vector row: {}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read vectors from Iceberg: {}", e.getMessage(), e);
        }

        return latestLiveVectors(records);
    }

    private List<VectorRecord> latestLiveVectors(List<VectorRecord> records) {
        Map<String, VectorRecord> latestBySourceRow = new LinkedHashMap<>();

        records.stream()
                .filter(record -> {
                    boolean hasKey = sourceKey(record) != null;
                    if (!hasKey) {
                        log.warn("Dropping vector row without source key. vectorId={}, sourceTable={}, sourceRowId={}, modelName={}, metadataKeys={}",
                                record.getVectorId(),
                                record.getSourceTable(),
                                record.getSourceRowId(),
                                record.getModelName(),
                                record.getMetadata() == null ? List.of() : record.getMetadata().keySet());
                    }
                    return hasKey;
                })
                .sorted(Comparator.comparing(
                        VectorRecord::getCreatedAt,
                        Comparator.nullsFirst(Comparator.naturalOrder())
                ))
                .forEach(record -> latestBySourceRow.put(sourceKey(record), record));

        return latestBySourceRow.values().stream()
                .filter(record -> !record.isDeleted())
                .toList();
    }

    private String sourceKey(VectorRecord record) {
        String sourceTable = record.getSourceTable();
        String sourceRowId = record.getSourceRowId();
        if (record.getMetadata() != null) {
            if (sourceTable == null || sourceTable.isBlank()) {
                sourceTable = record.getMetadata().get("source_table");
            }
            sourceRowId = firstNonBlank(sourceRowId, record.getMetadata().get("source_row_id"));
            sourceRowId = firstNonBlank(sourceRowId, record.getMetadata().get("id"));
        }
        if (sourceTable == null || sourceTable.isBlank() || sourceRowId == null || sourceRowId.isBlank()) {
            return null;
        }
        return sourceTable + "::" + sourceRowId + "::" + record.getModelName();
    }

    private Table loadVectorTable() {
        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(vectorNamespace), Constants.VECTOR_TABLE_NAME);
        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Vector table existence check failed: {}", e.getMessage());
            return null;
        }

        if (!tableExists) {
            log.warn("Vector table {}.{} does not exist yet", vectorNamespace, Constants.VECTOR_TABLE_NAME);
            return null;
        }
        return catalog.loadTable(identifier);
    }

    private VectorRecord fromIcebergRecord(Record record) {
        List<Double> embedding = toDoubleList(field(record, "embedding", 3));
        Map<String, String> metadata = toStringMap(field(record, "metadata", 5));

        Object createdAtValue = field(record, "created_at", 7);
        Instant createdAt = null;
        if (createdAtValue instanceof Instant) {
            createdAt = (Instant) createdAtValue;
        } else if (createdAtValue instanceof OffsetDateTime) {
            createdAt = ((OffsetDateTime) createdAtValue).toInstant();
        }

        String sourceTable = asString(field(record, "source_table", 1));
        if (sourceTable == null && metadata != null) {
            sourceTable = metadata.get("source_table");
        }

        return VectorRecord.builder()
            .vectorId(asString(field(record, "vector_id", 0)))
            .sourceTable(sourceTable)
                .sourceRowId(firstNonBlank(
                        firstNonBlank(asString(field(record, "source_row_id", 2)), metadata.get("source_row_id")),
                        metadata.get("id")))
                .embedding(embedding)
                .text(asString(field(record, "text", 4)))
                .metadata(metadata)
                .modelName(asString(field(record, "model_name", 6)))
                .createdAt(createdAt)
                .deleted(metadata != null && Objects.equals("true", metadata.get("deleted")))
                .build();
    }

    private Object field(Record record, String name, int position) {
        try {
            Object value = record.getField(name);
            if (value != null) {
                return value;
            }
        } catch (Exception ignored) {
            // Fall through to positional lookup.
        }
        try {
            return record.get(position);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String firstNonBlank(String primary, String fallback) {
        return primary == null || primary.isBlank() ? fallback : primary;
    }

    private String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private List<Double> toDoubleList(Object value) {
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        return list.stream()
                .filter(Objects::nonNull)
                .map(item -> item instanceof Number number
                        ? number.doubleValue()
                        : Double.parseDouble(item.toString()))
                .toList();
    }

    private Map<String, String> toStringMap(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return new HashMap<>();
        }
        Map<String, String> result = new HashMap<>();
        map.forEach((key, mapValue) -> {
            if (key != null && mapValue != null) {
                result.put(key.toString(), mapValue.toString());
            }
        });
        return result;
    }
}
