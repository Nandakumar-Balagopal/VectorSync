package io.vectorsync.worker.service;

import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.worker.service.iceberg.IcebergTableService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.parquet.Parquet;
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
public class VectorStoreService {

    private final IcebergTableService icebergTableService;

    public VectorStoreService(IcebergTableService icebergTableService) {
        this.icebergTableService = icebergTableService;
    }

    public void writeVectors(List<VectorRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        log.info("Writing {} vector records to store", records.size());
        Table table;
        try {
            table = icebergTableService.loadOrCreateVectorTable();
        } catch (Exception e) {
            log.warn("Vector table unavailable, skipping write: {}", e.getMessage());
            return;
        }
        Schema schema = table.schema();
        PartitionSpec spec = table.spec();
        OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .format(FileFormat.PARQUET)
                .build();

        Map<String, PartitionBatch> batches = new HashMap<>();
        for (VectorRecord record : records) {
            Record icebergRecord = toIcebergRecord(schema, record);
            PartitionKey partitionKey = new PartitionKey(spec, schema);
            partitionKey.partition(icebergRecord);
            String partitionPath = partitionKey.toPath();

            batches.computeIfAbsent(partitionPath, key -> new PartitionBatch(partitionKey))
                    .records.add(icebergRecord);
        }

        AppendFiles append = table.newAppend();
        for (PartitionBatch batch : batches.values()) {
            EncryptedOutputFile encryptedOutputFile = outputFileFactory.newOutputFile(batch.partitionKey);
            OutputFile outputFile = encryptedOutputFile.encryptingOutputFile();
            long recordCount = batch.records.size();
            long fileSize;
            org.apache.iceberg.Metrics metrics;

            try {
                FileAppender<Record> appender = Parquet.write(outputFile)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build();

                try (appender) {
                    for (Record icebergRecord : batch.records) {
                        appender.add(icebergRecord);
                    }
                }

                fileSize = appender.length();
                metrics = appender.metrics();
            } catch (Exception e) {
                log.error("Failed to write vector batch: {}", e.getMessage(), e);
                continue;
            }

            DataFile dataFile = DataFiles.builder(spec)
                    .withEncryptedOutputFile(encryptedOutputFile)
                    .withFileSizeInBytes(fileSize)
                    .withRecordCount(recordCount)
                    .withMetrics(metrics)
                    .withFormat(FileFormat.PARQUET)
                    .build();

            append.appendFile(dataFile);
        }

        append.commit();
    }

    public List<VectorRecord> getAllVectors() {
        Table table;
        try {
            table = icebergTableService.loadOrCreateVectorTable();
        } catch (Exception e) {
            log.warn("Vector table unavailable: {}", e.getMessage());
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

    public List<VectorRecord> getVectorsByTable(String tableName) {
        return getAllVectors().stream()
                .filter(v -> v.getSourceTable().equals(tableName))
                .toList();
    }

    public long getVectorCount() {
        return getAllVectors().size();
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

    private Record toIcebergRecord(Schema schema, VectorRecord record) {
        Record icebergRecord = GenericRecord.create(schema);
        icebergRecord.setField("vector_id", record.getVectorId());
        String sourceTable = record.getSourceTable();
        if (sourceTable == null && record.getMetadata() != null) {
            sourceTable = record.getMetadata().get("source_table");
        }
        icebergRecord.setField("source_table", sourceTable);
        icebergRecord.setField("source_row_id", record.getSourceRowId());
        icebergRecord.setField("embedding", record.getEmbedding());
        icebergRecord.setField("text", record.getText());
        Map<String, String> metadata = record.getMetadata() == null ? new HashMap<>() : new HashMap<>(record.getMetadata());
        metadata.put("deleted", String.valueOf(record.isDeleted()));
        icebergRecord.setField("metadata", metadata);
        icebergRecord.setField("model_name", record.getModelName());
        Instant createdAt = record.getCreatedAt();
        if (createdAt != null) {
            icebergRecord.setField("created_at", OffsetDateTime.ofInstant(createdAt, java.time.ZoneOffset.UTC));
        }
        return icebergRecord;
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
        } else if (createdAtValue instanceof Long) {
            long micros = (Long) createdAtValue;
            long seconds = micros / 1_000_000L;
            long nanos = (micros % 1_000_000L) * 1_000L;
            createdAt = Instant.ofEpochSecond(seconds, nanos);
        }

        return VectorRecord.builder()
                .vectorId(asString(record.getField("vector_id")))
                .sourceTable(firstNonBlank(asString(field(record, "source_table", 1)), metadata.get("source_table")))
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
            // Fall through to positional lookup. Some Iceberg generic records expose projected
            // values positionally even when name lookup returns null for nested/projection scans.
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

    private static class PartitionBatch {
        private final PartitionKey partitionKey;
        private final List<Record> records = new ArrayList<>();

        private PartitionBatch(PartitionKey partitionKey) {
            this.partitionKey = partitionKey;
        }
    }
}
