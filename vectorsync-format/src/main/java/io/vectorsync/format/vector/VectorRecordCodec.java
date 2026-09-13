package io.vectorsync.format.vector;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.VectorRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Converts between {@link VectorRecord} and Iceberg generic records.
 *
 * <p>Unified from two previously duplicated implementations in {@code VectorStoreService} and
 * {@code VectorSyncReader}. Those had drifted apart: only the writer's copy decoded
 * epoch-microsecond {@code created_at} values, so the search path silently dropped timestamps on
 * any scan that returned them as a {@code Long}. This unifies on the superset, which makes the
 * search path strictly more tolerant than before.
 */
public final class VectorRecordCodec {

    /** Positional fallbacks, used when a projected scan returns null for a named lookup. */
    private static final int POS_VECTOR_ID = 0;
    private static final int POS_SOURCE_TABLE = 1;
    private static final int POS_SOURCE_ROW_ID = 2;
    private static final int POS_EMBEDDING = 3;
    private static final int POS_TEXT = 4;
    private static final int POS_METADATA = 5;
    private static final int POS_MODEL_NAME = 6;
    private static final int POS_CREATED_AT = 7;

    private static final String METADATA_DELETED = "deleted";
    private static final String METADATA_SOURCE_TABLE = "source_table";
    private static final String METADATA_SOURCE_ROW_ID = "source_row_id";
    private static final String METADATA_ID = "id";

    private VectorRecordCodec() {
    }

    public static Record toIcebergRecord(Schema schema, VectorRecord record) {
        Record icebergRecord = GenericRecord.create(schema);
        icebergRecord.setField(Constants.VECTOR_ID_COLUMN, record.getVectorId());

        String sourceTable = record.getSourceTable();
        if (sourceTable == null && record.getMetadata() != null) {
            sourceTable = record.getMetadata().get(METADATA_SOURCE_TABLE);
        }
        icebergRecord.setField(Constants.SOURCE_TABLE_COLUMN, sourceTable);
        icebergRecord.setField(Constants.SOURCE_ROW_ID_COLUMN, record.getSourceRowId());
        icebergRecord.setField(Constants.EMBEDDING_COLUMN, record.getEmbedding());
        icebergRecord.setField(Constants.TEXT_COLUMN, record.getText());

        Map<String, String> metadata = record.getMetadata() == null
                ? new HashMap<>()
                : new HashMap<>(record.getMetadata());
        metadata.put(METADATA_DELETED, String.valueOf(record.isDeleted()));
        icebergRecord.setField(Constants.METADATA_COLUMN, metadata);

        icebergRecord.setField(Constants.MODEL_NAME_COLUMN, record.getModelName());

        Instant createdAt = record.getCreatedAt();
        if (createdAt != null) {
            icebergRecord.setField(Constants.CREATED_AT_COLUMN, OffsetDateTime.ofInstant(createdAt, ZoneOffset.UTC));
        }

        return icebergRecord;
    }

    public static VectorRecord fromIcebergRecord(Record record) {
        List<Double> embedding = toDoubleList(field(record, Constants.EMBEDDING_COLUMN, POS_EMBEDDING));
        Map<String, String> metadata = toStringMap(field(record, Constants.METADATA_COLUMN, POS_METADATA));

        return VectorRecord.builder()
                .vectorId(asString(field(record, Constants.VECTOR_ID_COLUMN, POS_VECTOR_ID)))
                .sourceTable(firstNonBlank(
                        asString(field(record, Constants.SOURCE_TABLE_COLUMN, POS_SOURCE_TABLE)),
                        metadata.get(METADATA_SOURCE_TABLE)))
                .sourceRowId(firstNonBlank(
                        firstNonBlank(
                                asString(field(record, Constants.SOURCE_ROW_ID_COLUMN, POS_SOURCE_ROW_ID)),
                                metadata.get(METADATA_SOURCE_ROW_ID)),
                        metadata.get(METADATA_ID)))
                .embedding(embedding)
                .text(asString(field(record, Constants.TEXT_COLUMN, POS_TEXT)))
                .metadata(metadata)
                .modelName(asString(field(record, Constants.MODEL_NAME_COLUMN, POS_MODEL_NAME)))
                .createdAt(toInstant(field(record, Constants.CREATED_AT_COLUMN, POS_CREATED_AT)))
                .deleted(Objects.equals("true", metadata.get(METADATA_DELETED)))
                .build();
    }

    private static Instant toInstant(Object value) {
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (value instanceof Long micros) {
            long seconds = micros / 1_000_000L;
            long nanos = (micros % 1_000_000L) * 1_000L;
            return Instant.ofEpochSecond(seconds, nanos);
        }
        return null;
    }

    /**
     * Reads a field by name, falling back to a positional lookup. Some Iceberg generic records
     * expose projected values positionally even when the named lookup returns null.
     */
    private static Object field(Record record, String name, int position) {
        try {
            Object value = record.getField(name);
            if (value != null) {
                return value;
            }
        } catch (Exception ignored) {
            // Fall through to the positional lookup below.
        }
        try {
            return record.get(position);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String firstNonBlank(String primary, String fallback) {
        return primary == null || primary.isBlank() ? fallback : primary;
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static List<Double> toDoubleList(Object value) {
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

    private static Map<String, String> toStringMap(Object value) {
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
