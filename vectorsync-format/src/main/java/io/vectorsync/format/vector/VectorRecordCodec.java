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
 * <p>Embeddings are narrowed to float32 on write and widened back on read. Models emit float32, so
 * this is lossless in practice and halves stored size.
 */
public final class VectorRecordCodec {

    private VectorRecordCodec() {
    }

    public static Record toIcebergRecord(Schema schema, VectorRecord record) {
        Record icebergRecord = GenericRecord.create(schema);

        icebergRecord.setField(Constants.VECTOR_ID_COLUMN, record.getVectorId());
        icebergRecord.setField(Constants.SOURCE_TABLE_COLUMN, record.getSourceTable());
        icebergRecord.setField(Constants.SOURCE_ROW_ID_COLUMN, record.getSourceRowId());
        icebergRecord.setField(Constants.SOURCE_SNAPSHOT_ID_COLUMN, record.getSourceSnapshotId());
        icebergRecord.setField(Constants.SOURCE_SEQUENCE_NUMBER_COLUMN, record.getSourceSequenceNumber());
        icebergRecord.setField(Constants.SOURCE_COMMITTED_AT_COLUMN, record.getSourceCommittedAtMillis());
        icebergRecord.setField(Constants.CHUNK_ORDINAL_COLUMN, record.getChunkOrdinal());
        icebergRecord.setField(Constants.EMBEDDING_MODEL_COLUMN, record.getEmbeddingModel());
        icebergRecord.setField(Constants.EMBEDDING_VERSION_COLUMN, record.getEmbeddingVersion());
        icebergRecord.setField(Constants.MODEL_VERSION_COLUMN, record.modelVersion());
        icebergRecord.setField(Constants.EMBEDDING_DIM_COLUMN, record.getEmbeddingDim());
        icebergRecord.setField(Constants.PREPROCESSING_ID_COLUMN, record.getPreprocessingId());
        icebergRecord.setField(Constants.EMBEDDING_COLUMN, toFloatList(record.getEmbedding()));
        icebergRecord.setField(Constants.TEXT_COLUMN, record.getText());
        icebergRecord.setField(Constants.DELETED_COLUMN, record.isDeleted());

        Map<String, String> metadata = record.getMetadata() == null
                ? Map.of()
                : new HashMap<>(record.getMetadata());
        icebergRecord.setField(Constants.METADATA_COLUMN, metadata);

        Instant createdAt = record.getCreatedAt() == null ? Instant.now() : record.getCreatedAt();
        icebergRecord.setField(Constants.CREATED_AT_COLUMN, OffsetDateTime.ofInstant(createdAt, ZoneOffset.UTC));

        return icebergRecord;
    }

    public static VectorRecord fromIcebergRecord(Record record) {
        return VectorRecord.builder()
                .vectorId(asString(record.getField(Constants.VECTOR_ID_COLUMN)))
                .sourceTable(asString(record.getField(Constants.SOURCE_TABLE_COLUMN)))
                .sourceRowId(asString(record.getField(Constants.SOURCE_ROW_ID_COLUMN)))
                .sourceSnapshotId(asLong(record.getField(Constants.SOURCE_SNAPSHOT_ID_COLUMN)))
                .sourceSequenceNumber(asLong(record.getField(Constants.SOURCE_SEQUENCE_NUMBER_COLUMN)))
                .sourceCommittedAtMillis(asLong(record.getField(Constants.SOURCE_COMMITTED_AT_COLUMN)))
                .chunkOrdinal(asInt(record.getField(Constants.CHUNK_ORDINAL_COLUMN)))
                .embeddingModel(asString(record.getField(Constants.EMBEDDING_MODEL_COLUMN)))
                .embeddingVersion(asString(record.getField(Constants.EMBEDDING_VERSION_COLUMN)))
                .embeddingDim(asInt(record.getField(Constants.EMBEDDING_DIM_COLUMN)))
                .preprocessingId(asString(record.getField(Constants.PREPROCESSING_ID_COLUMN)))
                .embedding(toDoubleList(record.getField(Constants.EMBEDDING_COLUMN)))
                .text(asString(record.getField(Constants.TEXT_COLUMN)))
                .deleted(Boolean.TRUE.equals(record.getField(Constants.DELETED_COLUMN)))
                .metadata(toStringMap(record.getField(Constants.METADATA_COLUMN)))
                .createdAt(toInstant(record.getField(Constants.CREATED_AT_COLUMN)))
                .build();
    }

    static Instant toInstant(Object value) {
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (value instanceof Long micros) {
            long seconds = Math.floorDiv(micros, 1_000_000L);
            long nanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
            return Instant.ofEpochSecond(seconds, nanos);
        }
        return null;
    }

    private static List<Float> toFloatList(List<Double> embedding) {
        if (embedding == null) {
            return List.of();
        }
        return embedding.stream()
                .filter(Objects::nonNull)
                .map(Double::floatValue)
                .toList();
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

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static long asLong(Object value) {
        return value instanceof Number number ? number.longValue() : 0L;
    }

    private static int asInt(Object value) {
        return value instanceof Number number ? number.intValue() : 0;
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
