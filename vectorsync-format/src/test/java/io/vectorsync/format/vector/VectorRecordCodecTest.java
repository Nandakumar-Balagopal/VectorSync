package io.vectorsync.format.vector;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.VectorRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VectorRecordCodecTest {

    private static final Schema SCHEMA = VectorTableSchema.schema();
    private static final Instant CREATED_AT = Instant.parse("2026-01-02T03:04:05Z");

    private static VectorRecord sample(boolean deleted) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("snapshot_id", "102");
        metadata.put("operation", deleted ? "DELETE" : "INSERT");

        return VectorRecord.builder()
                .vectorId("v-1")
                .sourceTable("default.products")
                .sourceRowId("p-100")
                .embedding(List.of(0.5, -0.25, 1.0))
                .text("Trail Runner | Lightweight trail running shoe")
                .metadata(metadata)
                .modelName("all-MiniLM-L6-v2")
                .createdAt(CREATED_AT)
                .deleted(deleted)
                .build();
    }

    @Test
    @DisplayName("a live record survives a write/read round trip")
    void roundTripsLiveRecord() {
        VectorRecord decoded = VectorRecordCodec.fromIcebergRecord(
                VectorRecordCodec.toIcebergRecord(SCHEMA, sample(false)));

        assertEquals("v-1", decoded.getVectorId());
        assertEquals("default.products", decoded.getSourceTable());
        assertEquals("p-100", decoded.getSourceRowId());
        assertEquals(List.of(0.5, -0.25, 1.0), decoded.getEmbedding());
        assertEquals("Trail Runner | Lightweight trail running shoe", decoded.getText());
        assertEquals("all-MiniLM-L6-v2", decoded.getModelName());
        assertEquals(CREATED_AT, decoded.getCreatedAt());
        assertFalse(decoded.isDeleted());
        assertEquals("102", decoded.getMetadata().get("snapshot_id"));
    }

    @Test
    @DisplayName("the deleted flag survives the round trip via metadata")
    void roundTripsTombstone() {
        VectorRecord decoded = VectorRecordCodec.fromIcebergRecord(
                VectorRecordCodec.toIcebergRecord(SCHEMA, sample(true)));

        assertTrue(decoded.isDeleted());
        assertEquals("true", decoded.getMetadata().get("deleted"));
    }

    @Test
    @DisplayName("source table falls back to metadata when the column is absent")
    void fallsBackToMetadataSourceTable() {
        Record record = GenericRecord.create(SCHEMA);
        record.setField(Constants.VECTOR_ID_COLUMN, "v-2");
        record.setField(Constants.METADATA_COLUMN, Map.of(
                "source_table", "default.reviews",
                "id", "r-9"));
        record.setField(Constants.MODEL_NAME_COLUMN, "m1");

        VectorRecord decoded = VectorRecordCodec.fromIcebergRecord(record);

        assertEquals("default.reviews", decoded.getSourceTable());
        assertEquals("r-9", decoded.getSourceRowId());
    }

    @Test
    @DisplayName("epoch-microsecond timestamps decode, as produced by some projected scans")
    void decodesEpochMicrosTimestamp() {
        Record record = GenericRecord.create(SCHEMA);
        record.setField(Constants.VECTOR_ID_COLUMN, "v-3");
        record.setField(Constants.CREATED_AT_COLUMN, CREATED_AT.getEpochSecond() * 1_000_000L);

        assertEquals(CREATED_AT, VectorRecordCodec.fromIcebergRecord(record).getCreatedAt());
    }

    @Test
    @DisplayName("a missing embedding decodes to an empty list rather than null")
    void missingEmbeddingDecodesEmpty() {
        Record record = GenericRecord.create(SCHEMA);
        record.setField(Constants.VECTOR_ID_COLUMN, "v-4");

        VectorRecord decoded = VectorRecordCodec.fromIcebergRecord(record);

        assertEquals(List.of(), decoded.getEmbedding());
        assertNull(decoded.getCreatedAt());
        assertFalse(decoded.isDeleted());
    }

    @Test
    @DisplayName("a null metadata map on the DTO still yields the deleted marker on write")
    void writesDeletedMarkerWithNullMetadata() {
        VectorRecord withoutMetadata = VectorRecord.builder()
                .vectorId("v-5")
                .sourceTable("default.products")
                .sourceRowId("p-1")
                .embedding(List.of(1.0))
                .text("t")
                .metadata(null)
                .modelName("m1")
                .createdAt(CREATED_AT)
                .deleted(true)
                .build();

        VectorRecord decoded = VectorRecordCodec.fromIcebergRecord(
                VectorRecordCodec.toIcebergRecord(SCHEMA, withoutMetadata));

        assertTrue(decoded.isDeleted());
    }
}
