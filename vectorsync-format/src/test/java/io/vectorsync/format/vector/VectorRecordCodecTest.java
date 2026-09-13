package io.vectorsync.format.vector;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.VectorRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VectorRecordCodecTest {

    private static final Schema SCHEMA = VectorTableSchema.schema();
    private static final Instant CREATED_AT = Instant.parse("2026-01-02T03:04:05Z");

    private static VectorRecord sample(boolean deleted) {
        return VectorRecord.builder()
                .vectorId("v-1")
                .sourceTable("default.products")
                .sourceRowId("p-100")
                .sourceSnapshotId(4242L)
                .sourceSequenceNumber(7L)
                .sourceCommittedAtMillis(1_760_000_000_000L)
                .chunkOrdinal(3)
                .embeddingModel("all-MiniLM-L6-v2")
                .embeddingVersion("v2")
                .embeddingDim(3)
                .preprocessingId("pp-abc")
                .embedding(List.of(0.5, -0.25, 1.0))
                .text("Trail Runner | Lightweight trail running shoe")
                .deleted(deleted)
                .metadata(Map.of("operation", deleted ? "DELETE" : "INSERT"))
                .createdAt(CREATED_AT)
                .build();
    }

    @Test
    @DisplayName("all lineage fields survive a write/read round trip")
    void roundTripsLineage() {
        VectorRecord decoded = VectorRecordCodec.fromIcebergRecord(
                VectorRecordCodec.toIcebergRecord(SCHEMA, sample(false)));

        assertEquals("v-1", decoded.getVectorId());
        assertEquals("default.products", decoded.getSourceTable());
        assertEquals("p-100", decoded.getSourceRowId());
        assertEquals(4242L, decoded.getSourceSnapshotId());
        assertEquals(7L, decoded.getSourceSequenceNumber());
        assertEquals(1_760_000_000_000L, decoded.getSourceCommittedAtMillis());
        assertEquals(3, decoded.getChunkOrdinal());
        assertEquals("all-MiniLM-L6-v2", decoded.getEmbeddingModel());
        assertEquals("v2", decoded.getEmbeddingVersion());
        assertEquals(3, decoded.getEmbeddingDim());
        assertEquals("pp-abc", decoded.getPreprocessingId());
        assertEquals("Trail Runner | Lightweight trail running shoe", decoded.getText());
        assertEquals(CREATED_AT, decoded.getCreatedAt());
        assertFalse(decoded.isDeleted());
        assertEquals("INSERT", decoded.getMetadata().get("operation"));
    }

    @Test
    @DisplayName("the synthetic model_version partition column is written")
    void writesModelVersionPartitionColumn() {
        Record encoded = VectorRecordCodec.toIcebergRecord(SCHEMA, sample(false));

        assertEquals("all-MiniLM-L6-v2:v2", encoded.getField(Constants.MODEL_VERSION_COLUMN));
    }

    @Test
    @DisplayName("embeddings are narrowed to float32 on write")
    void narrowsEmbeddingToFloat() {
        Record encoded = VectorRecordCodec.toIcebergRecord(SCHEMA, sample(false));

        Object embedding = encoded.getField(Constants.EMBEDDING_COLUMN);
        assertTrue(embedding instanceof List<?>);
        assertEquals(List.of(0.5f, -0.25f, 1.0f), embedding);
    }

    @Test
    @DisplayName("float32 values widen back without drift for representable values")
    void widensFloatBackToDouble() {
        VectorRecord decoded = VectorRecordCodec.fromIcebergRecord(
                VectorRecordCodec.toIcebergRecord(SCHEMA, sample(false)));

        assertEquals(List.of(0.5, -0.25, 1.0), decoded.getEmbedding());
    }

    @Test
    @DisplayName("deleted is a typed column, not a metadata string")
    void deletedIsTypedColumn() {
        Record encoded = VectorRecordCodec.toIcebergRecord(SCHEMA, sample(true));

        assertEquals(Boolean.TRUE, encoded.getField(Constants.DELETED_COLUMN));
        assertTrue(VectorRecordCodec.fromIcebergRecord(encoded).isDeleted());
    }

    @Test
    @DisplayName("a tombstone carries no text and no embedding")
    void tombstoneHasNoPayload() {
        VectorRecord tombstone = VectorRecord.builder()
                .vectorId("v-del")
                .sourceTable("default.products")
                .sourceRowId("p-100")
                .sourceSnapshotId(99L)
                .embeddingModel("m")
                .embeddingVersion("v1")
                .embeddingDim(0)
                .preprocessingId("pp")
                .embedding(List.of())
                .text(null)
                .deleted(true)
                .createdAt(CREATED_AT)
                .build();

        VectorRecord decoded = VectorRecordCodec.fromIcebergRecord(
                VectorRecordCodec.toIcebergRecord(SCHEMA, tombstone));

        assertNull(decoded.getText());
        assertEquals(List.of(), decoded.getEmbedding());
        assertTrue(decoded.isDeleted());
    }

    @Test
    @DisplayName("epoch-microsecond timestamps decode, as produced by some projected scans")
    void decodesEpochMicrosTimestamp() {
        assertEquals(CREATED_AT, VectorRecordCodec.toInstant(CREATED_AT.getEpochSecond() * 1_000_000L));
    }

    @Test
    @DisplayName("a null createdAt is stamped at write time rather than failing a required column")
    void stampsMissingCreatedAt() {
        VectorRecord noTimestamp = sample(false);
        noTimestamp.setCreatedAt(null);

        Record encoded = VectorRecordCodec.toIcebergRecord(SCHEMA, noTimestamp);

        assertNotNull(encoded.getField(Constants.CREATED_AT_COLUMN));
        assertNotNull(VectorRecordCodec.fromIcebergRecord(encoded).getCreatedAt());
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
        assertEquals(0L, decoded.getSourceSnapshotId());
    }
}
