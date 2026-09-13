package io.vectorsync.format.vector;

import io.vectorsync.common.dto.VectorRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the resolution semantics that every reader of the vector table must agree on.
 */
class VectorResolutionTest {

    private static final Instant T1 = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant T2 = Instant.parse("2026-01-02T00:00:00Z");
    private static final Instant T3 = Instant.parse("2026-01-03T00:00:00Z");

    private static VectorRecord record(String vectorId,
                                       String sourceRowId,
                                       String modelName,
                                       Instant createdAt,
                                       boolean deleted) {
        return VectorRecord.builder()
                .vectorId(vectorId)
                .sourceTable("default.products")
                .sourceRowId(sourceRowId)
                .embedding(List.of(1.0, 0.0))
                .text("text-" + vectorId)
                .metadata(Map.of())
                .modelName(modelName)
                .createdAt(createdAt)
                .deleted(deleted)
                .build();
    }

    @Test
    @DisplayName("newest record wins for the same source row and model")
    void newestWins() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", "m1", T1, false),
                record("v2", "p-100", "m1", T2, false)
        ));

        assertEquals(1, resolved.size());
        assertEquals("v2", resolved.get(0).getVectorId());
    }

    @Test
    @DisplayName("input ordering does not affect the winner")
    void orderIndependent() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v2", "p-100", "m1", T2, false),
                record("v1", "p-100", "m1", T1, false)
        ));

        assertEquals(1, resolved.size());
        assertEquals("v2", resolved.get(0).getVectorId());
    }

    @Test
    @DisplayName("a tombstone hides the source row")
    void tombstoneHidesRow() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", "m1", T1, false),
                record("v2", "p-100", "m1", T2, true)
        ));

        assertTrue(resolved.isEmpty());
    }

    @Test
    @DisplayName("a re-insert after a tombstone revives the source row")
    void reinsertAfterTombstone() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", "m1", T1, false),
                record("v2", "p-100", "m1", T2, true),
                record("v3", "p-100", "m1", T3, false)
        ));

        assertEquals(1, resolved.size());
        assertEquals("v3", resolved.get(0).getVectorId());
    }

    @Test
    @DisplayName("different models coexist rather than superseding each other")
    void modelsCoexist() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", "model-v1", T1, false),
                record("v2", "p-100", "model-v2", T2, false)
        ));

        assertEquals(2, resolved.size());
        assertEquals(
                List.of("model-v1", "model-v2"),
                resolved.stream().map(VectorRecord::getModelName).sorted().toList());
    }

    @Test
    @DisplayName("a tombstone for one model does not hide another model's vector")
    void tombstoneIsPerModel() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", "model-v1", T1, true),
                record("v2", "p-100", "model-v2", T2, false)
        ));

        assertEquals(1, resolved.size());
        assertEquals("model-v2", resolved.get(0).getModelName());
    }

    @Test
    @DisplayName("rows without a resolvable source key are dropped")
    void dropsRowsWithoutSourceKey() {
        VectorRecord keyless = VectorRecord.builder()
                .vectorId("v-keyless")
                .sourceTable(null)
                .sourceRowId(null)
                .embedding(List.of(1.0))
                .metadata(Map.of())
                .modelName("m1")
                .createdAt(T1)
                .build();

        assertTrue(VectorResolution.latestLiveVectors(List.of(keyless)).isEmpty());
    }

    @Test
    @DisplayName("source key falls back to metadata source_row_id then id")
    void sourceKeyFallsBackToMetadata() {
        VectorRecord viaSourceRowId = VectorRecord.builder()
                .vectorId("v1")
                .sourceRowId(null)
                .metadata(Map.of("source_table", "default.products", "source_row_id", "p-100"))
                .modelName("m1")
                .createdAt(T1)
                .build();

        VectorRecord viaId = VectorRecord.builder()
                .vectorId("v2")
                .sourceRowId(null)
                .metadata(Map.of("source_table", "default.products", "id", "p-200"))
                .modelName("m1")
                .createdAt(T1)
                .build();

        assertEquals("default.products::p-100::m1", VectorResolution.sourceKey(viaSourceRowId));
        assertEquals("default.products::p-200::m1", VectorResolution.sourceKey(viaId));
    }

    @Test
    @DisplayName("a null createdAt sorts oldest so a timestamped record supersedes it")
    void nullCreatedAtSortsFirst() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v-null", "p-100", "m1", null, false),
                record("v-dated", "p-100", "m1", T1, false)
        ));

        assertEquals(1, resolved.size());
        assertEquals("v-dated", resolved.get(0).getVectorId());
    }
}
