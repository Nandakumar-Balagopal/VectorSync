package io.vectorsync.format.vector;

import io.vectorsync.common.dto.VectorRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the resolution semantics every reader of the vector table must agree on.
 */
class VectorResolutionTest {

    private static VectorRecord record(String vectorId,
                                       String sourceRowId,
                                       long snapshotId,
                                       String model,
                                       String version,
                                       boolean deleted) {
        return VectorRecord.builder()
                .vectorId(vectorId)
                .sourceTable("default.products")
                .sourceRowId(sourceRowId)
                .sourceSnapshotId(snapshotId)
                // Snapshot ids are random in Iceberg; the sequence number is what orders history,
                // so tests derive one from the logical version being expressed.
                .sourceSequenceNumber(snapshotId / 100)
                .chunkOrdinal(0)
                .embeddingModel(model)
                .embeddingVersion(version)
                .embeddingDim(2)
                .preprocessingId("pp1")
                .embedding(List.of(1.0, 0.0))
                .text("text-" + vectorId)
                .deleted(deleted)
                .createdAt(Instant.parse("2026-01-01T00:00:00Z"))
                .build();
    }

    @Test
    @DisplayName("the newest source version wins for the same key")
    void newestSnapshotWins() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", 100L, "m", "v1", false),
                record("v2", "p-100", 200L, "m", "v1", false)
        ));

        assertEquals(1, resolved.size());
        assertEquals("v2", resolved.get(0).getVectorId());
    }

    @Test
    @DisplayName("input ordering does not affect the winner")
    void orderIndependent() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v2", "p-100", 200L, "m", "v1", false),
                record("v1", "p-100", 100L, "m", "v1", false)
        ));

        assertEquals("v2", resolved.get(0).getVectorId());
    }

    @Test
    @DisplayName("a random-looking snapshot id does not affect ordering")
    void snapshotIdDoesNotOrder() {
        // Mirrors real Iceberg ids: the later commit has the numerically smaller snapshot id.
        VectorRecord earlier = record("v-earlier", "p-100", 100L, "m", "v1", false);
        earlier.setSourceSnapshotId(7139976223410259010L);
        earlier.setSourceSequenceNumber(1L);

        VectorRecord later = record("v-later", "p-100", 200L, "m", "v1", true);
        later.setSourceSnapshotId(2135807640327332542L);
        later.setSourceSequenceNumber(2L);

        assertTrue(VectorResolution.latestLiveVectors(List.of(earlier, later)).isEmpty(),
                "the later tombstone must win even though its snapshot id is smaller");
    }

    @Test
    @DisplayName("wall-clock createdAt does not override version ordering")
    void snapshotBeatsWallClock() {
        VectorRecord older = record("v-old", "p-100", 100L, "m", "v1", false);
        older.setCreatedAt(Instant.parse("2026-06-01T00:00:00Z"));

        VectorRecord newer = record("v-new", "p-100", 200L, "m", "v1", false);
        newer.setCreatedAt(Instant.parse("2026-01-01T00:00:00Z"));

        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(older, newer));

        assertEquals(1, resolved.size());
        assertEquals("v-new", resolved.get(0).getVectorId(), "higher snapshot must win despite an older clock");
    }

    @Test
    @DisplayName("createdAt breaks ties only within one snapshot")
    void createdAtBreaksTiesWithinSnapshot() {
        VectorRecord first = record("v-first", "p-100", 100L, "m", "v1", false);
        first.setCreatedAt(Instant.parse("2026-01-01T00:00:00Z"));

        VectorRecord second = record("v-second", "p-100", 100L, "m", "v1", false);
        second.setCreatedAt(Instant.parse("2026-01-02T00:00:00Z"));

        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(first, second));

        assertEquals("v-second", resolved.get(0).getVectorId());
    }

    @Test
    @DisplayName("a tombstone hides the source row")
    void tombstoneHidesRow() {
        assertTrue(VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", 100L, "m", "v1", false),
                record("v2", "p-100", 200L, "m", "v1", true)
        )).isEmpty());
    }

    @Test
    @DisplayName("a re-insert at a later snapshot revives the source row")
    void reinsertAfterTombstone() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", 100L, "m", "v1", false),
                record("v2", "p-100", 200L, "m", "v1", true),
                record("v3", "p-100", 300L, "m", "v1", false)
        ));

        assertEquals(1, resolved.size());
        assertEquals("v3", resolved.get(0).getVectorId());
    }

    @Test
    @DisplayName("embedding versions coexist rather than superseding each other")
    void versionsCoexist() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", 100L, "minilm", "v1", false),
                record("v2", "p-100", 100L, "mpnet", "v2", false)
        ));

        assertEquals(2, resolved.size());
        assertEquals(
                List.of("minilm:v1", "mpnet:v2"),
                resolved.stream().map(VectorRecord::modelVersion).sorted().toList());
    }

    @Test
    @DisplayName("a tombstone in one version does not hide another version")
    void tombstoneIsPerModelVersion() {
        List<VectorRecord> resolved = VectorResolution.latestLiveVectors(List.of(
                record("v1", "p-100", 200L, "minilm", "v1", true),
                record("v2", "p-100", 200L, "mpnet", "v2", false)
        ));

        assertEquals(1, resolved.size());
        assertEquals("mpnet:v2", resolved.get(0).modelVersion());
    }

    @Test
    @DisplayName("chunks of the same row resolve independently")
    void chunksResolveIndependently() {
        VectorRecord chunk0 = record("v-c0", "p-100", 100L, "m", "v1", false);
        VectorRecord chunk1 = record("v-c1", "p-100", 100L, "m", "v1", false);
        chunk1.setChunkOrdinal(1);

        assertEquals(2, VectorResolution.latestLiveVectors(List.of(chunk0, chunk1)).size());
    }

    @Test
    @DisplayName("asOf ignores embeddings derived from later snapshots")
    void asOfSnapshotIsReproducible() {
        List<VectorRecord> history = List.of(
                record("v1", "p-100", 100L, "m", "v1", false),
                record("v2", "p-100", 200L, "m", "v1", false),
                record("v3", "p-100", 300L, "m", "v1", true)
        );

        assertEquals("v1", VectorResolution.liveVectorsAsOf(history, 1L).get(0).getVectorId());
        assertEquals("v2", VectorResolution.liveVectorsAsOf(history, 2L).get(0).getVectorId());
        assertTrue(VectorResolution.liveVectorsAsOf(history, 3L).isEmpty());
    }

    @Test
    @DisplayName("rows without a resolvable source key are dropped")
    void dropsRowsWithoutSourceKey() {
        VectorRecord keyless = VectorRecord.builder()
                .vectorId("v-keyless")
                .sourceTable(null)
                .sourceRowId(null)
                .embeddingModel("m")
                .embeddingVersion("v1")
                .embedding(List.of(1.0))
                .createdAt(Instant.now())
                .build();

        assertTrue(VectorResolution.latestLiveVectors(List.of(keyless)).isEmpty());
    }

    @Test
    @DisplayName("the source key spans table, row, chunk, and model version")
    void sourceKeyShape() {
        assertEquals(
                "default.products::p-100::0::m:v1",
                VectorResolution.sourceKey(record("v1", "p-100", 100L, "m", "v1", false)));
    }
}
