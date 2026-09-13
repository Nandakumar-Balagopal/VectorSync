package io.vectorsync.format.vector;

import io.vectorsync.common.dto.VectorRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class VectorIdsTest {

    private static VectorRecord record() {
        return VectorRecord.builder()
                .sourceTable("default.products")
                .sourceRowId("p-100")
                .sourceSnapshotId(100L)
                .chunkOrdinal(0)
                .embeddingModel("minilm")
                .embeddingVersion("v1")
                .preprocessingId("pp1")
                .embedding(List.of(1.0))
                .createdAt(Instant.parse("2026-01-01T00:00:00Z"))
                .build();
    }

    @Test
    @DisplayName("the same lineage always yields the same id, so retries are idempotent")
    void deterministic() {
        assertEquals(VectorIds.vectorId(record()), VectorIds.vectorId(record()));
    }

    @Test
    @DisplayName("identity ignores wall-clock time and payload")
    void ignoresNonLineageFields() {
        VectorRecord other = record();
        other.setCreatedAt(Instant.parse("2030-12-31T23:59:59Z"));
        other.setEmbedding(List.of(9.0, 9.0));
        other.setText("completely different text");
        other.setDeleted(true);

        assertEquals(VectorIds.vectorId(record()), VectorIds.vectorId(other));
    }

    @Test
    @DisplayName("every lineage field changes identity")
    void lineageFieldsAreSignificant() {
        String base = VectorIds.vectorId(record());

        VectorRecord table = record();
        table.setSourceTable("default.reviews");
        assertNotEquals(base, VectorIds.vectorId(table));

        VectorRecord row = record();
        row.setSourceRowId("p-200");
        assertNotEquals(base, VectorIds.vectorId(row));

        VectorRecord snapshot = record();
        snapshot.setSourceSnapshotId(200L);
        assertNotEquals(base, VectorIds.vectorId(snapshot));

        VectorRecord chunk = record();
        chunk.setChunkOrdinal(1);
        assertNotEquals(base, VectorIds.vectorId(chunk));

        VectorRecord model = record();
        model.setEmbeddingModel("mpnet");
        assertNotEquals(base, VectorIds.vectorId(model));

        VectorRecord version = record();
        version.setEmbeddingVersion("v2");
        assertNotEquals(base, VectorIds.vectorId(version));

        VectorRecord preprocessing = record();
        preprocessing.setPreprocessingId("pp2");
        assertNotEquals(base, VectorIds.vectorId(preprocessing));
    }

    @Test
    @DisplayName("field boundaries are unambiguous")
    void noFieldBoundaryCollision() {
        VectorRecord a = record();
        a.setSourceTable("ab");
        a.setSourceRowId("c");

        VectorRecord b = record();
        b.setSourceTable("a");
        b.setSourceRowId("bc");

        assertNotEquals(VectorIds.vectorId(a), VectorIds.vectorId(b));
    }

    @Test
    @DisplayName("preprocessing id tracks the embedded columns and their join separator")
    void preprocessingIdTracksConfiguration() {
        String base = VectorIds.preprocessingId(List.of("name", "description"), " | ");

        assertEquals(base, VectorIds.preprocessingId(List.of("name", "description"), " | "));
        assertNotEquals(base, VectorIds.preprocessingId(List.of("description", "name"), " | "));
        assertNotEquals(base, VectorIds.preprocessingId(List.of("name"), " | "));
        assertNotEquals(base, VectorIds.preprocessingId(List.of("name", "description"), " "));
    }

    @Test
    @DisplayName("index identity covers coverage plus algorithm configuration")
    void indexIdCoversConfiguration() {
        String base = VectorIds.indexId("default.products", 100L, "minilm", "v1", "hnsw", "m=16,ef=100");

        assertEquals(base, VectorIds.indexId("default.products", 100L, "minilm", "v1", "hnsw", "m=16,ef=100"));
        assertNotEquals(base, VectorIds.indexId("default.products", 200L, "minilm", "v1", "hnsw", "m=16,ef=100"));
        assertNotEquals(base, VectorIds.indexId("default.products", 100L, "minilm", "v2", "hnsw", "m=16,ef=100"));
        assertNotEquals(base, VectorIds.indexId("default.products", 100L, "minilm", "v1", "ivf", "m=16,ef=100"));
        assertNotEquals(base, VectorIds.indexId("default.products", 100L, "minilm", "v1", "hnsw", "m=32,ef=100"));
    }
}
