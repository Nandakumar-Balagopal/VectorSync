package io.vectorsync.worker.service;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.worker.service.embedding.EmbeddingException;
import io.vectorsync.worker.service.embedding.EmbeddingService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CDCServiceTest {

    private static final TableConfig CONFIG = TableConfig.builder()
            .tableId("t-1")
            .catalog("default")
            .tableName("default.products")
            .embeddingColumns(List.of("name", "description"))
            .modelName("all-MiniLM-L6-v2")
            .embeddingVersion("v1")
            .enabled(true)
            .build();

    /** Records how it was called, so batching can be asserted rather than assumed. */
    private static class RecordingEmbeddingService implements EmbeddingService {
        final List<List<String>> batchCalls = new ArrayList<>();
        int singleCalls;
        boolean fail;

        @Override
        public List<Double> generateEmbedding(String text) {
            singleCalls++;
            return List.of(1.0, 0.0, 0.0);
        }

        @Override
        public List<List<Double>> generateEmbeddings(List<String> texts) throws EmbeddingException {
            batchCalls.add(List.copyOf(texts));
            if (fail) {
                throw new EmbeddingException("provider unavailable");
            }
            return texts.stream().map(text -> List.of(1.0, 0.0, 0.0)).toList();
        }
    }

    private static ChangeEvent event(String operation, String id, String name, long snapshotId) {
        return ChangeEvent.builder()
                .tableId("t-1")
                .snapshotId(snapshotId)
                .previousSnapshotId(snapshotId - 1)
                .operation(operation)
                .rowData(new java.util.HashMap<>(Map.of(
                        "id", id,
                        "name", name,
                        "description", "description of " + name)))
                .detectedAt(Instant.now())
                .build();
    }

    @Test
    @DisplayName("a whole snapshot is embedded in one batch call, not one per row")
    void embedsInOneBatch() {
        RecordingEmbeddingService embeddings = new RecordingEmbeddingService();
        CDCService service = new CDCService(embeddings);

        CDCService.MaterializationResult result = service.processChangeEvents(CONFIG, List.of(
                event(ChangeEvent.OPERATION_INSERT, "p-1", "Trail Runner", 100L),
                event(ChangeEvent.OPERATION_INSERT, "p-2", "City Sneaker", 100L),
                event(ChangeEvent.OPERATION_UPDATE, "p-3", "Summit Boot", 100L)));

        assertEquals(1, embeddings.batchCalls.size(), "one round trip for the whole snapshot");
        assertEquals(3, embeddings.batchCalls.get(0).size());
        assertEquals(0, embeddings.singleCalls, "the per-text path must not be used");
        assertEquals(3, result.records().size());
        assertTrue(result.complete());
    }

    @Test
    @DisplayName("lineage is captured on every record")
    void capturesLineage() {
        CDCService service = new CDCService(new RecordingEmbeddingService());

        VectorRecord record = service.processChangeEvents(CONFIG,
                List.of(event(ChangeEvent.OPERATION_INSERT, "p-1", "Trail Runner", 4242L)))
                .records().get(0);

        assertEquals("default.products", record.getSourceTable());
        assertEquals("p-1", record.getSourceRowId());
        assertEquals(4242L, record.getSourceSnapshotId());
        assertEquals(0, record.getChunkOrdinal());
        assertEquals("all-MiniLM-L6-v2", record.getEmbeddingModel());
        assertEquals("v1", record.getEmbeddingVersion());
        assertEquals(3, record.getEmbeddingDim());
        assertNotNull(record.getPreprocessingId());
        assertNotNull(record.getVectorId());
        assertEquals("Trail Runner | description of Trail Runner", record.getText());
        assertEquals("INSERT", record.getMetadata().get("operation"));
    }

    @Test
    @DisplayName("identity is deterministic across runs")
    void identityIsDeterministic() {
        CDCService service = new CDCService(new RecordingEmbeddingService());
        ChangeEvent change = event(ChangeEvent.OPERATION_INSERT, "p-1", "Trail Runner", 100L);

        String first = service.processChangeEvents(CONFIG, List.of(change)).records().get(0).getVectorId();
        String second = service.processChangeEvents(CONFIG, List.of(change)).records().get(0).getVectorId();

        assertEquals(first, second);
    }

    @Test
    @DisplayName("a delete becomes a tombstone and needs no embedding")
    void deleteProducesTombstone() {
        RecordingEmbeddingService embeddings = new RecordingEmbeddingService();
        CDCService service = new CDCService(embeddings);

        CDCService.MaterializationResult result = service.processChangeEvents(CONFIG,
                List.of(event(ChangeEvent.OPERATION_DELETE, "p-1", "Trail Runner", 200L)));

        assertTrue(embeddings.batchCalls.isEmpty(), "a tombstone must not call the provider");
        VectorRecord tombstone = result.records().get(0);
        assertTrue(tombstone.isDeleted());
        assertEquals(List.of(), tombstone.getEmbedding());
        assertEquals(0, tombstone.getEmbeddingDim());
        assertTrue(result.complete());
    }

    @Test
    @DisplayName("deletes still materialize when the embedding provider is down")
    void tombstonesSurviveProviderFailure() {
        RecordingEmbeddingService embeddings = new RecordingEmbeddingService();
        embeddings.fail = true;
        CDCService service = new CDCService(embeddings);

        CDCService.MaterializationResult result = service.processChangeEvents(CONFIG, List.of(
                event(ChangeEvent.OPERATION_DELETE, "p-1", "Trail Runner", 200L),
                event(ChangeEvent.OPERATION_INSERT, "p-2", "City Sneaker", 200L)));

        assertEquals(1, result.records().size(), "the tombstone is unaffected");
        assertEquals(1, result.failed(), "the insert failed to embed");
        assertFalse(result.complete(), "so the watermark must hold");
    }

    @Test
    @DisplayName("a failed batch fails every row in it so the watermark holds")
    void batchFailureBlocksWatermark() {
        RecordingEmbeddingService embeddings = new RecordingEmbeddingService();
        embeddings.fail = true;
        CDCService service = new CDCService(embeddings);

        CDCService.MaterializationResult result = service.processChangeEvents(CONFIG, List.of(
                event(ChangeEvent.OPERATION_INSERT, "p-1", "a", 100L),
                event(ChangeEvent.OPERATION_INSERT, "p-2", "b", 100L)));

        assertTrue(result.records().isEmpty());
        assertEquals(2, result.failed());
        assertFalse(result.complete());
    }

    @Test
    @DisplayName("a row with no embeddable text is skipped, not counted as a failure")
    void blankTextIsSkippedNotFailed() {
        CDCService service = new CDCService(new RecordingEmbeddingService());

        ChangeEvent blank = ChangeEvent.builder()
                .tableId("t-1")
                .snapshotId(100L)
                .operation(ChangeEvent.OPERATION_INSERT)
                .rowData(new java.util.HashMap<>(Map.of("id", "p-1")))
                .detectedAt(Instant.now())
                .build();

        CDCService.MaterializationResult result = service.processChangeEvents(CONFIG, List.of(blank));

        assertTrue(result.records().isEmpty());
        assertEquals(0, result.failed());
        assertTrue(result.complete(), "a legitimately empty snapshot is safe to advance past");
    }

    @Test
    @DisplayName("a row without an id fails rather than getting a random identity")
    void missingIdFails() {
        CDCService service = new CDCService(new RecordingEmbeddingService());

        ChangeEvent noId = ChangeEvent.builder()
                .tableId("t-1")
                .snapshotId(100L)
                .operation(ChangeEvent.OPERATION_INSERT)
                .rowData(new java.util.HashMap<>(Map.of("name", "Trail Runner")))
                .detectedAt(Instant.now())
                .build();

        CDCService.MaterializationResult result = service.processChangeEvents(CONFIG, List.of(noId));

        assertEquals(1, result.failed());
        assertFalse(result.complete());
    }

    @Test
    @DisplayName("a provider returning the wrong count fails the batch instead of mismatching rows")
    void countMismatchFailsBatch() {
        CDCService service = new CDCService(new EmbeddingService() {
            @Override
            public List<Double> generateEmbedding(String text) {
                return List.of(1.0);
            }

            @Override
            public List<List<Double>> generateEmbeddings(List<String> texts) {
                return List.of(List.of(1.0));  // one embedding for two texts
            }
        });

        CDCService.MaterializationResult result = service.processChangeEvents(CONFIG, List.of(
                event(ChangeEvent.OPERATION_INSERT, "p-1", "a", 100L),
                event(ChangeEvent.OPERATION_INSERT, "p-2", "b", 100L)));

        assertTrue(result.records().isEmpty());
        assertEquals(2, result.failed());
    }
}
