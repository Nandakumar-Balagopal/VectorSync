package io.vectorsync.searchservice;

import io.vectorsync.common.dto.SearchResult;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.index.IndexAliasStore;
import io.vectorsync.format.index.IndexManifestEntry;
import io.vectorsync.format.index.IndexStatus;
import io.vectorsync.format.io.IcebergAppender;
import io.vectorsync.format.vector.VectorIds;
import io.vectorsync.format.vector.VectorRecordCodec;
import io.vectorsync.format.vector.VectorTableSchema;
import io.vectorsync.searchservice.service.EvaluationService;
import io.vectorsync.searchservice.service.ProvenanceService;
import io.vectorsync.searchservice.service.SearchService;
import io.vectorsync.searchservice.service.iceberg.IcebergCatalogService;
import io.vectorsync.searchservice.service.iceberg.VectorSyncReader;
import io.vectorsync.searchservice.service.index.HnswIndexBuilder;
import io.vectorsync.searchservice.service.index.IndexRegistry;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the whole lifecycle through the real Spring beans against a local filesystem warehouse:
 * materialize two embedding versions, build an index over each, promote, search, evaluate,
 * roll back, and trace provenance.
 *
 * <p>Mock embeddings make similarity scores meaningless, so this asserts mechanics and lineage
 * rather than retrieval quality. The Docker E2E covers real embeddings.
 */
@SpringBootTest(properties = {
        "embedding.provider=mock",
        "iceberg.vector.namespace=vector",
})
class LifecycleIntegrationTest {

    private static final String SOURCE_TABLE = "default.products";
    private static final String MODEL = "all-MiniLM-L6-v2";
    private static final String V1 = MODEL + ":v1";
    private static final String V2 = MODEL + ":v2";
    private static final int DIM = 384;

    private static Path warehouse;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws IOException {
        warehouse = Files.createTempDirectory("vectorsync-search-it-");
        registry.add("iceberg.catalog.warehouse", () -> "file://" + warehouse);
        registry.add("vectorsync.index.base-uri", () -> "file://" + warehouse + "/indexes");
    }

    @Autowired
    IndexRegistry indexRegistry;
    @Autowired
    HnswIndexBuilder builder;
    @Autowired
    SearchService searchService;
    @Autowired
    EvaluationService evaluationService;
    @Autowired
    ProvenanceService provenanceService;
    @Autowired
    VectorSyncReader vectorSyncReader;
    @Autowired
    IcebergCatalogService catalogService;

    @BeforeEach
    void seedVectors() {
        if (!vectorSyncReader.readRaw().isEmpty()) {
            return;
        }

        Table table = VectorTableSchema.loadOrCreate(catalogService.getCatalog(), "vector");
        List<VectorRecord> records = List.of(
                vector("p-100", 100L, "v1", 0),
                vector("p-200", 100L, "v1", 1),
                vector("p-300", 100L, "v1", 2),
                vector("p-100", 100L, "v2", 3),
                vector("p-200", 100L, "v2", 4),
                vector("p-300", 100L, "v2", 5));

        IcebergAppender.append(table, records.stream()
                .map(record -> VectorRecordCodec.toIcebergRecord(table.schema(), record))
                .toList());
    }

    private VectorRecord vector(String rowId, long snapshotId, String version, int seed) {
        return vectorFor(SOURCE_TABLE, rowId, snapshotId, version, seed);
    }

    private VectorRecord vectorFor(String sourceTable,
                                   String rowId,
                                   long snapshotId,
                                   String version,
                                   int seed) {
        List<Double> embedding = new java.util.ArrayList<>(DIM);
        for (int i = 0; i < DIM; i++) {
            embedding.add(Math.sin(seed + 1 + i * 0.01));
        }

        VectorRecord record = VectorRecord.builder()
                .sourceTable(sourceTable)
                .sourceRowId(rowId)
                .sourceSnapshotId(snapshotId)
                .sourceSequenceNumber(snapshotId / 100)
                .chunkOrdinal(0)
                .embeddingModel(MODEL)
                .embeddingVersion(version)
                .embeddingDim(DIM)
                .preprocessingId("pp-1")
                .embedding(embedding)
                .text("product " + rowId + " " + version)
                .deleted(false)
                .metadata(Map.of("operation", "INSERT"))
                .createdAt(Instant.now())
                .build();
        record.setVectorId(VectorIds.vectorId(record));
        return record;
    }

    private IndexManifestEntry buildIndex(String modelVersion) {
        String[] parts = modelVersion.split(":", 2);
        return builder.build(SOURCE_TABLE, 100L, 1L, parts[0], parts[1],
                vectorSyncReader.readForIndex(SOURCE_TABLE, modelVersion));
    }

    @Test
    @DisplayName("both embedding versions are materialized and discoverable")
    void bothVersionsMaterialized() {
        assertEquals(List.of(V1, V2), vectorSyncReader.modelVersionsFor(SOURCE_TABLE));
        assertEquals(3, vectorSyncReader.readForIndex(SOURCE_TABLE, V1).size());
        assertEquals(3, vectorSyncReader.readForIndex(SOURCE_TABLE, V2).size());
    }

    @Test
    @DisplayName("an index artifact is durable, registered READY, and reopenable")
    void buildProducesDurableArtifact() {
        IndexManifestEntry entry = buildIndex(V1);

        assertEquals(IndexStatus.READY, entry.getStatus());
        assertEquals(3L, entry.getVectorCount());
        assertEquals(DIM, entry.getDimension());
        assertEquals("hnsw", entry.getIndexAlgorithm());
        assertEquals("cosine", entry.getSimilarityMetric());
        assertFalse(entry.getIndexFiles().isEmpty(), "artifact files must be recorded");
        assertTrue(entry.getIndexUri().contains(entry.getIndexId()));

        // registered in the manifest and readable back through Iceberg
        IndexManifestEntry stored = indexRegistry.manifest().findById(entry.getIndexId()).orElseThrow();
        assertEquals(IndexStatus.READY, stored.getStatus());
        assertEquals(entry.getIndexFiles(), stored.getIndexFiles());
    }

    @Test
    @DisplayName("rebuilding the same coverage and parameters yields the same index id")
    void buildIsDeterministic() {
        assertEquals(buildIndex(V1).getIndexId(), buildIndex(V1).getIndexId());
    }

    @Test
    @DisplayName("different embedding versions produce distinct indexes")
    void versionsProduceDistinctIndexes() {
        assertFalse(buildIndex(V1).getIndexId().equals(buildIndex(V2).getIndexId()));
    }

    @Test
    @DisplayName("search falls back to exact scan when nothing is promoted")
    void fallsBackToExactWhenUnpromoted() throws Exception {
        List<SearchResult> results = searchService.search("running shoe", 3, "default.unpromoted_table");

        assertNotNull(results);
        assertTrue(results.isEmpty(), "no vectors exist for that table");
    }

    @Test
    @DisplayName("promote, serve, roll back: the alias decides which version answers queries")
    void promoteServeRollback() throws Exception {
        IndexManifestEntry v1 = buildIndex(V1);
        IndexManifestEntry v2 = buildIndex(V2);

        // Tests share one Spring context and warehouse, so assert the delta this test adds
        // rather than an absolute count.
        int historyBefore = indexRegistry.aliases().history(SOURCE_TABLE).size();

        indexRegistry.aliases().promote(IndexAliasStore.PRODUCTION, SOURCE_TABLE, v1.getIndexId(), "test", "v1");
        assertEquals(v1.getIndexId(), indexRegistry.promotedIndex(SOURCE_TABLE).orElseThrow().getIndexId());

        List<SearchResult> onV1 = searchService.search("product", 3, SOURCE_TABLE);
        assertEquals(3, onV1.size());

        indexRegistry.aliases().promote(IndexAliasStore.PRODUCTION, SOURCE_TABLE, v2.getIndexId(), "test", "v2");
        assertEquals(v2.getIndexId(), indexRegistry.promotedIndex(SOURCE_TABLE).orElseThrow().getIndexId());

        List<SearchResult> onV2 = searchService.search("product", 3, SOURCE_TABLE);
        assertEquals(3, onV2.size());
        assertTrue(onV2.stream().allMatch(result -> result.getText().endsWith("v2")),
                "serving must come from the promoted version");

        // rollback
        String previous = indexRegistry.aliases()
                .previous(IndexAliasStore.PRODUCTION, SOURCE_TABLE).orElseThrow().getIndexId();
        assertEquals(v1.getIndexId(), previous);

        indexRegistry.aliases().promote(IndexAliasStore.PRODUCTION, SOURCE_TABLE, previous, "test", "rollback");
        List<SearchResult> rolledBack = searchService.search("product", 3, SOURCE_TABLE);
        assertTrue(rolledBack.stream().allMatch(result -> result.getText().endsWith("v1")),
                "rollback must restore the earlier version");

        List<io.vectorsync.format.index.IndexAliasEntry> history = indexRegistry.aliases().history(SOURCE_TABLE);
        assertEquals(historyBefore + 3, history.size(), "every promotion and rollback is retained");
        assertEquals(
                List.of(v1.getIndexId(), v2.getIndexId(), v1.getIndexId()),
                history.subList(history.size() - 3, history.size()).stream()
                        .map(io.vectorsync.format.index.IndexAliasEntry::getIndexId)
                        .toList());
    }

    @Test
    @DisplayName("index recall is measured against exact search and recorded on the manifest")
    void evaluationRecordsMetrics() throws Exception {
        IndexManifestEntry entry = buildIndex(V1);

        EvaluationService.EvaluationReport report = evaluationService.evaluate(
                entry.getIndexId(),
                List.of(new EvaluationService.QueryJudgement("product p-100", List.of("p-100")),
                        new EvaluationService.QueryJudgement("product p-200", List.of("p-200"))),
                3,
                "test-fixture@v1");

        assertEquals(2, report.queryCount());
        assertEquals(3, report.k());
        assertTrue(report.indexRecallAtK() >= 0.0 && report.indexRecallAtK() <= 1.0);
        assertNotNull(report.precisionAtK(), "labels were supplied, so precision is computed");
        assertTrue(report.precisionPersisted(), "a fixtureRef was supplied, so precision is evidence");

        Map<String, String> metrics = indexRegistry.manifest()
                .findById(entry.getIndexId()).orElseThrow().getEvalMetrics();
        assertTrue(metrics.containsKey("index_recall@3"));
        assertEquals("2", metrics.get("eval_query_count"));
        assertEquals("test-fixture@v1", metrics.get("eval_fixture"));
        assertEquals("supplied_queries", metrics.get("recall_source"));
    }

    @Test
    @DisplayName("precision without a fixture reference is returned but not recorded as evidence")
    void precisionWithoutFixtureIsNotPersisted() throws Exception {
        IndexManifestEntry entry = buildIndex(V1);

        EvaluationService.EvaluationReport report = evaluationService.evaluate(
                entry.getIndexId(),
                List.of(new EvaluationService.QueryJudgement("product p-100", List.of("p-100"))),
                3,
                null);

        assertNotNull(report.precisionAtK(), "the caller still gets the number back");
        assertFalse(report.precisionPersisted());

        Map<String, String> metrics = indexRegistry.manifest()
                .findById(entry.getIndexId()).orElseThrow().getEvalMetrics();
        assertTrue(metrics.containsKey("index_recall@3"), "recall needs no attribution");
        assertFalse(metrics.containsKey("precision@3"),
                "an unattributable score on the artifact would look like audit evidence");
        assertFalse(metrics.containsKey("eval_fixture"));
    }

    @Test
    @DisplayName("staleness is measured against the vector table, not against other indexes")
    void stalenessComesFromTheDataNotTheManifest() {
        // Its own source table: appending newer data to the shared one would change the version
        // list and vector counts the other tests assert on.
        String table = "default.staleness_probe";
        Table vectorTable = VectorTableSchema.loadOrCreate(catalogService.getCatalog(), "vector");

        appendOne(vectorTable, vectorFor(table, "s-1", 100L, "v1", 20));
        IndexManifestEntry entry = builder.build(table, 100L, 1L, MODEL, "v1",
                vectorSyncReader.readForIndex(table, V1));
        assertEquals(1L, entry.getSourceSequenceNumber());

        // The data moves on. The index is now stale by construction.
        appendOne(vectorTable, vectorFor(table, "s-2", 200L, "v1", 21));

        assertEquals(2L, vectorSyncReader.latestSourceSequenceNumber(table),
                "the source of truth for 'has the data moved on'");

        // The manifest only knows what indexes cover, so it names this index as the newest thing in
        // existence and the index compares favourably against itself forever. That was the bug.
        assertEquals(1L, indexRegistry.manifest().latestCoveredSequenceNumber(table));
        assertTrue(entry.getSourceSequenceNumber()
                        >= indexRegistry.manifest().latestCoveredSequenceNumber(table),
                "comparing an index against other indexes can never mark it stale");
        assertFalse(entry.getSourceSequenceNumber()
                        >= vectorSyncReader.latestSourceSequenceNumber(table),
                "comparing it against the data does");
    }

    private void appendOne(Table vectorTable, VectorRecord record) {
        IcebergAppender.append(vectorTable,
                List.of(VectorRecordCodec.toIcebergRecord(vectorTable.schema(), record)));
    }

    @Test
    @DisplayName("an unscoped exact scan resolves to one embedding space instead of mixing them")
    void unscopedExactSearchDoesNotMixModelVersions() throws Exception {
        String resolved = searchService.resolveScope(SOURCE_TABLE, null);
        assertNotNull(resolved, "an unscoped scan must still pick a version");
        assertTrue(vectorSyncReader.modelVersionsFor(SOURCE_TABLE).contains(resolved));

        // Ask for more results than one version holds. Scanning every version would return the
        // same source row once per version, scored across incompatible embedding spaces.
        List<SearchResult> results = searchService.searchExact("product p-100", 20, SOURCE_TABLE, null);
        List<String> rowIds = results.stream().map(SearchResult::getSourceRowId).toList();

        assertEquals(rowIds.size(), Set.copyOf(rowIds).size(),
                "a row appearing twice means the scan spanned more than one embedding version");
    }

    @Test
    @DisplayName("index recall runs with no caller input by sampling its own probes, and is deterministic")
    void indexRecallIsLabelFreeAndDeterministic() throws Exception {
        IndexManifestEntry entry = buildIndex(V1);

        EvaluationService.EvaluationReport first =
                evaluationService.evaluateIndexRecall(entry.getIndexId(), 3, 2);
        EvaluationService.EvaluationReport second =
                evaluationService.evaluateIndexRecall(entry.getIndexId(), 3, 2);

        assertEquals(2, first.probeCount());
        assertEquals(0, first.queryCount());
        assertNull(first.precisionAtK(), "no labels were involved");
        // Probing an index with vectors drawn from the index's own contents: each probe is its own
        // nearest neighbour, so a correctly built graph cannot miss the top hit.
        assertEquals(1.0, first.indexRecallAtK(), 1e-9);

        assertEquals(first.indexRecallAtK(), second.indexRecallAtK(), 1e-9);
        assertEquals(first.perProbeRecall().keySet(), second.perProbeRecall().keySet(),
                "sampling must be stable or the score cannot be compared against a threshold");

        Map<String, String> metrics = indexRegistry.manifest()
                .findById(entry.getIndexId()).orElseThrow().getEvalMetrics();
        assertEquals("2", metrics.get("recall_probe_count"));
        assertEquals("sampled_probes", metrics.get("recall_source"));
    }

    @Test
    @DisplayName("provenance walks a result back to the source snapshot it derives from")
    void provenanceWalksBackToSource() throws Exception {
        IndexManifestEntry entry = buildIndex(V2);
        indexRegistry.aliases().promote(IndexAliasStore.PRODUCTION, SOURCE_TABLE, entry.getIndexId(), "test", null);

        SearchResult result = searchService.search("product", 1, SOURCE_TABLE).get(0);
        Map<String, Object> chain = provenanceService.explain(result.getVectorId());

        assertNotNull(chain.get("vector"));
        assertNotNull(chain.get("servedByAlias"));
        assertNotNull(chain.get("index"));

        @SuppressWarnings("unchecked")
        Map<String, Object> embedding = (Map<String, Object>) chain.get("embedding");
        assertEquals(MODEL, embedding.get("model"));
        assertEquals("v2", embedding.get("version"));
        assertEquals("pp-1", embedding.get("preprocessingId"));

        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) chain.get("source");
        assertEquals(SOURCE_TABLE, source.get("table"));
        assertEquals(100L, source.get("snapshotId"));
    }

    @Test
    @DisplayName("row history shows every stored version of a source row")
    void rowHistoryShowsAllVersions() {
        List<Map<String, Object>> history = provenanceService.history(SOURCE_TABLE, "p-100");

        assertEquals(2, history.size(), "one entry per embedding version");
        assertEquals(
                List.of(V1, V2),
                history.stream().map(entry -> String.valueOf(entry.get("modelVersion"))).sorted().toList());
    }
}
