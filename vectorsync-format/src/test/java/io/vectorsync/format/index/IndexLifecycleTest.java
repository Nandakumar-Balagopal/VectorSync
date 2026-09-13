package io.vectorsync.format.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the promote/rollback primitive against a real Iceberg catalog on the local filesystem.
 *
 * <p>This is the core of the lifecycle thesis: swapping which index version serves production is
 * one commit, rolling back is another, and the whole history stays auditable.
 */
class IndexLifecycleTest {

    private static final String NAMESPACE = "vector";
    private static final String SOURCE_TABLE = "default.products";

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;
    private IndexManifestStore manifest;
    private IndexAliasStore aliases;

    @BeforeEach
    void setUp() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        manifest = new IndexManifestStore(catalog, NAMESPACE);
        aliases = new IndexAliasStore(catalog, NAMESPACE);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    private IndexManifestEntry entry(String indexId,
                                     long snapshotId,
                                     String version,
                                     IndexStatus status,
                                     Map<String, String> evalMetrics) {
        return IndexManifestEntry.builder()
                .indexId(indexId)
                .sourceTable(SOURCE_TABLE)
                .sourceSnapshotId(snapshotId)
                .embeddingModel("all-MiniLM-L6-v2")
                .embeddingVersion(version)
                .indexAlgorithm("hnsw")
                .indexParams(Map.of("m", "16", "efConstruction", "100"))
                .similarityMetric("cosine")
                .dimension(384)
                .indexUri("file://" + warehouse + "/indexes/" + indexId)
                .indexFiles(List.of("_0.cfs", "segments_1"))
                .vectorCount(4)
                .status(status)
                .evalMetrics(evalMetrics)
                .builtAt(Instant.now())
                .build();
    }

    @Test
    @DisplayName("a manifest entry round-trips through Iceberg with its lineage intact")
    void manifestRoundTrip() {
        manifest.put(entry("idx-v1", 100L, "v1", IndexStatus.READY, Map.of("recall@10", "0.72")));

        IndexManifestEntry stored = manifest.findById("idx-v1").orElseThrow();

        assertEquals(SOURCE_TABLE, stored.getSourceTable());
        assertEquals(100L, stored.getSourceSnapshotId());
        assertEquals("all-MiniLM-L6-v2", stored.getEmbeddingModel());
        assertEquals("v1", stored.getEmbeddingVersion());
        assertEquals("all-MiniLM-L6-v2:v1", stored.modelVersion());
        assertEquals("hnsw", stored.getIndexAlgorithm());
        assertEquals("16", stored.getIndexParams().get("m"));
        assertEquals("cosine", stored.getSimilarityMetric());
        assertEquals(384, stored.getDimension());
        assertEquals(List.of("_0.cfs", "segments_1"), stored.getIndexFiles());
        assertEquals(4L, stored.getVectorCount());
        assertEquals(IndexStatus.READY, stored.getStatus());
        assertEquals("0.72", stored.getEvalMetrics().get("recall@10"));
    }

    @Test
    @DisplayName("two embedding versions coexist in the manifest")
    void versionsCoexistInManifest() {
        manifest.put(entry("idx-v1", 100L, "v1", IndexStatus.READY, Map.of()));
        manifest.put(entry("idx-v2", 100L, "v2", IndexStatus.READY, Map.of()));

        assertEquals(2, manifest.findForTable(SOURCE_TABLE).size());
        assertEquals("idx-v1",
                manifest.findLatestReady(SOURCE_TABLE, "all-MiniLM-L6-v2:v1").orElseThrow().getIndexId());
        assertEquals("idx-v2",
                manifest.findLatestReady(SOURCE_TABLE, "all-MiniLM-L6-v2:v2").orElseThrow().getIndexId());
    }

    @Test
    @DisplayName("an incomplete build is never returned as servable")
    void buildingIndexIsNotServable() {
        manifest.put(entry("idx-partial", 100L, "v1", IndexStatus.BUILDING, Map.of()));

        assertTrue(manifest.findLatestReady(SOURCE_TABLE, "all-MiniLM-L6-v2:v1").isEmpty());
        assertFalse(manifest.findById("idx-partial").orElseThrow().isServable());
    }

    @Test
    @DisplayName("a later entry for the same index id supersedes the earlier one")
    void statusUpdateSupersedes() {
        manifest.put(entry("idx-v1", 100L, "v1", IndexStatus.BUILDING, Map.of()));
        manifest.put(entry("idx-v1", 100L, "v1", IndexStatus.READY, Map.of("recall@10", "0.81")));

        assertEquals(1, manifest.findForTable(SOURCE_TABLE).size());
        IndexManifestEntry stored = manifest.findById("idx-v1").orElseThrow();
        assertEquals(IndexStatus.READY, stored.getStatus());
        assertEquals("0.81", stored.getEvalMetrics().get("recall@10"));
    }

    @Test
    @DisplayName("promote, re-promote and roll back all resolve correctly and stay auditable")
    void promoteAndRollback() {
        manifest.put(entry("idx-v1", 100L, "v1", IndexStatus.READY, Map.of("recall@10", "0.72")));
        manifest.put(entry("idx-v2", 100L, "v2", IndexStatus.READY, Map.of("recall@10", "0.81")));

        // nothing promoted yet
        assertTrue(aliases.resolveProduction(SOURCE_TABLE).isEmpty());

        aliases.promote(IndexAliasStore.PRODUCTION, SOURCE_TABLE, "idx-v1", "test", "initial");
        assertEquals("idx-v1", aliases.resolveProduction(SOURCE_TABLE).orElseThrow().getIndexId());

        aliases.promote(IndexAliasStore.PRODUCTION, SOURCE_TABLE, "idx-v2", "test",
                "promote v2 after recall@10 0.81 vs 0.72");
        assertEquals("idx-v2", aliases.resolveProduction(SOURCE_TABLE).orElseThrow().getIndexId());

        // rollback target is the previously served index
        assertEquals("idx-v1", aliases.previous(IndexAliasStore.PRODUCTION, SOURCE_TABLE).orElseThrow().getIndexId());

        aliases.promote(IndexAliasStore.PRODUCTION, SOURCE_TABLE, "idx-v1", "test", "rollback");
        assertEquals("idx-v1", aliases.resolveProduction(SOURCE_TABLE).orElseThrow().getIndexId());

        List<IndexAliasEntry> history = aliases.history(SOURCE_TABLE);
        assertEquals(3, history.size(), "every promotion is retained for audit");
        assertEquals(
                List.of("idx-v1", "idx-v2", "idx-v1"),
                history.stream().map(IndexAliasEntry::getIndexId).toList());
        assertEquals("rollback", history.get(2).getNote());
    }

    @Test
    @DisplayName("aliases are scoped per source table")
    void aliasesAreScopedPerTable() {
        aliases.promote(IndexAliasStore.PRODUCTION, SOURCE_TABLE, "idx-products", "test", null);
        aliases.promote(IndexAliasStore.PRODUCTION, "default.reviews", "idx-reviews", "test", null);

        assertEquals("idx-products", aliases.resolveProduction(SOURCE_TABLE).orElseThrow().getIndexId());
        assertEquals("idx-reviews", aliases.resolveProduction("default.reviews").orElseThrow().getIndexId());
    }

    @Test
    @DisplayName("distinct aliases on one table resolve independently")
    void distinctAliasesCoexist() {
        aliases.promote(IndexAliasStore.PRODUCTION, SOURCE_TABLE, "idx-v1", "test", null);
        aliases.promote("candidate", SOURCE_TABLE, "idx-v2", "test", null);

        assertEquals("idx-v1", aliases.resolveProduction(SOURCE_TABLE).orElseThrow().getIndexId());
        assertEquals("idx-v2", aliases.resolve("candidate", SOURCE_TABLE).orElseThrow().getIndexId());
    }

    @Test
    @DisplayName("an unpromoted table resolves to nothing rather than guessing")
    void noAliasResolvesEmpty() {
        Optional<IndexAliasEntry> resolved = aliases.resolveProduction("default.never_promoted");

        assertTrue(resolved.isEmpty());
    }
}
