package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.derive.ClusteredIndex;
import io.vectorsync.format.derive.EmbeddingEntry;
import io.vectorsync.format.derive.EmbeddingStore;
import io.vectorsync.worker.client.DerivationControlClient;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two properties of the Tier-3 build that nothing checked, both of which were false.
 *
 * <p>The first is that the index holds one point per piece of content. Tier 1 is meant to hold
 * exactly one row per {@code (content, model, config)}, but it is an append-only table with no
 * uniqueness enforcement, so two derive batches that both miss the dedup probe write the same key
 * twice. The build clustered whatever Tier 1 handed it, so those duplicates became duplicate index
 * vectors: the cluster they land in is inflated and a probe of that partition reads the same vector
 * repeatedly, which is the exact opposite of the candidate reduction this table exists to
 * demonstrate.
 *
 * <p>The second is that staleness is observable. The index is built by an explicit call and never
 * refreshed, so an index built once serves a shrinking fraction of the corpus while returning
 * confident, exactly scored neighbours. {@code requireFresh} makes that a caller's choice: off by
 * default so an ordinary probe pays nothing, and a loud refusal rather than partial results when it
 * is asked for.
 *
 * <p>Runs the real service against a real Iceberg catalog in a temp warehouse. Tier 1 is seeded
 * directly rather than through the derive pipeline, because the duplicate-key state under test is
 * one the pipeline tries hard not to produce.
 */
@SpringBootTest(properties = {
        "embedding.provider=mock",
        "iceberg.vector.namespace=vector",
        "vectorsync.runner.enabled=false",
        "vectorsync.legacy-sync.enabled=false",
})
class ClusterIndexScopeTest {

    private static final String NAMESPACE = "vector";
    private static final String SOURCE_TABLE = "default.cluster_scope";
    private static final String MODEL_VERSION = "all-MiniLM-L6-v2:v1";
    private static final String CONFIG_ID = "0123456789abcdef";
    private static final int CLUSTERS = 3;

    private static Path warehouse;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws IOException {
        warehouse = Files.createTempDirectory("vectorsync-cluster-scope-");
        registry.add("iceberg.catalog.warehouse", () -> "file://" + warehouse);
    }

    @Autowired
    ClusterIndexService clusterIndex;
    @Autowired
    IcebergCatalogService catalogService;

    /** Not exercised here; mocked so the context does not need a control plane to start. */
    @MockitoBean
    DerivationControlClient control;

    /**
     * Every derived table, dropped between tests. The centroid table has to go too: it is shared by
     * every scope and a leftover generation would answer for a scope whose rows this test removed --
     * which is precisely the inconsistency the production code now prevents.
     */
    @BeforeEach
    void dropDerivedTables() {
        Catalog catalog = catalogService.getCatalog();
        EmbeddingStore.drop(catalog, NAMESPACE);
        ClusteredIndex.drop(catalog, NAMESPACE, SOURCE_TABLE);
        catalog.dropTable(
                TableIdentifier.of(Namespace.of(NAMESPACE), ClusteredIndex.CENTROIDS_TABLE_NAME),
                true);
    }

    /** A 64-hex content hash, so the store's two-character prefix partition behaves as in production. */
    private static String hash(int ordinal) {
        return String.format("%02x", ordinal).repeat(32);
    }

    private static EmbeddingEntry entry(int ordinal) {
        return EmbeddingEntry.builder()
                .contentHash(hash(ordinal))
                .modelVersion(MODEL_VERSION)
                .configId(CONFIG_ID)
                .embeddingDim(4)
                // Spread over the unit sphere enough that k-means has something to separate, and
                // deterministic so the assignment is the same on every run.
                .embedding(new float[]{ordinal + 1f, (ordinal % 3) + 1f, 1f, 0.5f})
                .text("content " + ordinal)
                .createdAt(Instant.ofEpochSecond(1_700_000_000L + ordinal))
                .build();
    }

    /** Appends one commit per call, so duplicates land in separate data files as they really would. */
    private void seed(int... ordinals) {
        Table store = EmbeddingStore.loadOrCreate(catalogService.getCatalog(), NAMESPACE);
        List<EmbeddingEntry> entries = new ArrayList<>(ordinals.length);
        for (int ordinal : ordinals) {
            entries.add(entry(ordinal));
        }
        EmbeddingStore.append(store, entries);
    }

    /** Rows the clustered table holds for the scope, counted from the rows themselves. */
    private long indexedRows() {
        Table clustered = ClusteredIndex.loadOrCreate(
                catalogService.getCatalog(), NAMESPACE, SOURCE_TABLE);
        long count = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(clustered)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, MODEL_VERSION))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, CONFIG_ID))
                .build()) {
            for (Record ignored : rows) {
                count++;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read the clustered index", e);
        }
        return count;
    }

    private ClusterIndexService.ScopeStatus status() {
        List<ClusterIndexService.ScopeStatus> scopes =
                clusterIndex.status(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID);
        assertEquals(1, scopes.size());
        return scopes.get(0);
    }

    private static float[] query() {
        return new float[]{3f, 2f, 1f, 0.5f};
    }

    @Test
    @DisplayName("the index holds one vector per distinct content, not one per Tier-1 row")
    void buildDeduplicatesByContentHash() {
        // Six distinct contents over nine rows: three of them present twice, as two concurrent
        // derive batches that both missed the dedup probe would leave them.
        seed(0, 1, 2, 3, 4, 5);
        seed(1, 3, 5);

        ClusterIndexService.BuildReport report =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        assertEquals(9, report.canonicalRowsRead(), "the seed itself is wrong if this is not nine");
        assertEquals(6, report.vectors(), "duplicate content was clustered more than once");
        assertEquals(6, indexedRows(), "the clustered table holds duplicate vectors");
        assertEquals(6, report.clusterSizes().stream().mapToInt(Integer::intValue).sum(),
                "cluster sizes must account for exactly the vectors written");
    }

    @Test
    @DisplayName("rebuilding an unchanged scope leaves the same number of vectors, not twice as many")
    void rebuildIsIdempotent() {
        seed(0, 1, 2, 3, 4, 5);

        ClusterIndexService.BuildReport first =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);
        long afterFirst = indexedRows();

        ClusterIndexService.BuildReport second =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        // The old build appended a relabelled generation over the previous one, so every vector was
        // present twice under two different cluster ids and the probe scored a mixture.
        assertEquals(first.vectors(), second.vectors());
        assertEquals(afterFirst, indexedRows(), "the rebuild added a second generation");
        assertEquals(first.clusterSizes(), second.clusterSizes(),
                "the fit is deterministic, so an unchanged scope must reassign identically");
        assertEquals(ClusterIndexService.Freshness.FRESH, status().freshness());
    }

    @Test
    @DisplayName("status reports the index's vector count against Tier 1's for the scope")
    void statusReportsStaleness() {
        seed(0, 1, 2, 3, 4, 5);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        ClusterIndexService.ScopeStatus fresh = status();
        assertEquals(6, fresh.indexedVectors());
        assertEquals(6, fresh.canonicalVectors());
        assertEquals(CLUSTERS, fresh.centroids(),
                "more than k centroid rows means two fitted generations are on disk");
        assertEquals(ClusterIndexService.Freshness.FRESH, fresh.freshness());

        // Tier 1 moves on; nothing rebuilds the index.
        seed(6, 7);

        ClusterIndexService.ScopeStatus behind = status();
        assertEquals(6, behind.indexedVectors());
        assertEquals(8, behind.canonicalVectors());
        assertEquals(ClusterIndexService.Freshness.BEHIND, behind.freshness());
    }

    @Test
    @DisplayName("a stale index serves an ordinary probe and refuses one that asked for freshness")
    void requireFreshRefusesAStaleIndex() {
        seed(0, 1, 2, 3, 4, 5);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);
        seed(6, 7);

        // Default off: a probe that did not ask about freshness still answers, from the index as
        // built. This is the benchmark path and it must not pay for the check either.
        ClusteredIndex.ProbeResult served = clusterIndex.probe(
                SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, query(), 3, CLUSTERS, false);
        assertFalse(served.candidates().isEmpty(), "a stale index should still serve a probe");

        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> clusterIndex.probe(
                        SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, query(), 3, CLUSTERS, true));
        assertTrue(refused.getMessage().contains("requireFresh"),
                "the refusal must name what was asked for: " + refused.getMessage());
        assertTrue(refused.getMessage().contains("BEHIND"),
                "the refusal must say why: " + refused.getMessage());

        // And it stops refusing once the index covers the scope again.
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);
        assertFalse(clusterIndex.probe(
                        SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, query(), 3, CLUSTERS, true)
                .candidates().isEmpty());
    }

    @Test
    @DisplayName("a build over a scope with no canonical vectors refuses instead of emptying the index")
    void buildRefusesToEmptyAServingScope() {
        seed(0, 1, 2, 3, 4, 5);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        // A scope that Tier 1 knows nothing about. Committing this would replace six serving vectors
        // with none, which is what dropping the table used to do unconditionally.
        assertThrows(IllegalStateException.class,
                () -> clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, "ffffffffffffffff", CLUSTERS));

        assertEquals(6, indexedRows(), "a refused build must leave the serving scope intact");
    }
}
