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
        // Must be dropped too. A coverage row surviving into the next test makes its first build
        // report "reused" against an index that test never created, which would turn the invariance
        // assertions below into tautologies that pass for the wrong reason.
        catalog.dropTable(
                TableIdentifier.of(Namespace.of(NAMESPACE), ClusteredIndex.COVERAGE_TABLE_NAME),
                true);
    }

    /** Snapshots on the clustered table: the direct measure of whether a rebuild wrote anything. */
    private int clusteredSnapshotCount() {
        Table clustered = ClusteredIndex.loadOrCreate(
                catalogService.getCatalog(), NAMESPACE, SOURCE_TABLE);
        int snapshots = 0;
        for (org.apache.iceberg.Snapshot ignored : clustered.snapshots()) {
            snapshots++;
        }
        return snapshots;
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
        assertEquals(ClusterIndexService.Freshness.FRESH, status().freshness());

        // The second build does not refit at all now: the coverage digest matched, so it reused.
        // This assertion used to compare cluster sizes to prove the fit was deterministic, which no
        // longer applies on this path because no fit runs -- see reassignmentIsDeterministic for
        // that property, which forces two real fits to test it.
        assertTrue(second.reused(), "an unchanged scope was refitted rather than reused");
        assertEquals(List.of(), second.clusterSizes(), "a reuse reported cluster sizes it did not fit");
    }

    @Test
    @DisplayName("two real fits over identical content assign identically")
    void reassignmentIsDeterministic() {
        seed(0, 1, 2, 3, 4, 5);
        ClusterIndexService.BuildReport first =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        // Force a second genuine fit by discarding the coverage record only. The content, the
        // clustered rows and the centroids are all left alone, so this isolates the fit itself --
        // which must be reproducible, or the recall figures published for one build say nothing
        // about the next.
        catalogService.getCatalog().dropTable(
                TableIdentifier.of(Namespace.of(NAMESPACE), ClusteredIndex.COVERAGE_TABLE_NAME),
                true);

        ClusterIndexService.BuildReport refitted =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        assertFalse(refitted.reused(), "the coverage record was dropped, so this must refit");
        assertEquals(first.clusterSizes(), refitted.clusterSizes(),
                "the fit is deterministic over a sorted input, so an unchanged scope must assign "
                        + "identically; if it does not, the coverage skip is hiding a real "
                        + "difference rather than avoiding redundant work");
        assertEquals(first.coverageDigest(), refitted.coverageDigest());
        assertEquals(6, indexedRows());
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

    // ------------------------------------------------------- coverage invariance
    //
    // These four are the evidence for the one architectural claim here that the prior art does not
    // obviously cover: that the index's identity is a function of the CONTENT it covers rather than
    // of the source snapshot it was built from. The consequence is that layout-only mutations of the
    // source -- compaction, rewrite_data_files, a sort reorganisation, a partition rewrite -- cost
    // nothing at the vector layer, where a per-file or per-snapshot index must rebuild for each one.
    // An index keyed by snapshot id cannot have this property by construction, which is exactly the
    // limitation of the legacy HNSW path in this repo: VectorIds.indexId takes sourceSnapshotId.

    @Test
    @DisplayName("a layout-only rewrite of Tier 1 costs no index rebuild")
    void layoutOnlyRewriteIsFreeForTheIndex() {
        seed(0, 1, 2, 3, 4, 5);
        ClusterIndexService.BuildReport first =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);
        assertFalse(first.reused(), "the first build cannot be a reuse");
        int snapshotsAfterBuild = clusteredSnapshotCount();

        // The same six contents appended again. To this layer that is indistinguishable from a
        // compaction: new data files, a different file count, a different row count, a different
        // scan order -- and an identical set of covered content hashes.
        seed(0, 1, 2, 3, 4, 5);

        ClusterIndexService.BuildReport second =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        assertTrue(second.reused(),
                "the covered content is unchanged, so the index must be reused rather than rebuilt");
        assertEquals(first.coverageDigest(), second.coverageDigest(),
                "the digest moved although no content changed, so it is keyed on something "
                        + "physical -- row count, file identity or scan order -- and the invariance "
                        + "claim does not hold");
        assertEquals(0, second.iterations(), "k-means ran during a reuse");
        assertEquals(snapshotsAfterBuild, clusteredSnapshotCount(),
                "the clustered table gained a snapshot during a reuse, so it was rewritten after "
                        + "all and the rebuild was not actually avoided");
        assertEquals(6, indexedRows(), "the reused index no longer holds the scope's content");
    }

    @Test
    @DisplayName("genuinely new content forces a rebuild")
    void newContentRebuilds() {
        seed(0, 1, 2, 3, 4, 5);
        ClusterIndexService.BuildReport first =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);
        int snapshotsAfterBuild = clusteredSnapshotCount();

        seed(6, 7);
        ClusterIndexService.BuildReport second =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        // The other half of the property: skipping must be driven by content, not by "an index
        // exists". A mechanism that reused unconditionally would pass the test above and serve
        // stale results forever.
        assertFalse(second.reused(), "new content was not indexed");
        assertTrue(!first.coverageDigest().equals(second.coverageDigest()),
                "the digest did not change although two new contents were added");
        assertEquals(8, indexedRows());
        assertTrue(clusteredSnapshotCount() > snapshotsAfterBuild,
                "a rebuild committed nothing");
    }

    @Test
    @DisplayName("a coverage record that outlived its rows does not authorise a reuse")
    void coverageWithoutRowsRebuilds() {
        seed(0, 1, 2, 3, 4, 5);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        // A purge, a manual drop or a half-applied migration can leave the coverage row behind. If
        // a matching digest alone were enough, this would serve an empty index indefinitely.
        ClusteredIndex.drop(catalogService.getCatalog(), NAMESPACE, SOURCE_TABLE);

        ClusterIndexService.BuildReport rebuilt =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        assertFalse(rebuilt.reused(),
                "the digest matched but the rows were gone, and the build was skipped anyway");
        assertEquals(6, indexedRows(), "the index was not actually rebuilt");
    }

    @Test
    @DisplayName("coverage is scoped, so rebuilding one scope does not make a peer look current")
    void coverageIsPerScope() {
        seed(0, 1, 2, 3, 4, 5);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        // Same content, different model version: a distinct scope that has never been built. If
        // coverage were held in a shared structure -- a snapshot summary map, say -- writing one
        // scope's digest could make another read as covered, which is the cross-scope interference
        // that the per-table purge already caused once in this class.
        Table store = EmbeddingStore.loadOrCreate(catalogService.getCatalog(), NAMESPACE);
        EmbeddingStore.append(store, List.of(EmbeddingEntry.builder()
                .contentHash(hash(0))
                .modelVersion("all-MiniLM-L6-v2:v2")
                .configId(CONFIG_ID)
                .embeddingDim(4)
                .embedding(new float[]{1f, 1f, 1f, 0.5f})
                .text("content 0")
                .createdAt(Instant.ofEpochSecond(1_700_000_000L))
                .build()));

        ClusterIndexService.BuildReport other =
                clusterIndex.build(SOURCE_TABLE, "all-MiniLM-L6-v2:v2", CONFIG_ID, 1);

        assertFalse(other.reused(),
                "an unbuilt scope reported as already covered, so coverage is not scope-isolated");
    }

    // ------------------------------------------------------- incremental maintenance
    //
    // Coverage invariance makes layout churn free. These make real change cheap: new content is
    // assigned against the centroids already on disk and appended, so nothing is relabelled and no
    // refit runs. The bound on when that is allowed is the interesting part -- append against stale
    // centroids drifts from what a fresh fit would give, which costs recall rather than correctness.

    @Test
    @DisplayName("added content is appended against existing centroids, without a refit")
    void addedContentIsAppendedNotRefitted() {
        seed(0, 1, 2, 3, 4, 5, 6, 7);
        ClusterIndexService.BuildReport first =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);
        assertFalse(first.incremental(), "the first build has nothing to append to");

        // One new content over eight indexed: comfortably inside the incremental fraction.
        seed(8);
        ClusterIndexService.BuildReport second =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        assertTrue(second.incremental(), "a small addition triggered a full refit");
        assertFalse(second.reused(), "new content must not be reported as already covered");
        assertEquals(0, second.iterations(), "k-means ran during an incremental append");
        assertEquals(9, indexedRows(), "the appended vector is missing, or was double-written");
        assertEquals(ClusterIndexService.Freshness.FRESH, status().freshness());
        assertEquals(CLUSTERS, status().centroids(),
                "an incremental append rewrote the centroids; they must keep describing the "
                        + "assignment the appended rows were actually given");
    }

    @Test
    @DisplayName("an incremental append leaves exactly one row per content, not two")
    void incrementalAppendDoesNotDuplicate() {
        seed(0, 1, 2, 3, 4, 5, 6, 7);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);
        seed(8);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        // The failure this guards is the one ClusteredIndex.append's javadoc warns about: appending
        // after a refit leaves each vector under both its old and its new cluster id, and the probe
        // then scores a mixture of two assignments. Assigning against existing centroids relabels
        // nothing, so it cannot happen -- but only as long as the append path never refits.
        Table clustered = ClusteredIndex.loadOrCreate(
                catalogService.getCatalog(), NAMESPACE, SOURCE_TABLE);
        java.util.Set<String> distinct =
                ClusteredIndex.scopeContentHashes(clustered, MODEL_VERSION, CONFIG_ID);
        assertEquals(9, distinct.size(), "distinct content in the index is wrong");
        assertEquals(distinct.size(), indexedRows(),
                "there are more rows than distinct contents, so a vector is present twice under "
                        + "two cluster ids");
    }

    @Test
    @DisplayName("growth beyond the incremental fraction refits instead of appending")
    void largeGrowthRefits() {
        seed(0, 1, 2, 3);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        // Doubling the scope is far past the 0.25 default. Appending that much against centroids
        // fitted over the original quarter would drift badly, so a refit is the better trade.
        seed(4, 5, 6, 7);
        ClusterIndexService.BuildReport grown =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);

        assertFalse(grown.incremental(), "a doubling of the scope was appended rather than refitted");
        assertTrue(grown.iterations() > 0, "a refit that ran no k-means iterations is not a refit");
        assertEquals(8, indexedRows());
    }

    @Test
    @DisplayName("removed content forces a refit, because a subset delete is not expressible")
    void removalForcesRefit() {
        seed(0, 1, 2, 3, 4, 5);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, CLUSTERS);
        assertEquals(6, indexedRows());

        // Drop Tier 1 and reseed a strict subset. content_hash is not a partition field of the
        // clustered table, so Iceberg cannot delete a subset of a scope's rows by row filter --
        // it refuses a filter it cannot prove covers whole files. So a removal must refit.
        EmbeddingStore.drop(catalogService.getCatalog(), NAMESPACE);
        seed(0, 1, 2);

        ClusterIndexService.BuildReport shrunk =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, 2);

        assertFalse(shrunk.incremental(), "a removal was treated as an append");
        assertFalse(shrunk.reused());
        assertEquals(3, indexedRows(),
                "the removed content is still in the index, so the refit did not replace the scope");
    }

    // ------------------------------------------------------- drift bound
    //
    // The incremental guard originally compared an append against the CURRENT index size, and that
    // is self-defeating under steady growth: a scope growing a few percent per pass never exceeds
    // the fraction, so every pass appends, centroids fitted over the original corpus are never
    // refit, and the assignment drifts without bound. The guard written to prevent drift was
    // exactly what permitted it. It now measures against the size at the last refit, which makes
    // the comparison cumulative by construction.

    @Test
    @DisplayName("repeated small appends eventually force a refit rather than drifting forever")
    void steadyGrowthEventuallyRefits() {
        seed(0, 1, 2, 3);
        ClusterIndexService.BuildReport first =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, 2);
        assertFalse(first.incremental(), "the first build establishes the baseline");

        // One content at a time over a baseline of four. Every single append is 25% or less of the
        // CURRENT size at the moment it happens, so the old guard admitted all of them and never
        // refit. Against the baseline, the second one crosses.
        boolean refitted = false;
        for (int ordinal = 4; ordinal < 9 && !refitted; ordinal++) {
            seed(ordinal);
            ClusterIndexService.BuildReport report =
                    clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, 2);
            if (!report.incremental() && !report.reused()) {
                refitted = true;
            }
        }

        assertTrue(refitted,
                "five successive single-content appends over a baseline of four never triggered a "
                        + "refit, so centroid drift is unbounded and the guard is measuring against "
                        + "the current size rather than the last refit");
    }

    @Test
    @DisplayName("a refit re-establishes the baseline, so growth is measured from there")
    void refitResetsTheBaseline() {
        seed(0, 1, 2, 3);
        clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, 2);

        // Double it, which forces a refit and should reset the baseline to 8.
        seed(4, 5, 6, 7);
        ClusterIndexService.BuildReport refit =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, 2);
        assertFalse(refit.incremental(), "doubling should refit");

        // One more content over a baseline of eight is well inside the fraction, so it must append.
        // If the baseline had not been reset, this would still be measured against four and refit.
        seed(8);
        ClusterIndexService.BuildReport after =
                clusterIndex.build(SOURCE_TABLE, MODEL_VERSION, CONFIG_ID, 2);
        assertTrue(after.incremental(),
                "the baseline was not reset by the refit, so a small append is still being "
                        + "measured against the pre-refit size");
        assertEquals(9, indexedRows());
    }

    @Test
    @DisplayName("cluster count is sized from the corpus, not fixed")
    void clusterCountFollowsCorpusSize() {
        // sqrt(n), the standard IVF sizing heuristic. A fixed count cannot be right across corpus
        // sizes, and being wrong is expensive rather than merely suboptimal: NFCorpus needed 53% of
        // the table at 16 clusters for the quality it reached at 3.8% with 64.
        assertEquals(1, ClusterIndexService.suggestedClusters(0));
        assertEquals(1, ClusterIndexService.suggestedClusters(4));
        assertEquals(10, ClusterIndexService.suggestedClusters(100));
        assertEquals(100, ClusterIndexService.suggestedClusters(10_000));
        // Near the corpus this project actually measured: sqrt(3593) is 60, against the 64 that
        // measured well. Agreement on one corpus is encouraging, not proof it generalises.
        assertEquals(60, ClusterIndexService.suggestedClusters(3_593));
        // Capped, so a very large corpus cannot mint more partitions than a catalog wants to track.
        assertEquals(4096, ClusterIndexService.suggestedClusters(1_000_000_000L));
    }
}
