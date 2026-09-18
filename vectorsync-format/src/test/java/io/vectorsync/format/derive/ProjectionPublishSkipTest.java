package io.vectorsync.format.derive;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Publishing the serving projection is an {@code overwriteByRowFilter} over the whole
 * {@code (source_table, model_version)} slice, and the runner asks for it on every cycle in which
 * any file was processed. Deleting and rewriting every projected row because one unrelated file
 * moved is the cost this skip removes, so what these tests assert is mostly negative: that no
 * snapshot appears when nothing changed, and that every state which is not genuinely current still
 * commits.
 *
 * <p>Snapshot <em>ids</em> are the assertion rather than row counts, because a rebuild that
 * produced identical rows would be invisible to a count -- and a needless rebuild is exactly the
 * defect under test.
 *
 * <p>Runs against a real Iceberg table on the local filesystem: the skip is decided from a snapshot
 * summary written by the commit itself, so a fake catalog would test the test's idea of a summary
 * rather than Iceberg's.
 */
class ProjectionPublishSkipTest {

    private static final String NAMESPACE = "vector";
    private static final String SOURCE_TABLE = "default.products";
    private static final int DIMENSION = 4;

    /**
     * The summary keys, restated rather than imported. They are an operator-facing contract read
     * out of table metadata by anything that wants to know how current a projection is, so a test
     * that shared the production constants could rename them both at once and notice nothing.
     */
    private static final String SUMMARY_ROWS = "vectorsync.projection.rows";
    private static final String SUMMARY_VECTORS = "vectorsync.projection.distinct-vectors";
    private static final String SUMMARY_DIM = "vectorsync.projection.embedding-dim";
    private static final String SUMMARY_CONFIG_ID = "vectorsync.projection.config-id";
    private static final String SUMMARY_SEQUENCE = "vectorsync.projection.source-sequence-number";
    private static final String SUMMARY_UNRESOLVED = "vectorsync.projection.unresolved-rows";

    private static final MaterializationSpec SPEC = MaterializationSpec.builder()
            .sourceTable(SOURCE_TABLE)
            .keyColumns(List.of("id"))
            .embeddingColumns(List.of("title", "body"))
            .joinSeparator(" ")
            .modelName("all-MiniLM-L6-v2")
            .modelRevision("e4ce9877")
            .embeddingVersion("v1")
            .normalize(true)
            .build();

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;
    private Table contentMap;
    private Table embeddingStore;

    @BeforeEach
    void setUp() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        contentMap = ContentMap.loadOrCreate(catalog, NAMESPACE);
        embeddingStore = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    /** Real SHA-256 hex, so the store's content-hash bounds and range pruning behave realistically. */
    private static String hash(String text) {
        return ContentHash.of(text);
    }

    /** One live mapping from a row/chunk to content, asserted at a source sequence number. */
    private void mapping(String rowId, int ordinal, String contentHash, long sequence) {
        ContentMap.append(contentMap, List.of(ContentMapEntry.builder()
                .sourceTable(SOURCE_TABLE)
                .sourceRowId(rowId)
                .chunkOrdinal(ordinal)
                .contentHash(contentHash)
                .configId(SPEC.configId())
                .modelVersion(SPEC.modelVersion())
                // Descending as the sequence number ascends, mirroring real Iceberg: snapshot ids
                // are random longs and must never order history.
                .sourceSnapshotId(Long.MAX_VALUE - sequence * 7919L)
                .sourceSequenceNumber(sequence)
                .sourceCommittedAtMillis(1_700_000_000_000L + sequence * 1_000L)
                .createdAt(Instant.ofEpochSecond(1_700_000_000L + sequence))
                .deleted(false)
                .build()));
    }

    /** The vector for a piece of content under this spec's model and configuration. */
    private void vector(String contentHash, float seed) {
        float[] embedding = new float[DIMENSION];
        for (int i = 0; i < DIMENSION; i++) {
            embedding[i] = seed + i;
        }
        EmbeddingStore.append(embeddingStore, List.of(EmbeddingEntry.builder()
                .contentHash(contentHash)
                .modelVersion(SPEC.modelVersion())
                .configId(SPEC.configId())
                .embeddingDim(DIMENSION)
                .embedding(embedding)
                .text("text behind " + contentHash)
                .createdAt(Instant.ofEpochSecond(1_700_000_100L))
                .build()));
    }

    /** Two rows over two distinct vectors, everything resolvable: what a healthy cycle leaves. */
    private void seedTwoResolvableRows() {
        vector(hash("alpha"), 0.1f);
        vector(hash("bravo"), 0.2f);
        mapping("r-1", 0, hash("alpha"), 10L);
        mapping("r-2", 0, hash("bravo"), 20L);
    }

    private ProjectionBuilder.Projection build() {
        return ProjectionBuilder.build(catalog, NAMESPACE, SPEC);
    }

    /** Freshly loaded every time: a stale {@link Table} would not see the commit under test. */
    private Snapshot projectionSnapshot() {
        return catalog.loadTable(ProjectionBuilder.identifier(NAMESPACE, SPEC)).currentSnapshot();
    }

    private long projectionSnapshotId() {
        return projectionSnapshot().snapshotId();
    }

    private Map<String, String> projectionSummary() {
        return projectionSnapshot().summary();
    }

    private int projectionSnapshotCount() {
        int count = 0;
        for (Snapshot ignored : catalog.loadTable(ProjectionBuilder.identifier(NAMESPACE, SPEC)).snapshots()) {
            count++;
        }
        return count;
    }

    private long contentMapWatermark() {
        return ContentMap.latestSequenceNumber(contentMap, SOURCE_TABLE, SPEC.configId());
    }

    @Test
    @DisplayName("a first build commits a snapshot carrying the watermark and counts it covers")
    void firstBuildPublishesTheSummary() {
        seedTwoResolvableRows();

        ProjectionBuilder.Projection projection = build();

        assertEquals(2L, projection.rowsWritten());
        assertEquals(2L, projection.distinctVectors());
        assertEquals(DIMENSION, projection.embeddingDim());
        assertEquals(0L, projection.unresolvedRows());
        assertEquals(ProjectionBuilder.identifier(NAMESPACE, SPEC), projection.tableIdentifier());

        assertNotNull(projectionSnapshot(), "a first build must leave a snapshot to serve from");
        Map<String, String> summary = projectionSummary();
        assertEquals(SPEC.configId(), summary.get(SUMMARY_CONFIG_ID));
        assertEquals("2", summary.get(SUMMARY_ROWS));
        assertEquals("2", summary.get(SUMMARY_VECTORS));
        assertEquals(String.valueOf(DIMENSION), summary.get(SUMMARY_DIM));
        assertEquals("0", summary.get(SUMMARY_UNRESOLVED));
        // Everything resolved, so the covered watermark is the content map's own.
        assertEquals("20", summary.get(SUMMARY_SEQUENCE));
        assertEquals(20L, contentMapWatermark());
    }

    @Test
    @DisplayName("a second build with no content-map change publishes no new snapshot")
    void unchangedContentMapSkipsThePublish() {
        seedTwoResolvableRows();

        ProjectionBuilder.Projection first = build();
        long firstSnapshotId = projectionSnapshotId();

        ProjectionBuilder.Projection second = build();

        assertEquals(firstSnapshotId, projectionSnapshotId(),
                "the content map did not advance, so the slice must not be deleted and rewritten");
        assertEquals(1, projectionSnapshotCount(), "no no-op snapshot either");

        // The caller's logging and metrics read these, so the skip has to reconstruct them from the
        // summary rather than returning zeros.
        assertEquals(first.rowsWritten(), second.rowsWritten());
        assertEquals(first.distinctVectors(), second.distinctVectors());
        assertEquals(first.embeddingDim(), second.embeddingDim());
        assertEquals(0L, second.unresolvedRows());
        assertEquals(first.tableIdentifier(), second.tableIdentifier());
    }

    @Test
    @DisplayName("appending a content map entry makes the next build commit again")
    void anAdvancedContentMapRebuilds() {
        seedTwoResolvableRows();
        build();
        long firstSnapshotId = projectionSnapshotId();

        vector(hash("charlie"), 0.3f);
        mapping("r-3", 0, hash("charlie"), 30L);

        ProjectionBuilder.Projection rebuilt = build();

        assertNotEquals(firstSnapshotId, projectionSnapshotId(),
                "the content map advanced to 30, so the projection owes a new snapshot");
        assertEquals(3L, rebuilt.rowsWritten());
        assertEquals(3L, rebuilt.distinctVectors());
        assertEquals("30", projectionSummary().get(SUMMARY_SEQUENCE));
        assertEquals(30L, contentMapWatermark());
    }

    @Test
    @DisplayName("a projection whose summary recorded unresolved rows rebuilds instead of skipping")
    void unresolvedRowsNeverSkip() {
        String present = hash("present");
        String pending = hash("not-embedded-yet");
        vector(present, 0.1f);
        // Both mappings at source sequence 0, which is the one state where the watermark comparison
        // cannot detect incompleteness on its own: the covered-sequence clamp publishes
        // oldestUnresolved - 1 floored at zero, so here it publishes 0 -- exactly what the content
        // map reports. The unresolved-rows check is the only thing standing between this projection
        // and being declared current, and being declared current is what would strand r-2 forever.
        mapping("r-1", 0, present, 0L);
        mapping("r-2", 0, pending, 0L);

        ProjectionBuilder.Projection incomplete = build();

        assertEquals(1L, incomplete.rowsWritten());
        assertEquals(1L, incomplete.unresolvedRows());
        assertEquals("0", projectionSummary().get(SUMMARY_SEQUENCE));
        assertEquals("1", projectionSummary().get(SUMMARY_UNRESOLVED));
        assertEquals(0L, contentMapWatermark(),
                "the published watermark already equals the content map's, so only unresolved-rows differs");

        long incompleteSnapshotId = projectionSnapshotId();

        // The missing embedding lands. This retry is the whole reason an incomplete projection must
        // never be skipped.
        vector(pending, 0.2f);
        ProjectionBuilder.Projection retried = build();

        assertNotEquals(incompleteSnapshotId, projectionSnapshotId(),
                "an incomplete projection must keep rebuilding until the missing embeddings land");
        assertEquals(2L, retried.rowsWritten());
        assertEquals(0L, retried.unresolvedRows());
        assertEquals("0", projectionSummary().get(SUMMARY_UNRESOLVED));
    }

    @Test
    @DisplayName("a pinned as-of build rebuilds even when the current watermark already matches")
    void pinnedAsOfNeverSkips() {
        seedTwoResolvableRows();
        build();
        long firstSnapshotId = projectionSnapshotId();

        // Same sequence number the unbounded build just published, so every skip condition but the
        // as-of one holds. A pinned build is a deliberate reproducibility request and owes its own
        // snapshot regardless.
        ProjectionBuilder.Projection pinned =
                ProjectionBuilder.buildAsOf(catalog, NAMESPACE, SPEC, 20L);

        assertNotEquals(firstSnapshotId, projectionSnapshotId(),
                "a pinned as-of build must materialize rather than report the existing snapshot");
        assertEquals(2L, pinned.rowsWritten());

        // And a pin at an earlier version really does project that version, which is what makes the
        // never-skip rule load-bearing rather than pedantic.
        long rebuiltSnapshotId = projectionSnapshotId();
        ProjectionBuilder.Projection earlier =
                ProjectionBuilder.buildAsOf(catalog, NAMESPACE, SPEC, 10L);

        assertNotEquals(rebuiltSnapshotId, projectionSnapshotId());
        assertEquals(1L, earlier.rowsWritten(), "only r-1 existed at sequence 10");
        assertEquals("10", projectionSummary().get(SUMMARY_SEQUENCE));
    }

    @Test
    @DisplayName("an unresolved mapping clamps the published watermark below the content map's latest")
    void coveredSequenceNumberIsClampedBelowTheMissingMapping() {
        String resolved = hash("resolved");
        String pending = hash("awaiting-inference");
        vector(resolved, 0.1f);
        mapping("r-1", 0, resolved, 10L);
        // Live in the content map at 20, but the embedding pass has not produced its vector.
        mapping("r-2", 0, pending, 20L);

        ProjectionBuilder.Projection projection = build();

        assertEquals(1L, projection.rowsWritten());
        assertEquals(1L, projection.unresolvedRows());
        assertEquals(20L, contentMapWatermark());
        assertEquals("10", projectionSummary().get(SUMMARY_SEQUENCE),
                "publishing 20 would report the projection current and r-2 would never be picked up");

        long clampedSnapshotId = projectionSnapshotId();

        vector(pending, 0.2f);
        ProjectionBuilder.Projection caughtUp = build();

        assertNotEquals(clampedSnapshotId, projectionSnapshotId(),
                "the clamped watermark is below the content map's, so the next cycle must rebuild");
        assertEquals(2L, caughtUp.rowsWritten());
        assertEquals(0L, caughtUp.unresolvedRows());
        assertEquals("20", projectionSummary().get(SUMMARY_SEQUENCE),
                "now that nothing is missing the projection covers the content map in full");
    }
}
