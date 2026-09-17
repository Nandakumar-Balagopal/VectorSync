package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.format.derive.ContentMap;
import io.vectorsync.format.derive.EmbeddingStore;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.format.derive.ProjectionBuilder;
import io.vectorsync.worker.client.DerivationControlClient;
import io.vectorsync.worker.service.embedding.EmbeddingService;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * End-to-end proof for the current three-tier pipeline, which had none.
 *
 * <p>The repository contains two derivation paths. The legacy one is proved end to end by
 * {@code deployment/test-e2e-lifecycle.sh} -- 247 lines asserting on fifteen endpoints and on
 * returned row ids. The current content-addressed path had no equivalent at any level: its unit
 * tests cover {@code ContentMap} collapse, projection publishing, heap bounds and cluster scoping
 * individually, and nothing ran a source table through the whole path and checked that the right
 * row came back. Deciding whether to retire the legacy path is not answerable without this.
 *
 * <p>So this asserts retrieval correctness, not counts. A pipeline can produce exactly the right
 * number of vectors in exactly the right tables and still return the wrong neighbour, and counts are
 * what unit tests here already cover.
 *
 * <p>Queries Tier 2 in process with cosine rather than through Trino. That is the same arithmetic
 * the generated view performs -- {@code SqlViewGenerator} emits one {@code cosine_similarity} call
 * over the same column -- so it tests the data rather than the engine. What it therefore does NOT
 * prove is engine-side partition pruning, which is measured separately under
 * {@code docs/DEMO.md} against real Trino.
 */
@SpringBootTest(properties = {
        "embedding.provider=mock",
        "iceberg.vector.namespace=vector",
        "vectorsync.runner.enabled=false",
        "vectorsync.legacy-sync.enabled=false",
})
class CurrentPipelineEndToEndTest {

    private static final String NAMESPACE = "vector";
    private static final String SOURCE_TABLE = "default.e2e_products";
    private static Path warehouse;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws IOException {
        warehouse = Files.createTempDirectory("vectorsync-e2e-");
        registry.add("iceberg.catalog.warehouse", () -> "file://" + warehouse);
    }

    @Autowired
    DeriveOrchestrationService orchestration;
    @Autowired
    IcebergCatalogService catalogService;
    @Autowired
    EmbeddingService embeddings;
    @Autowired
    ContentHashIndex hashIndex;
    @Autowired
    ReconcileService reconcile;
    @Autowired
    io.vectorsync.worker.service.iceberg.IncrementalChangeDetector detector;

    @MockitoBean
    DerivationControlClient control;

    private static final Schema SOURCE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "description", Types.StringType.get()));

    @BeforeEach
    void reset() {
        hashIndex.invalidate();
        var catalog = catalogService.getCatalog();
        EmbeddingStore.drop(catalog, NAMESPACE);
        ContentMap.drop(catalog, NAMESPACE);
        ProjectionBuilder.drop(catalog, NAMESPACE, spec());
        catalog.dropTable(TableIdentifier.of(Namespace.of("default"), "e2e_products"), true);
        hashIndex.invalidate();

        // The probe answers from the embedding store itself. DeriveOrchestrationService is the
        // queue-free entry point, so unlike MaterializationRunner it does not report written hashes
        // to a control plane -- and mirroring the store is the more faithful stub anyway: the store
        // is the authority that embedded_content exists to record durably.
        when(control.probeEmbedded(anyString(), anyString(), any())).thenAnswer(invocation -> {
            String modelVersion = invocation.getArgument(0);
            String configId = invocation.getArgument(1);
            Collection<String> asked = invocation.getArgument(2);
            Table store = EmbeddingStore.loadIfExists(catalogService.getCatalog(), NAMESPACE);
            if (store == null) {
                return Set.of();
            }
            return EmbeddingStore.findExistingHashes(store, asked, modelVersion, configId);
        });
    }

    private static MaterializationSpec spec() {
        return MaterializationSpec.builder()
                .sourceTable(SOURCE_TABLE)
                .keyColumns(List.of("id"))
                .embeddingColumns(List.of("name", "description"))
                .joinSeparator(" ")
                .chunker("whole")
                .modelName("all-MiniLM-L6-v2")
                .modelRevision("e2e")
                .embeddingVersion("v1")
                .build();
    }

    private static TableConfig config() {
        return TableConfig.builder()
                .tableName(SOURCE_TABLE)
                .embeddingColumns(List.of("name", "description"))
                .modelName("all-MiniLM-L6-v2")
                .embeddingVersion("v1")
                .enabled(true)
                .build();
    }

    /** Appends rows to the source table as a real Iceberg commit, creating it if needed. */
    private void appendSource(List<String[]> rows) {
        var catalog = catalogService.getCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of("default"), "e2e_products");
        if (!catalog.tableExists(identifier)) {
            io.vectorsync.format.catalog.Namespaces.ensureExists(catalog, identifier);
            catalog.createTable(identifier, SOURCE_SCHEMA, PartitionSpec.unpartitioned());
        }
        Table table = catalog.loadTable(identifier);

        List<Record> records = new ArrayList<>(rows.size());
        for (String[] row : rows) {
            GenericRecord record = GenericRecord.create(SOURCE_SCHEMA);
            record.setField("id", row[0]);
            record.setField("name", row[1]);
            record.setField("description", row[2]);
            records.add(record);
        }
        io.vectorsync.format.io.IcebergAppender.append(table, records);
    }

    /** The exact text the pipeline would have embedded for these column values. */
    private static String canonical(String name, String description) {
        // Assembled by the production canonicaliser rather than by hand: the join separator is part
        // of config_id, so a hand-built string is a second implementation that can silently drift
        // from the one that produced the stored vectors.
        return io.vectorsync.format.derive.ContentHash.canonicalText(
                List.of(name, description), " ");
    }

    /** The whole point: top-k over Tier 2, the same cosine the generated view computes. */
    private List<String> topK(String queryText, int k) {
        List<Double> query;
        try {
            // The model name must match what the pipeline embedded the rows under.
            // generateEmbedding(text) delegates with a null model, and the mock seeds its vectors
            // from (text, model) -- so querying through the one-argument overload embeds the query
            // in a different space than the rows and the ranking becomes noise. This is the same
            // failure the repo already records for the real provider: sending the configured
            // default while recording the spec's model made the stored lineage a lie.
            query = embeddings.generateEmbedding(queryText, spec().getModelName());
        } catch (Exception e) {
            throw new IllegalStateException("Could not embed the query", e);
        }

        Table projection = ProjectionBuilder.loadIfExists(
                catalogService.getCatalog(), NAMESPACE, spec());
        assertTrue(projection != null, "no Tier-2 projection exists; the pipeline never published");

        record Scored(String rowId, double score) {
        }
        List<Scored> scored = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(projection).build()) {
            for (Record row : rows) {
                @SuppressWarnings("unchecked")
                List<Float> embedding = (List<Float>) row.getField(Constants.EMBEDDING_COLUMN);
                double dot = 0;
                double normA = 0;
                double normB = 0;
                for (int i = 0; i < embedding.size(); i++) {
                    double a = embedding.get(i);
                    double b = query.get(i);
                    dot += a * b;
                    normA += a * a;
                    normB += b * b;
                }
                double score = dot / (Math.sqrt(normA) * Math.sqrt(normB) + 1e-12);
                scored.add(new Scored(
                        String.valueOf(row.getField(Constants.SOURCE_ROW_ID_COLUMN)), score));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not query the projection", e);
        }

        scored.sort(Comparator.comparingDouble(Scored::score).reversed());
        return scored.stream().limit(k).map(Scored::rowId).toList();
    }

    private long projectionRowCount() {
        Table projection = ProjectionBuilder.loadIfExists(
                catalogService.getCatalog(), NAMESPACE, spec());
        if (projection == null) {
            return 0;
        }
        long count = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(projection).build()) {
            for (Record ignored : rows) {
                count++;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return count;
    }

    @Test
    @DisplayName("a source table flows through all three tiers and returns the right row")
    void backfillThenRetrieve() {
        appendSource(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Lightweight shoe for rocky mountain trails"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"},
                new String[]{"p-300", "Wool Blanket", "Heavy blanket woven from merino wool"},
                new String[]{"p-400", "Desk Lamp", "Adjustable lamp with a warm dimmable bulb"}));

        MaterializationSpec spec = spec();
        var pass = orchestration.backfill(spec, config(), System.currentTimeMillis());
        assertTrue(pass.complete(), "the backfill did not complete: " + pass);

        ProjectionBuilder.Projection projection = ProjectionBuilder.build(
                catalogService.getCatalog(), NAMESPACE, spec);

        assertEquals(4, projection.rowsWritten(), "one projection row per source row");
        assertEquals(0, projection.unresolvedRows(),
                "a mapping with no vector means Tier 1 and Tier 2 disagree");
        assertEquals(4, projectionRowCount());

        // Retrieval correctness, which is what the legacy e2e asserts and no test here did.
        // The mock embedder is a deterministic function of text, so a query equal to a row's
        // canonical text must rank that row first.
        List<String> top = topK(canonical("Trail Runner", "Lightweight shoe for rocky mountain trails"), 1);
        assertEquals(List.of("p-100"), top,
                "the pipeline published vectors but the wrong row ranks first, so the projection's "
                        + "row-to-vector mapping is wrong");
    }

    @Test
    @DisplayName("an append is incremental and leaves earlier rows retrievable")
    void incrementalAppendKeepsEarlierRows() {
        appendSource(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Lightweight shoe for rocky mountain trails"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));
        MaterializationSpec spec = spec();
        var first = orchestration.backfill(spec, config(), System.currentTimeMillis());
        long anchor = first.snapshotId();
        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);

        appendSource(List.<String[]>of(
                new String[]{"p-300", "Wool Blanket", "Heavy blanket woven from merino wool"}));

        var second = orchestration.incremental(spec, config(), anchor, System.currentTimeMillis());
        assertTrue(second.complete(), "the incremental pass did not complete: " + second);
        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);

        assertEquals(3, projectionRowCount(), "the appended row is missing from serving");
        assertEquals(List.of("p-300"),
                topK(canonical("Wool Blanket", "Heavy blanket woven from merino wool"), 1),
                "the newly appended row is not retrievable");
        assertEquals(List.of("p-100"),
                topK(canonical("Trail Runner", "Lightweight shoe for rocky mountain trails"), 1),
                "an incremental pass broke retrieval of a row derived earlier");
    }

    @Test
    @DisplayName("re-deriving an unchanged source costs no inference and changes no answer")
    void rederivingIsFree() {
        appendSource(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Lightweight shoe for rocky mountain trails"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));
        MaterializationSpec spec = spec();

        var first = orchestration.backfill(spec, config(), System.currentTimeMillis());
        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);
        List<String> before = topK(canonical("Espresso Machine", "Pulls a double shot in twenty seconds"), 1);

        var again = orchestration.backfill(spec, config(), System.currentTimeMillis());

        // The headline claim of the whole architecture, asserted end to end rather than per unit.
        assertEquals(0, again.inferenceCalls(),
                "re-deriving an unchanged source called the model again, so the content-addressed "
                        + "claim does not hold through the full path");
        assertEquals(before, topK(canonical("Espresso Machine", "Pulls a double shot in twenty seconds"), 1),
                "a no-op re-derive changed the answer");
    }

    @Test
    @DisplayName("a deleted source row is swept out of serving")
    void deletedRowsAreReconciled() {
        appendSource(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Lightweight shoe for rocky mountain trails"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));
        MaterializationSpec spec = spec();
        var pass = orchestration.backfill(spec, config(), System.currentTimeMillis());
        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);
        assertEquals(List.of("p-100"),
                topK(canonical("Trail Runner", "Lightweight shoe for rocky mountain trails"), 1));

        // Delete p-100 from the source with a real Iceberg delete.
        // Copy-on-write, which is what an engine actually emits for DELETE on a table with no
        // delete files: the data file is dropped and the survivors are rewritten. A row-filter
        // delete on "id = p-100" is refused outright -- Iceberg cannot prove the filter covers whole
        // files, so it raises "Cannot delete file where some, but not all, rows match filter".
        Table source = catalogService.getCatalog()
                .loadTable(TableIdentifier.of(Namespace.of("default"), "e2e_products"));
        source.newDelete()
                .deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue())
                .commit();
        appendSource(List.<String[]>of(
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));

        // This assertion used to be the inverse: it asserted p-100 stayed retrievable, and said
        // that if it ever failed the reconcile had landed and the test should be inverted. It has,
        // so it is.
        var version = detectorVersion();
        ReconcileService.SweepResult swept = reconcile.sweep(
                spec, config(), version[0], version[1], System.currentTimeMillis());

        assertTrue(swept.complete(), "the sweep refused: " + swept.note());
        assertEquals(1, swept.tombstoned(),
                "expected exactly p-100 to be retired, not " + swept.tombstoned()
                        + " rows -- over-tombstoning here retires live serving data");
        assertEquals(1, swept.keysPresent(), "the source should still hold one key");

        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);

        assertEquals(1, projectionRowCount(), "the tombstoned row is still in the projection");
        assertEquals(List.of("p-200"),
                topK(canonical("Trail Runner", "Lightweight shoe for rocky mountain trails"), 1),
                "a query for the deleted row's own text still returns it, so serving is stale");
    }

    /** {@code {snapshotId, sequenceNumber}} of the source's current version. */
    private long[] detectorVersion() {
        var v = detector.currentVersion(config());
        return new long[]{v.snapshotId(), v.sequenceNumber()};
    }

    @Test
    @DisplayName("a sweep over an unchanged source tombstones nothing")
    void sweepIsANoOpWhenNothingVanished() {
        appendSource(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Lightweight shoe for rocky mountain trails"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));
        MaterializationSpec spec = spec();
        orchestration.backfill(spec, config(), System.currentTimeMillis());
        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);

        var version = detectorVersion();
        ReconcileService.SweepResult swept = reconcile.sweep(
                spec, config(), version[0], version[1], System.currentTimeMillis());

        // The safety property that matters more than the feature: a sweep that runs when nothing was
        // deleted must be inert. If this ever tombstones anything, every reconcile silently retires
        // live rows.
        assertTrue(swept.complete());
        assertEquals(0, swept.tombstoned(), "a no-op sweep retired rows: " + swept.note());
        assertEquals(2, projectionRowCount());
        assertEquals(List.of("p-100"),
                topK(canonical("Trail Runner", "Lightweight shoe for rocky mountain trails"), 1));
    }

    @Test
    @DisplayName("a sweep that would retire every row refuses instead")
    void sweepRefusesToEmptyTheScope() {
        appendSource(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Lightweight shoe for rocky mountain trails"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));
        MaterializationSpec spec = spec();
        orchestration.backfill(spec, config(), System.currentTimeMillis());
        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);

        // Every row gone. An empty present-set is far more often a key projection that read nothing
        // -- a renamed column, an unreadable snapshot, a spec aimed at the wrong table -- than a
        // genuinely emptied table, and being wrong retires an entire materialization's serving data.
        Table source = catalogService.getCatalog()
                .loadTable(TableIdentifier.of(Namespace.of("default"), "e2e_products"));
        source.newDelete()
                .deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue())
                .commit();

        var version = detectorVersion();
        ReconcileService.SweepResult swept = reconcile.sweep(
                spec, config(), version[0], version[1], System.currentTimeMillis());

        assertFalse(swept.complete(), "an all-rows sweep was allowed to proceed");
        assertEquals(0, swept.tombstoned());
        assertEquals(2, projectionRowCount(), "serving data was retired despite the refusal");
        assertTrue(swept.note().contains("refusing"), swept.note());
    }

    // ------------------------------------------------------- update / merge behaviour
    //
    // I argued earlier that updates are the tractable case and only pure deletes are missing,
    // because the content map keys by source_row_id and collapses by sequence number -- so HOW an
    // engine expresses an old row version should not reach us. These two tests check that argument
    // against the code instead of leaving it as an argument.

    /** Rewrites the whole table as one overwrite commit, which is what copy-on-write emits. */
    private void copyOnWriteRewrite(List<String[]> survivors) {
        Table source = catalogService.getCatalog()
                .loadTable(TableIdentifier.of(Namespace.of("default"), "e2e_products"));

        List<Record> records = new ArrayList<>(survivors.size());
        for (String[] row : survivors) {
            GenericRecord record = GenericRecord.create(SOURCE_SCHEMA);
            record.setField("id", row[0]);
            record.setField("name", row[1]);
            record.setField("description", row[2]);
            records.add(record);
        }
        List<org.apache.iceberg.DataFile> written =
                io.vectorsync.format.io.IcebergAppender.writeFiles(source, records);

        // One commit that both removes and adds, so snapshot.operation() is "overwrite" -- the
        // shape Spark produces for copy-on-write UPDATE and MERGE.
        org.apache.iceberg.OverwriteFiles overwrite = source.newOverwrite()
                .overwriteByRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue());
        written.forEach(overwrite::addFile);
        overwrite.commit();
    }

    @Test
    @DisplayName("a copy-on-write UPDATE is refused by assess, not attempted")
    void copyOnWriteUpdateIsRefused() {
        appendSource(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Lightweight shoe for rocky mountain trails"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));
        MaterializationSpec spec = spec();
        var first = orchestration.backfill(spec, config(), System.currentTimeMillis());
        long anchor = first.snapshotId();
        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);

        // p-100's description changes. No row disappears -- this is a pure update.
        copyOnWriteRewrite(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Waterproof shoe for alpine scrambling"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));

        var pass = orchestration.incremental(spec, config(), anchor, System.currentTimeMillis());

        // This is the finding. assess() allowlists only APPEND and REPLACE, so an overwrite is
        // refused wholesale -- even though a pure update is the case the content map's row-id
        // keying and sequence collapse are built to absorb. The refusal is over-broad, not wrong:
        // it cannot distinguish an overwrite that only rewrote rows from one that removed some,
        // and guessing the difference would serve deleted rows.
        assertFalse(pass.complete(),
                "if this now passes, assess() has been narrowed to admit row-preserving "
                        + "overwrites and this test should assert the new content is retrievable "
                        + "instead");
        assertEquals(1, pass.filesFailed(), "the refusal should be reported, not silent");
        assertEquals(0, pass.inferenceCalls(), "a refused pass must not embed anything");
    }

    @Test
    @DisplayName("the updated content is derivable once the refusal is bypassed by re-anchoring")
    void updateIsCorrectlyDerivedAfterReanchor() {
        appendSource(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Lightweight shoe for rocky mountain trails"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));
        MaterializationSpec spec = spec();
        orchestration.backfill(spec, config(), System.currentTimeMillis());
        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);

        copyOnWriteRewrite(List.<String[]>of(
                new String[]{"p-100", "Trail Runner", "Waterproof shoe for alpine scrambling"},
                new String[]{"p-200", "Espresso Machine", "Pulls a double shot in twenty seconds"}));

        // Re-anchoring is what DEGRADED recovery does: pin the new snapshot and backfill it. This
        // is the path an operator already has, and it is what the argument about updates predicts
        // will work -- so it is worth knowing whether the DERIVE half is actually correct, quite
        // apart from whether assess() lets the incremental half run.
        var rederived = orchestration.backfill(spec, config(), System.currentTimeMillis());
        assertTrue(rederived.complete(), "the re-anchored backfill did not complete");
        ProjectionBuilder.build(catalogService.getCatalog(), NAMESPACE, spec);

        // The new content wins: the content map collapsed p-100's two versions by sequence number.
        assertEquals(List.of("p-100"),
                topK(canonical("Trail Runner", "Waterproof shoe for alpine scrambling"), 1),
                "the updated content is not retrievable, so collapse did not pick the new version");

        // And the old version is gone from serving rather than coexisting with the new one.
        assertEquals(2, projectionRowCount(),
                "the projection holds both versions of p-100, so the collapse kept a superseded row");

        // p-200 was unchanged, so its content hash was identical and cost no inference. This is the
        // half of MERGE support that already works: the expensive resource is protected even when
        // the whole file is rewritten.
        assertTrue(rederived.inferenceCalls() <= 1,
                "an update to one row of two re-embedded more than the changed row: "
                        + rederived.inferenceCalls() + " calls");
    }
}
