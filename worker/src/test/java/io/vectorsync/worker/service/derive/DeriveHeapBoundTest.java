package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.derive.ContentMap;
import io.vectorsync.format.derive.ContentMapEntry;
import io.vectorsync.format.derive.EmbeddingStore;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.worker.client.DerivationControlClient;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * The embed-and-append step is the only part of a derive pass whose peak memory is bounded by
 * nothing the caller controls.
 *
 * <p>The work unit is a source data file and the runner caps how many it leases, but the term that
 * actually dominates is the count of NOVEL CHUNKS, and chunking multiplies rows. 2,000 rows of
 * 32 KiB at a 512-character chunk size is roughly 128k novel chunks, which at 384 dimensions is
 * about a gigabyte once {@code EmbeddingStore.append} has boxed every float into a {@code
 * List<Float>} for the Parquet writer -- with a row cap and a byte cap both satisfied. So the bound
 * has to be expressed in distinct hashes, which is what {@code vectorsync.derive.embed-batch-hashes}
 * does.
 *
 * <p>The risk the bound introduces is that it changes what gets committed. An earlier design split
 * the work across several {@code derive()} calls instead, which looked equivalent and was not:
 * {@code textByHash} would stop collapsing duplicate content across the whole file, and {@code
 * writtenChunkCounts} merges by {@code Math::max} only within one call, so a source row id
 * appearing in two batches would have the later batch tombstone chunks the earlier one wrote live --
 * at the same sequence number, resolved by wall-clock. Serving output would become a function of
 * batch size. These tests exist to hold the line that sub-batching is invisible in the result.
 *
 * <p>{@code embed-batch-hashes} is set to the floor here so the loop actually runs more than once;
 * at the shipped default of 2,000 a test corpus would be a single batch and prove nothing.
 */
@SpringBootTest(properties = {
        "embedding.provider=mock",
        "iceberg.vector.namespace=vector",
        "vectorsync.runner.enabled=false",
        "vectorsync.legacy-sync.enabled=false",
        "vectorsync.derive.embed-batch-hashes=128",
})
class DeriveHeapBoundTest {

    /** Matches the floor in {@code DeriveService.embedBatchSize()}. */
    private static final int BATCH = 128;
    private static final String SOURCE_TABLE = "default.heap_bound";
    private static Path warehouse;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws IOException {
        warehouse = Files.createTempDirectory("vectorsync-heap-");
        registry.add("iceberg.catalog.warehouse", () -> "file://" + warehouse);
    }

    @Autowired
    DeriveService deriveService;
    @Autowired
    IcebergCatalogService catalogService;
    @Autowired
    ContentHashIndex hashIndex;

    @MockitoBean
    DerivationControlClient control;

    /** Stands in for embedded_content: a hash is known only once its work was reported complete. */
    private final Set<String> durable = new HashSet<>();

    @BeforeEach
    void reset() {
        durable.clear();
        hashIndex.invalidate();
        EmbeddingStore.drop(catalogService.getCatalog(), "vector");
        ContentMap.drop(catalogService.getCatalog(), "vector");
        hashIndex.invalidate();

        when(control.probeEmbedded(anyString(), anyString(), any()))
                .thenAnswer(invocation -> {
                    Collection<String> asked = invocation.getArgument(2);
                    Set<String> present = new HashSet<>();
                    for (String hash : asked) {
                        if (durable.contains(hash)) {
                            present.add(hash);
                        }
                    }
                    return present;
                });
    }

    private void reportComplete(DeriveResult result) {
        result.written().forEach(written -> durable.add(written.contentHash()));
    }

    private static MaterializationSpec spec() {
        return MaterializationSpec.builder()
                .sourceTable(SOURCE_TABLE)
                .keyColumns(List.of("id"))
                .embeddingColumns(List.of("description"))
                .joinSeparator(" ")
                .chunker("whole")
                .modelName("all-MiniLM-L6-v2")
                .modelRevision("heap")
                .embeddingVersion("v1")
                .build();
    }

    private static final Schema SOURCE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "description", Types.StringType.get()));

    /**
     * {@code distinct} unique descriptions spread over {@code rows} rows, so the row count and the
     * distinct-content count differ and the assertions can tell which one the code counted.
     */
    private List<Record> sourceRows(int rows, int distinct) {
        List<Record> source = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            GenericRecord row = GenericRecord.create(SOURCE_SCHEMA);
            row.setField("id", String.format("r-%05d", i));
            row.setField("description", "distinct content number " + (i % distinct));
            source.add(row);
        }
        return source;
    }

    private long storeVectorCount() {
        Table store = EmbeddingStore.loadIfExists(catalogService.getCatalog(), "vector");
        if (store == null) {
            return 0;
        }
        long count = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(store)
                .select(Constants.CONTENT_HASH_COLUMN)
                .build()) {
            for (Record ignored : rows) {
                count++;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read the embedding store", e);
        }
        return count;
    }

    private int storeSnapshotCount() {
        Table store = EmbeddingStore.loadIfExists(catalogService.getCatalog(), "vector");
        if (store == null) {
            return 0;
        }
        int snapshots = 0;
        for (Snapshot ignored : store.snapshots()) {
            snapshots++;
        }
        return snapshots;
    }

    @Test
    @DisplayName("a pass larger than one sub-batch still embeds each distinct content exactly once")
    void subBatchingDoesNotDuplicateInference() {
        // 300 distinct contents over 600 rows: more than two sub-batches, and a row count that
        // differs from the content count so a mix-up cannot pass by coincidence.
        DeriveResult result = deriveService.derive(spec(), sourceRows(600, 300), 100L, 1L, 1_000L);
        reportComplete(result);

        assertTrue(result.complete(), "the pass must succeed for the counts to mean anything");
        assertEquals(300, result.inferenceCalls(),
                "inference must equal distinct content, not rows and not sub-batches");
        assertEquals(300, storeVectorCount(),
                "the embedding store holds a different number of vectors than were embedded");
        assertEquals(300, result.written().size(),
                "the hashes reported to the control plane do not cover every vector written");
    }

    @Test
    @DisplayName("the sub-batch bound is real: the store is appended to once per sub-batch")
    void appendsOncePerSubBatch() {
        // The observable proof that the loop ran at all. 300 distinct contents at a batch of 128 is
        // ceil(300/128) = 3 appends, so 3 snapshots. Without the bound this is 1, and the whole
        // 300-vector payload is held in one boxed write.
        DeriveResult result = deriveService.derive(spec(), sourceRows(300, 300), 100L, 1L, 1_000L);
        reportComplete(result);

        assertTrue(result.complete());
        int expected = (300 + BATCH - 1) / BATCH;
        assertEquals(expected, storeSnapshotCount(),
                "expected one embedding-store commit per sub-batch; the loop did not run as "
                        + "configured. Note the inverse risk: a very small batch size turns a pass "
                        + "into thousands of commits, each re-parsing a grown metadata.json, which "
                        + "is why embedBatchSize() has a floor.");
    }

    @Test
    @DisplayName("content repeated across sub-batch boundaries is still embedded once")
    void withinFileDedupSurvivesBatching() {
        // Every description appears twice, and with 300 distinct contents the duplicate of an early
        // content lands far away in the same map -- but still inside one derive() call, so the
        // single textByHash must collapse it before any sub-batch is cut. This is the property that
        // splitting across derive() calls would have broken.
        List<Record> rows = new ArrayList<>();
        for (int pass = 0; pass < 2; pass++) {
            rows.addAll(sourceRows(300, 300));
        }
        // Distinct row ids across the two halves, so this tests content dedup and not row dedup.
        List<Record> renumbered = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            GenericRecord row = GenericRecord.create(SOURCE_SCHEMA);
            row.setField("id", String.format("r-%05d", i));
            row.setField("description", rows.get(i).getField("description"));
            renumbered.add(row);
        }

        DeriveResult result = deriveService.derive(spec(), renumbered, 100L, 1L, 1_000L);
        reportComplete(result);

        assertTrue(result.complete());
        assertEquals(300, result.inferenceCalls(),
                "duplicate content was embedded more than once, so within-file dedup did not "
                        + "survive sub-batching");
        assertEquals(300, storeVectorCount());
    }

    @Test
    @DisplayName("every row is mapped once, so sub-batching did not tombstone a peer batch's chunks")
    void everyRowIsMappedExactlyOnce() {
        MaterializationSpec spec = spec();
        DeriveResult result = deriveService.derive(spec, sourceRows(600, 300), 100L, 1L, 1_000L);
        reportComplete(result);
        assertTrue(result.complete());

        Table contentMap = ContentMap.loadIfExists(catalogService.getCatalog(), "vector");
        List<ContentMapEntry> live =
                ContentMap.liveEntries(contentMap, SOURCE_TABLE, spec.configId());

        assertEquals(600, live.size(),
                "expected one live mapping per source row; a different number means a sub-batch "
                        + "tombstoned chunks another sub-batch wrote live");
        Set<String> rowIds = new HashSet<>();
        live.forEach(entry -> rowIds.add(entry.getSourceRowId()));
        assertEquals(600, rowIds.size(), "a source row is mapped twice");
    }

    @Test
    @DisplayName("re-deriving a multi-sub-batch pass costs no inference")
    void rederivingCostsNothing() {
        MaterializationSpec spec = spec();
        List<Record> rows = sourceRows(600, 300);

        DeriveResult first = deriveService.derive(spec, rows, 100L, 1L, 1_000L);
        reportComplete(first);
        long vectorsAfterFirst = storeVectorCount();

        // Proves every sub-batch's hashes were reported, not just the last one's. If the written
        // list were overwritten per sub-batch rather than accumulated, the hashes from the earlier
        // sub-batches would never become durable and this pass would re-embed them.
        DeriveResult second = deriveService.derive(spec, rows, 101L, 2L, 2_000L);
        reportComplete(second);

        assertEquals(0, second.inferenceCalls(),
                "re-deriving unchanged rows re-embedded content, so some sub-batch's hashes were "
                        + "never durably recorded");
        assertEquals(vectorsAfterFirst, storeVectorCount(),
                "a no-op pass changed the embedding store");
    }
}
