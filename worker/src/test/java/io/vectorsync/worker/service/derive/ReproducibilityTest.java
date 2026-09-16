package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.derive.ContentMap;
import io.vectorsync.format.derive.ContentMapEntry;
import io.vectorsync.format.derive.EmbeddingStore;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.worker.client.DerivationControlClient;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
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
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * The claim this project leads with: given a source snapshot and a configuration id, the vector set
 * is reproducible -- and an incremental rebuild is therefore checkable against a full one.
 *
 * <p>It was designed for and never tested, which is the weakest position to be in: the property is
 * either true and unproven, or false and undetected. The mechanism that could break it is batching.
 * A full pass sees every row at once and can collapse duplicate content within the batch before
 * embedding; an incremental pass sees the same rows split across several files and must reach the
 * same answer by consulting the durable dedup record instead. If those two paths disagree -- by
 * embedding the same content twice, by assigning a different chunk ordinal, or by writing a
 * different content hash -- then "reproducible" is marketing.
 *
 * <p>The comparison is on committed Iceberg state, not on in-memory results: the deduplicated
 * vectors in the embedding store, and the live row-to-content mapping. Those are the artifacts a
 * reader consumes, so they are the ones that have to match.
 *
 * <p>The control-plane client is stubbed by an in-memory set that behaves like the real durable
 * record: it answers only for content whose work was reported complete. That keeps the test to one
 * process while preserving the one property the batching argument depends on.
 */
@SpringBootTest(properties = {
        "embedding.provider=mock",
        "iceberg.vector.namespace=vector",
        "vectorsync.runner.enabled=false",
        "vectorsync.legacy-sync.enabled=false",
})
class ReproducibilityTest {

    private static final String SOURCE_TABLE = "default.reproducibility";
    private static Path warehouse;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws IOException {
        warehouse = Files.createTempDirectory("vectorsync-repro-");
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
    void stubDurableStore() {
        durable.clear();
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

    /** Mirrors the runner: hashes become durable only after the completion that carried them. */
    private void reportComplete(DeriveResult result) {
        result.written().forEach(written -> durable.add(written.contentHash()));
    }

    private static MaterializationSpec spec() {
        return MaterializationSpec.builder()
                .sourceTable(SOURCE_TABLE)
                .keyColumns(List.of("id"))
                .embeddingColumns(List.of("name", "description"))
                .joinSeparator(" ")
                .chunker("whole")
                .modelName("all-MiniLM-L6-v2")
                .modelRevision("repro")
                .embeddingVersion("v1")
                .build();
    }

    /**
     * Rows with deliberate duplication, so the batching difference actually bites: a full pass
     * collapses these within one batch, an incremental pass has to recognise them across batches.
     */
    private List<Record> sourceRows(int count) {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "description", Types.StringType.get()));

        String[] texts = {
                "Solar lantern with a long runtime",
                "Insulated bottle that keeps drinks cold",
                "Mechanical keyboard with tactile switches",
                "Solar lantern with a long runtime",
                "Waterproof boots for wet trails",
        };

        List<Record> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            GenericRecord row = GenericRecord.create(schema);
            row.setField("id", String.format("r-%03d", i));
            row.setField("name", "Item " + (i % 3));
            row.setField("description", texts[i % texts.length]);
            rows.add(row);
        }
        return rows;
    }

    private void dropDerivedTables() {
        var catalog = catalogService.getCatalog();
        EmbeddingStore.drop(catalog, "vector");
        ContentMap.drop(catalog, "vector");
        hashIndex.invalidate();
        durable.clear();
    }

    /** Every deduplicated vector, as {@code content_hash -> the vector itself}. */
    private Map<String, List<Float>> embeddingStoreState() {
        Table store = EmbeddingStore.loadIfExists(catalogService.getCatalog(), "vector");
        Map<String, List<Float>> state = new LinkedHashMap<>();
        if (store == null) {
            return state;
        }
        try (CloseableIterable<Record> rows = IcebergGenerics.read(store).build()) {
            for (Record row : rows) {
                @SuppressWarnings("unchecked")
                List<Float> embedding = (List<Float>) row.getField(Constants.EMBEDDING_COLUMN);
                state.put(String.valueOf(row.getField(Constants.CONTENT_HASH_COLUMN)),
                        List.copyOf(embedding));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read the embedding store", e);
        }
        return state;
    }

    /** Live row-to-content mapping, as {@code rowId#chunk -> content_hash}. */
    private Map<String, String> contentMapState(MaterializationSpec spec) {
        Table map = ContentMap.loadIfExists(catalogService.getCatalog(), "vector");
        Map<String, String> state = new LinkedHashMap<>();
        if (map == null) {
            return state;
        }
        for (ContentMapEntry entry :
                ContentMap.liveEntries(map, spec.getSourceTable(), spec.configId())) {
            state.put(entry.getSourceRowId() + "#" + entry.getChunkOrdinal(), entry.getContentHash());
        }
        return state;
    }

    @Test
    @DisplayName("an incremental rebuild produces the same vectors as a full one")
    void incrementalMatchesFull() {
        MaterializationSpec spec = spec();
        List<Record> rows = sourceRows(25);

        // --- full pass: every row in one batch -------------------------------
        dropDerivedTables();
        DeriveResult full = deriveService.derive(spec, rows, 100L, 1L, 1_000L);
        reportComplete(full);

        assertTrue(full.complete(), "the full pass must succeed for the comparison to mean anything");
        Map<String, List<Float>> fullStore = embeddingStoreState();
        Map<String, String> fullMap = contentMapState(spec);
        assertFalse(fullStore.isEmpty(), "the full pass wrote no vectors");

        // --- incremental: the same rows, split across five batches -----------
        dropDerivedTables();
        int batch = 5;
        int incrementalInference = 0;
        for (int start = 0; start < rows.size(); start += batch) {
            DeriveResult pass = deriveService.derive(
                    spec, rows.subList(start, Math.min(start + batch, rows.size())),
                    100L, 1L, 1_000L);
            assertTrue(pass.complete(), "incremental pass at offset " + start + " failed");
            // Each batch is only durable once its completion is reported, exactly as the runner
            // does it -- so a later batch can only reuse content an earlier one committed.
            reportComplete(pass);
            incrementalInference += pass.inferenceCalls();
        }

        Map<String, List<Float>> incrementalStore = embeddingStoreState();
        Map<String, String> incrementalMap = contentMapState(spec);

        // --- the claim -------------------------------------------------------
        assertEquals(fullStore.keySet(), incrementalStore.keySet(),
                "the two paths deduplicated to different content");
        for (String hash : fullStore.keySet()) {
            assertEquals(fullStore.get(hash), incrementalStore.get(hash),
                    "vector for " + hash.substring(0, 12) + " differs between full and incremental");
        }
        assertEquals(fullMap, incrementalMap,
                "the row-to-content mapping differs between full and incremental");

        // Cost must match too. Equal outputs reached by embedding the same content repeatedly would
        // satisfy reproducibility while destroying the property the design exists for.
        assertEquals(full.inferenceCalls(), incrementalInference,
                "incremental paid a different amount of inference for an identical result");
        assertEquals(fullStore.size(), full.inferenceCalls(),
                "inference calls should equal distinct content");
    }

    @Test
    @DisplayName("re-deriving unchanged rows costs nothing and changes nothing")
    void rederivingIsIdempotent() {
        MaterializationSpec spec = spec();
        List<Record> rows = sourceRows(15);

        dropDerivedTables();
        DeriveResult first = deriveService.derive(spec, rows, 100L, 1L, 1_000L);
        reportComplete(first);
        Map<String, List<Float>> afterFirst = embeddingStoreState();

        DeriveResult second = deriveService.derive(spec, rows, 100L, 1L, 1_000L);
        reportComplete(second);

        assertEquals(0, second.inferenceCalls(),
                "re-deriving unchanged rows must not call the model again");
        assertEquals(afterFirst, embeddingStoreState(),
                "a no-op pass changed the embedding store");
    }

    @Test
    @DisplayName("the mock embedder is a function of its input, or none of the above means anything")
    void mockEmbedderIsDeterministic() {
        // Guards the guard. This mock previously used a shared unseeded Random, so the same text
        // produced a different vector every call -- which would make the comparisons above pass or
        // fail at random and quietly invalidate every determinism claim in the suite.
        MaterializationSpec spec = spec();
        List<Record> rows = sourceRows(5);

        dropDerivedTables();
        DeriveResult first = deriveService.derive(spec, rows, 100L, 1L, 1_000L);
        reportComplete(first);
        Map<String, List<Float>> once = embeddingStoreState();

        dropDerivedTables();
        DeriveResult again = deriveService.derive(spec, rows, 100L, 1L, 1_000L);
        reportComplete(again);

        assertEquals(once, embeddingStoreState(),
                "the same text embedded twice produced different vectors");
    }
}
