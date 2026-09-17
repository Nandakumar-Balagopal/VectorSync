package io.vectorsync.format.derive;

import io.vectorsync.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The embedding store's physical layout, and the migration off {@code hash_prefix}.
 *
 * <p>{@code hash_prefix} was a second identity partition field on the reasoning that uniformly
 * distributed content hashes give 256 free buckets. It was measured a net negative on both sides --
 * a realistic probe's prefix set covered every bucket so reads degraded to a full scan (9.3s to
 * 28.2s per table), and writes fragmented into 44,587 files averaging 9.2 KiB -- and has been
 * retired from the spec.
 *
 * <p>Two things about that retirement are dangerous enough to need pinning by test rather than by
 * comment.
 *
 * <p>First, the column must stay in the schema at field id 4 forever. Iceberg 1.5.0 accepts
 * {@code updateSchema().deleteColumn("hash_prefix")} without validating partition specs, and the
 * table is then permanently unusable: historical specs still source the missing field id, so
 * {@code PartitionSpec.partitionType()} throws in both scan planning and {@code newAppend}, and
 * {@code addColumn} cannot repair it. {@link #keepsTheColumnAtItsFieldId} is the guard that stops a
 * future cleanup from bricking the one artifact in this system that costs money to rebuild.
 *
 * <p>Second, the migration has to be in place and idempotent. It runs from
 * {@code loadOrCreate}, which is called once per derive pass rather than once per deployment, so a
 * second {@code removeField} for a name that is no longer a partition field would throw on the hot
 * path.
 */
class EmbeddingStoreLayoutTest {

    private static final String NAMESPACE = "vector";
    private static final String MODEL_VERSION = "all-MiniLM-L6-v2:v1";
    private static final String CONFIG_ID = "0123456789abcdef";

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;

    @BeforeEach
    void openCatalog() {
        catalog = new HadoopCatalog(new Configuration(), "file://" + warehouse);
    }

    @AfterEach
    void closeCatalog() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    /** Real SHA-256-shaped hex, so per-file content-hash bounds and range pruning are realistic. */
    private static String hashFor(int ordinal) {
        return String.format("%064x", ordinal * 0x9E3779B97F4A7C15L + 0x2545F4914F6CDD1DL);
    }

    private static List<EmbeddingEntry> entries(int... ordinals) {
        List<EmbeddingEntry> entries = new ArrayList<>(ordinals.length);
        for (int ordinal : ordinals) {
            entries.add(EmbeddingEntry.builder()
                    .contentHash(hashFor(ordinal))
                    .modelVersion(MODEL_VERSION)
                    .configId(CONFIG_ID)
                    .embedding(new float[] {ordinal, ordinal + 1f, ordinal + 2f})
                    .text("content " + ordinal)
                    .createdAt(Instant.parse("2026-01-01T00:00:00Z"))
                    .build());
        }
        return entries;
    }

    /**
     * Creates the store the way the retired code did, so the migration has something to migrate.
     * Deliberately not a call into production code: the whole point is a table that predates the
     * current spec, and production can no longer produce one.
     */
    private Table createTwoLevelStore() {
        Schema schema = EmbeddingStore.schema();
        PartitionSpec legacySpec = PartitionSpec.builderFor(schema)
                .identity(Constants.MODEL_VERSION_COLUMN)
                .identity(Constants.HASH_PREFIX_COLUMN)
                .build();

        TableIdentifier identifier =
                TableIdentifier.of(Namespace.of(NAMESPACE), Constants.EMBEDDING_STORE_TABLE_NAME);
        catalog.createNamespace(Namespace.of(NAMESPACE));
        return catalog.createTable(identifier, schema, legacySpec,
                Map.of(Constants.FORMAT_VERSION_PROPERTY,
                        String.valueOf(Constants.VECTOR_FORMAT_VERSION)));
    }

    @Test
    @DisplayName("a new store partitions by model version and nothing else")
    void partitionsByModelVersionOnly() {
        Table store = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);

        assertEquals(1, store.spec().fields().size(),
                "a second partition field is back: " + store.spec());
        assertEquals(Constants.MODEL_VERSION_COLUMN, store.spec().fields().get(0).name());
    }

    @Test
    @DisplayName("hash_prefix stays in the schema at field id 4, because deleting it bricks the table")
    void keepsTheColumnAtItsFieldId() {
        Table store = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);

        Types.NestedField field = store.schema().findField(Constants.HASH_PREFIX_COLUMN);
        assertNotNull(field, "hash_prefix was removed from the schema; see this test's javadoc "
                + "-- historical partition specs source its field id and the table becomes unusable");
        assertEquals(4, field.fieldId(), "hash_prefix moved off field id 4, which re-points "
                + "every existing data file");
        assertTrue(field.isRequired(), "hash_prefix is still written on every row");

        assertFalse(store.spec().fields().stream()
                        .anyMatch(f -> Constants.HASH_PREFIX_COLUMN.equals(f.name())),
                "hash_prefix is in the schema, as it must be, but must not be a partition field");
    }

    @Test
    @DisplayName("an existing two-level store is migrated in place, keeping its old files readable")
    void migratesAnExistingStoreWithoutRewriting() {
        Table legacy = createTwoLevelStore();
        assertEquals(2, legacy.spec().fields().size(), "the fixture is not a two-level table");
        EmbeddingStore.append(legacy, entries(1, 2, 3));

        Table migrated = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);

        assertEquals(1, migrated.spec().fields().size(),
                "the hash_prefix partition field was not retired");
        assertEquals(Constants.MODEL_VERSION_COLUMN, migrated.spec().fields().get(0).name());
        // Both specs must survive: the old files are still described by spec 0, and dropping it
        // would orphan them. This is what makes the migration metadata-only -- no data is rewritten
        // and no embedding is recomputed, which matters because Tier 1 is the artifact that cost
        // inference money.
        assertTrue(migrated.specs().size() >= 2,
                "the old spec was discarded, which orphans every file written under it");

        Set<String> found = EmbeddingStore.findExistingHashes(
                migrated, List.of(hashFor(1), hashFor(2), hashFor(3)), MODEL_VERSION, CONFIG_ID);
        assertEquals(3, found.size(),
                "rows written under the old spec are no longer findable after migration: " + found);

        Map<String, EmbeddingEntry> loaded = EmbeddingStore.load(
                migrated, List.of(hashFor(2)), MODEL_VERSION, CONFIG_ID);
        assertEquals(1, loaded.size(), "a pre-migration vector could not be loaded");
        assertEquals(3, loaded.get(hashFor(2)).getEmbedding().length);
    }

    @Test
    @DisplayName("migrating twice is a no-op rather than an error")
    void migrationIsIdempotent() {
        createTwoLevelStore();

        Table once = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);
        int specsAfterFirst = once.specs().size();

        // loadOrCreate runs once per derive pass. A second removeField for a name that is no longer
        // a partition field throws, so without the inspection guard this is where derivation breaks.
        Table twice = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);
        Table thrice = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);

        assertEquals(1, thrice.spec().fields().size());
        assertEquals(specsAfterFirst, twice.specs().size(),
                "a repeated load added a partition spec");
        assertEquals(specsAfterFirst, thrice.specs().size(),
                "a repeated load added a partition spec");
    }

    @Test
    @DisplayName("the range predicate still finds hashes written across many files")
    void rangePredicateFindsHashesAcrossFiles() {
        Table store = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);

        // Separate appends, so the hashes are spread over several data files and the per-file
        // content_hash bounds actually differ. This is the read path that used to lean on
        // hash_prefix partition pruning; if the replacement range predicate were wrong -- inverted
        // bounds, or a range that excludes its endpoints -- this is where it shows.
        EmbeddingStore.append(store, entries(1, 2));
        EmbeddingStore.append(store, entries(3, 4));
        EmbeddingStore.append(store, entries(5));

        List<String> all = List.of(hashFor(1), hashFor(2), hashFor(3), hashFor(4), hashFor(5));
        assertEquals(5, EmbeddingStore.findExistingHashes(store, all, MODEL_VERSION, CONFIG_ID).size(),
                "the range predicate lost rows that are present");

        // A single hash makes the range degenerate (lower == upper), which is the case an
        // exclusive-bound mistake silently breaks.
        assertEquals(Set.of(hashFor(3)),
                EmbeddingStore.findExistingHashes(store, List.of(hashFor(3)), MODEL_VERSION, CONFIG_ID),
                "a one-hash probe failed, so the range bounds are exclusive somewhere");

        assertEquals(5, EmbeddingStore.load(store, all, MODEL_VERSION, CONFIG_ID).size(),
                "the range predicate lost rows on the load path");

        assertTrue(EmbeddingStore.findExistingHashes(
                        store, List.of(hashFor(99)), MODEL_VERSION, CONFIG_ID).isEmpty(),
                "a hash that was never written was reported as present");
    }

    @Test
    @DisplayName("a probe is scoped to its configuration, not just its hash range")
    void probeIsScopedByConfig() {
        Table store = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);
        EmbeddingStore.append(store, entries(1, 2));

        // config_id is not a partition column, so it survives only as a residual predicate against
        // the projected schema. Projecting it away is what once failed with "Cannot find field";
        // dropping it altogether would silently report another configuration's vectors as this
        // one's, which is the more dangerous of the two failures.
        assertTrue(EmbeddingStore.findExistingHashes(
                        store, List.of(hashFor(1)), MODEL_VERSION, "ffffffffffffffff").isEmpty(),
                "a different config_id matched, so the scope predicate is not being applied");
        assertTrue(EmbeddingStore.findExistingHashes(
                        store, List.of(hashFor(1)), "some-other-model:v9", CONFIG_ID).isEmpty(),
                "a different model_version matched, so partition pruning is not being applied");
    }
}
