package io.vectorsync.format.derive;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tier 1 is one content map and one embedding store for the whole warehouse, so every
 * materialization, configuration and parallel derive thread commits to the same two tables. That is
 * deliberate -- content-addressed dedup only works if all derivations consult one store -- and it
 * makes them the most contended tables in the system while Iceberg's defaults gave them the retry
 * budget of a single-writer table.
 *
 * <p>The consequence was measured: with derivation parallelism at 4, an {@code embedding_store}
 * append lost the compare-and-set three times in five seconds, which is exactly the three attempts
 * a work item gets, and a transient lock conflict became a DEGRADED materialization with 2,000 rows
 * reported as unmaterialised.
 *
 * <p>These tests pin that the budget is actually raised, on new tables and on the warehouses that
 * already exist -- the latter being the ones with the problem.
 */
class SharedTablePropertiesTest {

    private static final String NAMESPACE = "vector";

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    @DisplayName("a freshly created content map carries a raised commit-retry budget")
    void contentMapIsCreatedTuned() {
        Table table = ContentMap.loadOrCreate(catalog, NAMESPACE);

        // Iceberg's default is 4, which is what turned contention into a DEGRADED materialization.
        assertEquals("20", table.properties().get(TableProperties.COMMIT_NUM_RETRIES));
        assertEquals("5000", table.properties().get(TableProperties.COMMIT_MAX_RETRY_WAIT_MS));
        assertTrue(Boolean.parseBoolean(
                table.properties().get(SharedTableProperties.TUNED_PROPERTY)));
    }

    @Test
    @DisplayName("a freshly created embedding store carries a raised commit-retry budget")
    void embeddingStoreIsCreatedTuned() {
        Table table = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);

        assertEquals("20", table.properties().get(TableProperties.COMMIT_NUM_RETRIES));
        assertTrue(Boolean.parseBoolean(
                table.properties().get(SharedTableProperties.TUNED_PROPERTY)));
    }

    @Test
    @DisplayName("loading an already-existing untuned store raises its budget")
    void existingStoreIsTunedOnLoad() {
        // The branch that actually matters, and the one a first attempt at this missed: applying
        // the settings only on the create path leaves every warehouse that predates them untuned,
        // which is exactly the population whose appends are losing the compare-and-set. Stripping
        // the marker reproduces such a warehouse.
        Table fresh = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);
        fresh.updateProperties()
                .remove(SharedTableProperties.TUNED_PROPERTY)
                .set(TableProperties.COMMIT_NUM_RETRIES, "4")
                .commit();

        Table reloaded = EmbeddingStore.loadOrCreate(catalog, NAMESPACE);

        assertEquals("20", reloaded.properties().get(TableProperties.COMMIT_NUM_RETRIES),
                "loading an existing store must raise its commit-retry budget");
        assertTrue(Boolean.parseBoolean(
                reloaded.properties().get(SharedTableProperties.TUNED_PROPERTY)));
    }

    @Test
    @DisplayName("loading an already-existing untuned content map raises its budget")
    void existingContentMapIsTunedOnLoad() {
        Table fresh = ContentMap.loadOrCreate(catalog, NAMESPACE);
        fresh.updateProperties()
                .remove(SharedTableProperties.TUNED_PROPERTY)
                .set(TableProperties.COMMIT_NUM_RETRIES, "4")
                .commit();

        Table reloaded = ContentMap.loadOrCreate(catalog, NAMESPACE);

        assertEquals("20", reloaded.properties().get(TableProperties.COMMIT_NUM_RETRIES));
        assertTrue(Boolean.parseBoolean(
                reloaded.properties().get(SharedTableProperties.TUNED_PROPERTY)));
    }

    @Test
    @DisplayName("an untuned table that already exists is upgraded, and only once")
    void existingTableIsUpgradedIdempotently() {
        // Stands in for a warehouse created before this existed, which is the population that
        // actually hit the contention.
        Table table = catalog.createTable(
                org.apache.iceberg.catalog.TableIdentifier.of(Namespace.of(NAMESPACE), "legacy"),
                ContentMap.schema());
        assertTrue(table.properties().get(SharedTableProperties.TUNED_PROPERTY) == null,
                "fixture should start untuned");
        long snapshotsBefore = table.properties().size();

        SharedTableProperties.ensureTuned(table);
        table.refresh();
        assertEquals("20", table.properties().get(TableProperties.COMMIT_NUM_RETRIES));
        assertTrue(table.properties().size() > snapshotsBefore);

        // Called on every load of the shared tables, so it has to be a property read rather than a
        // commit once the setting is present.
        String metadataBefore = ((org.apache.iceberg.BaseTable) table).operations()
                .current().metadataFileLocation();
        SharedTableProperties.ensureTuned(table);
        table.refresh();
        assertEquals(metadataBefore,
                ((org.apache.iceberg.BaseTable) table).operations()
                        .current().metadataFileLocation(),
                "a second call must not commit");
    }
}
