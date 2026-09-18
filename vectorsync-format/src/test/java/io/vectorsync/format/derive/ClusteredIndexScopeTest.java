package io.vectorsync.format.derive;

import io.vectorsync.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The clustered index is one Iceberg table per source table and many scopes inside it: its contents
 * are partitioned {@code identity(model_version), identity(config_id), identity(cluster_id)}, so
 * every {@code (model, config)} pair derived from that source shares the table. Rebuilding one scope
 * used to {@code dropTable(purge = true)} the whole thing, which meant a rebuild of one scope
 * physically deleted another scope's data files while that scope's centroids survived in
 * {@code vector_centroids} -- a probe then ranked against live centroids and found nothing, with no
 * error anywhere. And the drop happened before the replacement rows were written, so a failure in
 * that window left no index and nothing to recover from.
 *
 * <p>These tests pin the properties the replacement has to have: a scope is the unit of replacement,
 * a scope has exactly one centroid generation on disk, and the swap is one commit. They run against
 * a real Iceberg catalog on the local filesystem because all three are properties of commits and
 * partition-level deletes, which nothing in-memory reproduces.
 */
class ClusteredIndexScopeTest {

    private static final String NAMESPACE = "vector";
    private static final String SOURCE_TABLE = "default.products";

    private static final String MODEL_A = "all-MiniLM-L6-v2:v1";
    private static final String CONFIG_A = "0123456789abcdef";
    private static final String MODEL_B = "bge-small-en:v2";
    private static final String CONFIG_B = "fedcba9876543210";

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

    /** Reloaded every time, so no assertion can read a stale copy of the table metadata. */
    private Table clustered() {
        return ClusteredIndex.loadOrCreate(catalog, NAMESPACE, SOURCE_TABLE);
    }

    private Table centroids() {
        return ClusteredIndex.loadCentroidsOrCreate(catalog, NAMESPACE);
    }

    /**
     * Entries for one scope. The vectors are deliberately trivial: nothing here tests clustering
     * quality, only which rows survive which commit.
     */
    private static List<ClusteredIndex.Entry> entries(String modelVersion,
                                                      String configId,
                                                      String prefix,
                                                      int count) {
        List<ClusteredIndex.Entry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(new ClusteredIndex.Entry(
                    prefix + "-" + i, i % 2, modelVersion, configId, 3,
                    new float[]{i, 1f, 0f}, prefix + " text " + i));
        }
        return entries;
    }

    private static List<float[]> centroidSet(float seed, int k) {
        List<float[]> set = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            set.add(new float[]{seed + i, 1f, 0f});
        }
        return set;
    }

    /** Content hashes present in a scope, read from the rows rather than from metadata. */
    private Set<String> hashesIn(Table table, String modelVersion, String configId) {
        Set<String> hashes = new LinkedHashSet<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .build()) {
            for (Record row : rows) {
                hashes.add(String.valueOf(row.getField(Constants.CONTENT_HASH_COLUMN)));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read the clustered index", e);
        }
        return hashes;
    }

    /** Raw rows in a scope, at one snapshot. Counted, not collapsed by cluster id. */
    private long rowsAt(Table table, long snapshotId, String modelVersion, String configId) {
        long count = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .useSnapshot(snapshotId)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .build()) {
            for (Record ignored : rows) {
                count++;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read snapshot " + snapshotId, e);
        }
        return count;
    }

    private static int snapshotCount(Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    @Test
    @DisplayName("replacing one scope leaves another scope in the same clustered table untouched")
    void replacingOneScopeDoesNotTouchAnother() {
        ClusteredIndex.replaceScope(clustered(), MODEL_A, CONFIG_A, entries(MODEL_A, CONFIG_A, "a", 3));
        ClusteredIndex.replaceScope(clustered(), MODEL_B, CONFIG_B, entries(MODEL_B, CONFIG_B, "b", 2));

        // The regression: a per-table drop-and-rewrite here deleted every data file of scope A while
        // writing scope B, so A came back empty and its centroids kept answering for it.
        assertEquals(Set.of("a-0", "a-1", "a-2"), hashesIn(clustered(), MODEL_A, CONFIG_A),
                "writing scope B destroyed scope A's rows");
        assertEquals(Set.of("b-0", "b-1"), hashesIn(clustered(), MODEL_B, CONFIG_B));

        // And in the other direction: rebuilding A must not take B with it.
        ClusteredIndex.replaceScope(clustered(), MODEL_A, CONFIG_A, entries(MODEL_A, CONFIG_A, "a2", 4));

        assertEquals(Set.of("a2-0", "a2-1", "a2-2", "a2-3"), hashesIn(clustered(), MODEL_A, CONFIG_A),
                "scope A kept rows from the generation it replaced");
        assertEquals(Set.of("b-0", "b-1"), hashesIn(clustered(), MODEL_B, CONFIG_B),
                "rebuilding scope A destroyed scope B's rows");
    }

    @Test
    @DisplayName("a second centroid fit leaves exactly one generation on disk, the newer one")
    void replacingCentroidsLeavesOneGeneration() {
        ClusteredIndex.replaceCentroids(centroids(), MODEL_A, CONFIG_A, centroidSet(10f, 3));
        ClusteredIndex.replaceCentroids(centroids(), MODEL_A, CONFIG_A, centroidSet(90f, 3));

        // The regression: appendCentroids only ever appended, so two fits for one scope coexisted.
        // It never surfaced as an error because readCentroids keys by cluster id and the two
        // generations collapsed into k centroids drawn from two different k-means solutions -- the
        // probe then selected clusters that did not match the assignment on disk.
        Table table = centroids();
        long rows = rowsAt(table, table.currentSnapshot().snapshotId(), MODEL_A, CONFIG_A);
        assertEquals(3, rows, "two centroid generations are on disk for one scope");

        List<float[]> read = ClusteredIndex.readCentroids(table, MODEL_A, CONFIG_A);
        assertEquals(3, read.size());
        for (int cluster = 0; cluster < 3; cluster++) {
            assertArrayEquals(new float[]{90f + cluster, 1f, 0f}, read.get(cluster), 0f,
                    "cluster " + cluster + " came from the superseded fit");
        }
    }

    @Test
    @DisplayName("centroid scopes are replaced independently of each other")
    void replacingCentroidsIsScoped() {
        ClusteredIndex.replaceCentroids(centroids(), MODEL_A, CONFIG_A, centroidSet(10f, 3));
        ClusteredIndex.replaceCentroids(centroids(), MODEL_B, CONFIG_B, centroidSet(50f, 2));
        ClusteredIndex.replaceCentroids(centroids(), MODEL_A, CONFIG_A, centroidSet(90f, 3));

        assertEquals(2, ClusteredIndex.readCentroids(centroids(), MODEL_B, CONFIG_B).size(),
                "refitting scope A removed scope B's centroids");
        assertArrayEquals(new float[]{50f, 1f, 0f},
                ClusteredIndex.readCentroids(centroids(), MODEL_B, CONFIG_B).get(0), 0f);
    }

    @Test
    @DisplayName("a scope is never observed empty: each replacement is exactly one snapshot")
    void replacementIsOneSnapshot() {
        ClusteredIndex.replaceScope(clustered(), MODEL_A, CONFIG_A, entries(MODEL_A, CONFIG_A, "a", 3));
        ClusteredIndex.replaceScope(clustered(), MODEL_A, CONFIG_A, entries(MODEL_A, CONFIG_A, "a2", 4));

        Table table = clustered();
        assertEquals(2, snapshotCount(table),
                "each replacement must be a single commit; a delete followed by an append is two, "
                        + "and the state between them is an empty scope that readers can see");

        // Asserted over history rather than by racing a reader thread: if no snapshot in the table's
        // history has an empty scope, then no reader could ever have read one, whatever the timing.
        List<Long> observed = new ArrayList<>();
        for (Snapshot snapshot : table.snapshots()) {
            long rows = rowsAt(table, snapshot.snapshotId(), MODEL_A, CONFIG_A);
            assertTrue(rows > 0,
                    "snapshot " + snapshot.snapshotId() + " exposes the scope as empty");
            observed.add(rows);
        }
        assertEquals(List.of(3L, 4L), observed, "the two generations should be 3 rows then 4");
    }

    @Test
    @DisplayName("replacing a scope that was never written is a pure add, with nothing to delete")
    void replacingAnAbsentScopeIsAPureAdd() {
        Table fresh = clustered();
        assertEquals(0, snapshotCount(fresh), "the table under test must start with no snapshots");

        // Exercises the path where there is no starting snapshot to validate conflicts from: the
        // overwrite still has to commit rather than fail on an empty history.
        ClusteredIndex.replaceScope(fresh, MODEL_A, CONFIG_A, entries(MODEL_A, CONFIG_A, "a", 3));

        Table table = clustered();
        assertNotNull(table.currentSnapshot());
        assertEquals(Set.of("a-0", "a-1", "a-2"), hashesIn(table, MODEL_A, CONFIG_A));
        assertEquals(1, snapshotCount(table));
    }

    @Test
    @DisplayName("an identity-partition column reads back its value rather than null")
    void partitionColumnsAreNotNull() {
        ClusteredIndex.replaceScope(clustered(), MODEL_A, CONFIG_A, entries(MODEL_A, CONFIG_A, "a", 2));

        // withPartition is mandatory when building the DataFile: Iceberg serves identity-partition
        // columns as constants folded from the partition tuple rather than reading them from the
        // data file, so omitting it makes model_version, config_id and cluster_id read back null --
        // and then the next replacement's filter matches nothing.
        try (CloseableIterable<Record> rows = IcebergGenerics.read(clustered()).build()) {
            int seen = 0;
            for (Record row : rows) {
                assertEquals(MODEL_A, String.valueOf(row.getField(Constants.MODEL_VERSION_COLUMN)));
                assertEquals(CONFIG_A, String.valueOf(row.getField(Constants.CONFIG_ID_COLUMN)));
                assertNotNull(row.getField(ClusteredIndex.CLUSTER_ID_COLUMN));
                seen++;
            }
            assertEquals(2, seen);
        } catch (Exception e) {
            throw new IllegalStateException("Could not read the clustered index", e);
        }
    }

    @Test
    @DisplayName("the clustered table for a source is addressable without loading it")
    void identifierIsStable() {
        TableIdentifier identifier = ClusteredIndex.identifier(NAMESPACE, SOURCE_TABLE);

        assertEquals(TableIdentifier.of(Namespace.of(NAMESPACE), "clustered_default_products"),
                identifier);
    }
}
