package io.vectorsync.format.maintenance;

import io.vectorsync.format.io.IcebergAppender;
import io.vectorsync.format.maintenance.TableMaintenance.CompactionResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compaction is the one maintenance operation that rewrites data rather than metadata, which makes
 * it the one that can lose or duplicate rows. The first implementation did exactly that: it read the
 * whole table per rewrite group while replacing only that group's files, so every row in a file it
 * did not replace was written a second time and served twice.
 *
 * <p>So these tests are mostly about the round trip being lossless, including the part that is easy
 * to miss -- Iceberg is entitled to omit identity-partition columns from a data file and serve them
 * folded from the partition tuple, so a rewrite that reads files directly has to supply the tuple as
 * reader constants or silently null those columns out. There is also a convergence test, because a
 * compactor that keeps finding work on an already-compacted table would rewrite the warehouse
 * forever on a timer.
 *
 * <p>Against a real catalog on the local filesystem, since every property here is a property of
 * commits, manifests and Parquet files.
 */
class TableMaintenanceTest {

    private static final Namespace NAMESPACE = Namespace.of("vector");
    private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "vectors_test");

    /** Small enough that every file written here counts as fragmented, large enough to group them. */
    private static final long ONE_MIB = 1024L * 1024;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "tenant", Types.StringType.get()),
            Types.NestedField.required(2, "row_id", Types.StringType.get()),
            Types.NestedField.required(3, "text", Types.StringType.get()),
            Types.NestedField.optional(4, "embedding",
                    Types.ListType.ofRequired(5, Types.DoubleType.get())));

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        catalog.createNamespace(NAMESPACE);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    @DisplayName("compaction reduces file count and changes not one row")
    void compactionIsLossless() throws Exception {
        Table table = createTable();
        // One append per row, which is what the derive path does at its worst: a commit per pass
        // leaves a file per pass.
        for (int i = 0; i < 24; i++) {
            IcebergAppender.append(table, List.of(row("acme", "r" + i, "text " + i)));
        }
        table.refresh();

        Set<String> before = rowsOf(table);
        int filesBefore = countFiles(table);
        assertEquals(24, filesBefore, "each append should have written its own file");
        assertEquals(24, before.size());

        CompactionResult result = TableMaintenance.compactDataFiles(table, ONE_MIB, 5);
        table.refresh();

        assertTrue(result.didWork(), "24 tiny files in one partition should be compactable");
        assertTrue(countFiles(table) < filesBefore,
                "compaction should leave fewer files than it found");
        // The property the first implementation broke. Set equality catches loss; the size check
        // catches duplication, which set equality alone would hide.
        assertEquals(before, rowsOf(table), "compaction must preserve every row exactly");
        assertEquals(24, countRecords(table), "compaction must not duplicate rows");
    }

    @Test
    @DisplayName("identity partition columns survive the rewrite")
    void partitionColumnsSurvive() throws Exception {
        Table table = createTable();
        for (int i = 0; i < 12; i++) {
            // Two partitions, so a rewrite that mixed them or lost the tuple is visible in the data
            // rather than only in the file layout.
            IcebergAppender.append(table, List.of(row(i % 2 == 0 ? "acme" : "globex", "r" + i, "t" + i)));
        }
        table.refresh();

        assertTrue(TableMaintenance.compactDataFiles(table, ONE_MIB, 5).didWork());
        table.refresh();

        List<String> tenants = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                Object tenant = row.getField("tenant");
                assertTrue(tenant != null, "identity partition column read back as null");
                tenants.add(tenant.toString());
            }
        }
        assertEquals(12, tenants.size());
        assertEquals(6, tenants.stream().filter("acme"::equals).count());
        assertEquals(6, tenants.stream().filter("globex"::equals).count());

        // And the partition tuple itself still routes: a filter on the partition column must prune
        // rather than return everything.
        assertEquals(6, countRecords(table, "tenant", "acme"));
    }

    @Test
    @DisplayName("a second pass finds nothing left to do")
    void compactionConverges() throws Exception {
        Table table = createTable();
        for (int i = 0; i < 20; i++) {
            IcebergAppender.append(table, List.of(row("acme", "r" + i, "t" + i)));
        }
        table.refresh();

        assertTrue(TableMaintenance.compactDataFiles(table, ONE_MIB, 5).didWork());
        table.refresh();
        int afterFirst = countFiles(table);

        // Convergence is what the size threshold buys. Without it a timer would rewrite the same
        // rows on every tick forever, and the cost would look like steady-state load.
        CompactionResult second = TableMaintenance.compactDataFiles(table, ONE_MIB, 5);
        assertFalse(second.didWork(), "already-compacted table should need no work: " + second.note());
        assertEquals(afterFirst, countFiles(table));
    }

    @Test
    @DisplayName("too few files to be worth a commit is left alone")
    void respectsMinimumFileCount() throws Exception {
        Table table = createTable();
        for (int i = 0; i < 3; i++) {
            IcebergAppender.append(table, List.of(row("acme", "r" + i, "t" + i)));
        }
        table.refresh();

        CompactionResult result = TableMaintenance.compactDataFiles(table, ONE_MIB, 5);
        assertFalse(result.didWork(),
                "three files is below the threshold where a rewrite pays for its commit");
        assertEquals(3, countFiles(table));
    }

    @Test
    @DisplayName("files already at the target size are not rewritten")
    void leavesLargeFilesAlone() throws Exception {
        Table table = createTable();
        for (int i = 0; i < 10; i++) {
            IcebergAppender.append(table, List.of(row("acme", "r" + i, "t" + i)));
        }
        table.refresh();

        // A one-byte target makes every real file "large", which is the same condition a genuinely
        // compacted warehouse is in.
        CompactionResult result = TableMaintenance.compactDataFiles(table, 1L, 5);
        assertFalse(result.didWork());
        assertEquals(10, countFiles(table));
    }

    @Test
    @DisplayName("compaction still works after snapshots have been expired")
    void compactsAfterExpiry() throws Exception {
        Table table = createTable();
        for (int i = 0; i < 24; i++) {
            IcebergAppender.append(table, List.of(row("acme", "r" + i, "text " + i)));
        }
        table.refresh();

        // Expire first, which is the ordinary steady state rather than a corner case: the
        // maintenance pass expires snapshots on every tick, so by the time compaction next runs the
        // ancestry it would need is already gone. A rewrite with no validation anchor then fails
        // with "cannot determine history between starting snapshot null and the last known
        // ancestor", and because each group's failure is caught per group, the pass reports doing
        // nothing rather than failing -- so compaction silently never ran on a maintained table.
        TableMaintenance.expireSnapshots(table, Duration.ZERO, 2);
        table.refresh();
        assertEquals(2, countSnapshots(table), "fixture needs a truncated ancestry");

        Set<String> before = rowsOf(table);
        int filesBefore = countFiles(table);

        CompactionResult result = TableMaintenance.compactDataFiles(table, ONE_MIB, 5);
        table.refresh();

        assertTrue(result.didWork(),
                "compaction must work on a table with expired ancestry: " + result.note());
        assertTrue(countFiles(table) < filesBefore);
        assertEquals(before, rowsOf(table));
        assertEquals(24, countRecords(table));
    }

    @Test
    @DisplayName("a table a writer just committed to reads as busy")
    void busyTableIsDetected() {
        Table table = createTable();
        // An empty table has no writer to yield to, so it is never busy -- otherwise a warehouse
        // that has just been created could never be maintained.
        assertFalse(TableMaintenance.isBusy(table, Duration.ofSeconds(30)),
                "a table with no snapshot has no writer to yield to");

        IcebergAppender.append(table, List.of(row("acme", "r0", "t0")));
        table.refresh();

        // This is the guard that stops housekeeping from taking a commit away from the derive path,
        // which is what put a materialization into DEGRADED on a measured run.
        assertTrue(TableMaintenance.isBusy(table, Duration.ofSeconds(30)),
                "a table committed to a moment ago must be left to its writer");
        assertFalse(TableMaintenance.isBusy(table, Duration.ZERO),
                "a zero quiet period disables the guard");
    }

    @Test
    @DisplayName("an empty table is a no-op, not a failure")
    void emptyTableIsSafe() {
        Table table = createTable();
        assertFalse(TableMaintenance.compactDataFiles(table, ONE_MIB, 5).didWork());
        assertFalse(TableMaintenance.rewriteManifests(table));
        assertEquals(0, TableMaintenance.expireSnapshots(table, Duration.ZERO, 1));
    }

    @Test
    @DisplayName("snapshot expiry honours the floor and keeps the data")
    void expiryKeepsFloorAndData() throws Exception {
        Table table = createTable();
        for (int i = 0; i < 15; i++) {
            IcebergAppender.append(table, List.of(row("acme", "r" + i, "t" + i)));
        }
        table.refresh();
        Set<String> before = rowsOf(table);

        // Duration.ZERO makes every snapshot expirable, so what remains is the floor and nothing
        // else -- the floor is the whole point, because expiring to one snapshot would destroy the
        // history an as-of read needs.
        int expired = TableMaintenance.expireSnapshots(table, Duration.ZERO, 5);
        table.refresh();

        assertTrue(expired > 0, "15 snapshots with a floor of 5 should have expired some");
        assertEquals(5, countSnapshots(table), "expiry must not go below the floor");
        assertEquals(before, rowsOf(table), "expiring snapshots must not touch live data");
    }

    @Test
    @DisplayName("manifests coalesce without disturbing the rows")
    void manifestRewriteIsLossless() throws Exception {
        Table table = createTable();
        for (int i = 0; i < 8; i++) {
            IcebergAppender.append(table, List.of(row("acme", "r" + i, "t" + i)));
        }
        table.refresh();
        Set<String> before = rowsOf(table);

        assertTrue(TableMaintenance.rewriteManifests(table));
        table.refresh();

        assertEquals(before, rowsOf(table));
        assertEquals(8, countFiles(table), "rewriting manifests must not change the data files");
    }

    @Test
    @DisplayName("compaction then expiry leaves a readable table")
    void fullPassLeavesTableReadable() throws Exception {
        Table table = createTable();
        for (int i = 0; i < 30; i++) {
            IcebergAppender.append(table, List.of(row(i % 3 == 0 ? "acme" : "globex", "r" + i, "t" + i)));
        }
        table.refresh();
        Set<String> before = rowsOf(table);

        // The order the service uses: rewrite data, then manifests, then expire. Expiry with a low
        // floor is what actually deletes the replaced files, so this is the pass that would surface
        // a rewrite that left rows pointing at deleted data.
        assertTrue(TableMaintenance.compactDataFiles(table, ONE_MIB, 5).didWork());
        table.refresh();
        assertTrue(TableMaintenance.rewriteManifests(table));
        table.refresh();
        TableMaintenance.expireSnapshots(table, Duration.ZERO, 1);
        table.refresh();

        assertEquals(before, rowsOf(table),
                "every row must still be readable after its original files were deleted");
        assertEquals(30, countRecords(table));
    }

    private Table createTable() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("tenant").build();
        return catalog.createTable(TABLE, SCHEMA, spec);
    }

    private static Record row(String tenant, String rowId, String text) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("tenant", tenant);
        record.setField("row_id", rowId);
        record.setField("text", text);
        record.setField("embedding", List.of(0.1, 0.2, 0.3));
        return record;
    }

    /** Every row rendered as a comparable string, so loss and duplication are both detectable. */
    private static Set<String> rowsOf(Table table) throws Exception {
        Set<String> out = new LinkedHashSet<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                out.add(row.getField("tenant") + "|" + row.getField("row_id") + "|"
                        + row.getField("text") + "|" + row.getField("embedding"));
            }
        }
        return out;
    }

    private static long countRecords(Table table) throws Exception {
        long count = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record ignored : rows) {
                count++;
            }
        }
        return count;
    }

    private static long countRecords(Table table, String column, String value) throws Exception {
        long count = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(org.apache.iceberg.expressions.Expressions.equal(column, value))
                .build()) {
            for (Record ignored : rows) {
                count++;
            }
        }
        return count;
    }

    private static int countFiles(Table table) throws Exception {
        if (table.currentSnapshot() == null) {
            return 0;
        }
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask ignored : tasks) {
                count++;
            }
        }
        return count;
    }

    private static int countSnapshots(Table table) {
        int count = 0;
        for (var ignored : table.snapshots()) {
            count++;
        }
        return count;
    }
}
