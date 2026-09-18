package io.vectorsync.format.maintenance;

import io.vectorsync.format.io.IcebergAppender;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.PartitionUtil;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Keeps the derived tables readable as they age.
 *
 * <p>Nothing did this, and the mechanism is structural rather than speculative. The derived tables
 * commit once per derive pass and each commit writes at least one file per partition it touches, so
 * fragmentation grows in proportion to how incrementally a table is maintained: the tables that get
 * the most benefit from incremental derivation are the ones that end up with the most tiny files,
 * until planning a scan costs more than reading the data. It is the same fragmentation that made
 * {@code hash_prefix} a measured net negative, arriving here from commit frequency rather than from
 * partitioning. {@code avgRecordsPerFile} on the maintenance status endpoint is where it shows up
 * first, well before query latency moves.
 *
 * <p>Three operations, cheapest first, all from {@code iceberg-core} and {@code iceberg-parquet},
 * so none of this needs Spark:
 *
 * <ol>
 *   <li><b>Expire snapshots.</b> Bounds {@code metadata.json}, which every catalog load re-parses --
 *       and this system deliberately runs with {@code cache-enabled=false}, so that is every pass.
 *   <li><b>Rewrite manifests.</b> Coalesces manifest files, which is what scan planning reads.
 *   <li><b>Rewrite data files.</b> The expensive one: reads small files and writes their rows back
 *       as fewer, larger ones.
 * </ol>
 *
 * <p>Compaction is safe for this schema specifically because nothing derived orders by Iceberg
 * sequence numbers. The content map's collapse orders by its own {@code source_sequence_number}
 * <em>column</em>, and every reader of an Iceberg sequence number in this system reads it from the
 * source table. A rewrite changes file and snapshot metadata but no column, so history survives it
 * -- and because a rewrite commits a {@code replace} snapshot rather than an append, an incremental
 * append scan does not see compacted files as new rows either.
 *
 * <p>Everything here is safe to interrupt. Each operation is its own commit, so a process that dies
 * midway leaves a table that is less compacted than intended and never one that is wrong.
 */
@Slf4j
public final class TableMaintenance {

    /**
     * Files at or above this size are left alone.
     *
     * <p>128 MiB is a conventional Parquet target for object storage. The threshold is what makes
     * compaction converge: without it, every run would rewrite every file forever.
     */
    public static final long DEFAULT_TARGET_FILE_BYTES = 128L * 1024 * 1024;

    /**
     * Minimum small files in one rewrite group before it is worth a commit.
     *
     * <p>Rewriting two files into one costs a read, a write and a snapshot to save one file from a
     * scan plan. Below this the commit dominates and compaction makes the table worse.
     */
    public static final int DEFAULT_MIN_FILES_TO_COMPACT = 5;

    /**
     * Heap bound on one rewrite group, in rows.
     *
     * <p>This is the binding limit, not the byte target, and the reason is the payload. A group is
     * materialised as {@link Record} objects before being written back, and a 384-dimension
     * embedding is about 1.5 KiB in Parquet but roughly 9 KiB on heap as a boxed
     * {@code List<Double>} -- a Double per component, plus the reference to it. So Parquet size
     * understates heap by close to six times, and sizing purely by the byte target would be sizing
     * by the wrong number.
     *
     * <p>Measured against a real warehouse: an embedding store held 908,006 records in 534 files
     * averaging 2.4 MiB, so a 128 MiB group would have taken about 53 files and 90,000 records --
     * roughly 830 MB of live objects, against a JVM whose default maximum heap is a quarter of the
     * container's memory. 50,000 rows caps a group near 460 MB and still consolidates that store
     * from 1,700 records per file to 50,000, which is the entire point. Raise it when the payload
     * is text rather than vectors, or when the heap is known to be large.
     */
    public static final int DEFAULT_MAX_ROWS_PER_REWRITE = 50_000;

    /**
     * How recently a table must have been committed to for maintenance to leave it alone.
     *
     * <p>Maintenance and the derive path both commit to the same tables, and Iceberg resolves that
     * with a compare-and-set that one of them loses. Losing is cheap for maintenance -- it does
     * nothing and tries again next tick -- and expensive for the derive path, which reports the
     * batch as unmaterialised and spends one of a work item's three attempts. On a measured run
     * that asymmetry put a materialization into DEGRADED, from housekeeping.
     *
     * <p>So maintenance yields. A table committed to within this window is skipped entirely, which
     * makes the operation opportunistic rather than adversarial: fragmentation is a slow problem and
     * there is no version of it that is worth interrupting a writer for. Thirty seconds is longer
     * than the derive cycle's fifteen, so a continuously active table is simply never compacted by
     * this worker -- correctly, because a table being written every fifteen seconds has a writer
     * whose commits matter more than its file count.
     */
    public static final Duration DEFAULT_QUIET_PERIOD = Duration.ofSeconds(30);

    private TableMaintenance() {
    }

    /**
     * @return true when the table has been committed to recently enough that maintenance should
     *         leave it to its writer
     */
    public static boolean isBusy(Table table, Duration quietPeriod) {
        Snapshot current = table.currentSnapshot();
        if (current == null || quietPeriod == null || quietPeriod.isZero()) {
            return false;
        }
        long sinceMillis = System.currentTimeMillis() - current.timestampMillis();
        return sinceMillis >= 0 && sinceMillis < quietPeriod.toMillis();
    }

    /**
     * @param groupsCompacted rewrite groups committed; one commit each
     * @param filesRemoved    small files replaced
     * @param filesAdded      files written in their place
     * @param bytesRewritten  bytes read and written again; the cost of the operation
     */
    public record CompactionResult(int groupsCompacted,
                                   int filesRemoved,
                                   int filesAdded,
                                   long bytesRewritten,
                                   String note) {

        public static CompactionResult nothingToDo(String note) {
            return new CompactionResult(0, 0, 0, 0L, note);
        }

        public boolean didWork() {
            return groupsCompacted > 0;
        }
    }

    /**
     * Expires snapshots older than {@code retain}, always keeping at least {@code minSnapshots}.
     *
     * <p>Keeping a floor matters: expiring down to one snapshot removes the history that makes an
     * as-of read possible, and an as-of read is how this system reproduces a past vector set. The
     * retention window is a trade against reproducibility, not pure housekeeping.
     */
    public static int expireSnapshots(Table table, Duration retain, int minSnapshots) {
        int before = countSnapshots(table);
        if (before <= minSnapshots) {
            return 0;
        }

        try {
            table.expireSnapshots()
                    .expireOlderThan(Instant.now().minus(retain).toEpochMilli())
                    .retainLast(Math.max(1, minSnapshots))
                    .cleanExpiredFiles(true)
                    .commit();
        } catch (Exception e) {
            log.warn("Could not expire snapshots on {}: {}", table.name(), e.getMessage());
            return 0;
        }

        table.refresh();
        int removed = before - countSnapshots(table);
        if (removed > 0) {
            log.info("Expired {} snapshots on {} ({} remain)", removed, table.name(),
                    countSnapshots(table));
        }
        return removed;
    }

    /** Coalesces manifest files, which is what scan planning reads. Metadata only. */
    public static boolean rewriteManifests(Table table) {
        if (table.currentSnapshot() == null) {
            return false;
        }
        try {
            table.rewriteManifests().rewriteIf(manifest -> true).commit();
            log.info("Rewrote manifests on {}", table.name());
            return true;
        } catch (Exception e) {
            // Nothing is lost by failing: manifests are metadata, and the previous set still
            // describes the same data correctly.
            log.warn("Could not rewrite manifests on {}: {}", table.name(), e.getMessage());
            return false;
        }
    }

    /** Compaction with the default thresholds. */
    public static CompactionResult compactDataFiles(Table table) {
        return compactDataFiles(table, DEFAULT_TARGET_FILE_BYTES, DEFAULT_MIN_FILES_TO_COMPACT,
                DEFAULT_MAX_ROWS_PER_REWRITE);
    }

    public static CompactionResult compactDataFiles(Table table,
                                                    long targetFileBytes,
                                                    int minFilesToCompact) {
        return compactDataFiles(table, targetFileBytes, minFilesToCompact,
                DEFAULT_MAX_ROWS_PER_REWRITE);
    }

    /**
     * Rewrites small data files into larger ones.
     *
     * <p>Groups are built per partition, because the rows of one partition share a partition tuple
     * and can therefore be written back as files Iceberg will accept -- a group spanning partitions
     * would have to invent a tuple per output file. Within a partition, files are accumulated into
     * groups bounded by {@code targetFileBytes} and {@code maxRowsPerRewrite}, and each group is
     * its own commit, which bounds both the heap a rewrite holds and what one failure costs.
     *
     * <p>Files carrying delete files are skipped. Rewriting a file without re-applying its deletes
     * would resurrect deleted rows, and re-applying them is a merge-on-read concern this
     * append-only schema never creates.
     *
     * @param targetFileBytes    files at or above this are left alone, and groups stop growing here
     * @param minFilesToCompact  groups with fewer files than this are skipped
     * @param maxRowsPerRewrite  heap bound; see {@link #DEFAULT_MAX_ROWS_PER_REWRITE} for why this
     *                           rather than the byte target is usually what binds
     */
    public static CompactionResult compactDataFiles(Table table,
                                                    long targetFileBytes,
                                                    int minFilesToCompact,
                                                    int maxRowsPerRewrite) {
        if (table.currentSnapshot() == null) {
            return CompactionResult.nothingToDo("table has no snapshot");
        }
        // The snapshot the file list is planned against. Carried into the commit as the validation
        // anchor, which is both semantically right -- we are replacing the files that snapshot
        // described -- and necessary: a rewrite with no anchor tries to walk the whole ancestry to
        // validate, and on a table whose old snapshots have been expired that walk fails outright.
        long plannedSnapshotId = table.currentSnapshot().snapshotId();

        // Keyed by spec id AND partition tuple. The spec id is not decoration: a table whose
        // partitioning has been changed holds files written under both specs, and their tuples have
        // different arities, so they are not interchangeable in a rewrite. The embedding store is
        // exactly such a table -- hash_prefix was retired from its spec once it measured as a net
        // negative -- and grouping those files together would produce a partition tuple that fits
        // neither spec.
        Map<String, List<FileScanTask>> smallByPartition = new LinkedHashMap<>();
        int withDeletes = 0;
        int largeEnough = 0;
        int smallFiles = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.file().fileSizeInBytes() >= targetFileBytes) {
                    largeEnough++;
                    continue;
                }
                if (!task.deletes().isEmpty()) {
                    withDeletes++;
                    continue;
                }
                smallFiles++;
                smallByPartition
                        .computeIfAbsent(groupKey(task), key -> new ArrayList<>())
                        .add(task);
            }
        } catch (Exception e) {
            return CompactionResult.nothingToDo("could not plan files: " + e.getMessage());
        }

        int groups = 0;
        int removed = 0;
        int added = 0;
        long bytes = 0L;
        int eligible = 0;
        int failed = 0;
        String firstFailure = null;

        for (List<FileScanTask> partitionFiles : smallByPartition.values()) {
            for (List<FileScanTask> group :
                    groupFiles(partitionFiles, targetFileBytes, maxRowsPerRewrite)) {
                if (group.size() < minFilesToCompact) {
                    continue;
                }
                eligible++;
                try {
                    CompactionResult one = rewriteGroup(table, group, plannedSnapshotId);
                    groups += one.groupsCompacted();
                    removed += one.filesRemoved();
                    added += one.filesAdded();
                    bytes += one.bytesRewritten();
                } catch (Exception e) {
                    // Per group, so one unreadable group does not abandon the rest. A failed
                    // rewrite commits nothing, so its files are simply left fragmented.
                    failed++;
                    if (firstFailure == null) {
                        firstFailure = String.valueOf(e.getMessage());
                    }
                    log.warn("Could not compact a group of {}: {}", table.name(), e.getMessage());
                }
            }
        }

        if (groups == 0) {
            // Two very different outcomes used to report the same sentence, and the sentence
            // asserted a reason it had not checked. A table with hundreds of small files whose
            // every rewrite failed is not a table with nothing to do, and reading it as one is how
            // a broken compactor looks healthy.
            if (failed > 0) {
                return CompactionResult.nothingToDo(String.format(
                        "%d eligible group(s), all %d failed to rewrite; first failure: %s",
                        eligible, failed, firstFailure));
            }
            int biggest = smallByPartition.values().stream()
                    .mapToInt(List::size).max().orElse(0);
            return CompactionResult.nothingToDo(String.format(
                    "nothing eligible: %d small file(s) across %d partition-spec group(s), "
                            + "largest holding %d, none reaching the %d-file minimum after "
                            + "grouping; %d already at target size%s",
                    smallFiles, smallByPartition.size(), biggest, minFilesToCompact, largeEnough,
                    withDeletes > 0 ? "; " + withDeletes + " carry delete files" : ""));
        }

        log.info("Compacted {}: {} groups, {} files replaced by {}, {} MiB rewritten",
                table.name(), groups, removed, added, bytes / (1024 * 1024));
        return new CompactionResult(groups, removed, added, bytes,
                groups + " group(s) compacted");
    }

    /**
     * Splits one partition's small files into rewrite groups, each bounded by both the byte target
     * and the row cap so that a group is writable as roughly one file and holdable in heap.
     */
    private static List<List<FileScanTask>> groupFiles(List<FileScanTask> files,
                                                       long targetFileBytes,
                                                       int maxRowsPerRewrite) {
        List<List<FileScanTask>> groups = new ArrayList<>();
        List<FileScanTask> current = new ArrayList<>();
        long currentBytes = 0L;
        long currentRows = 0L;

        for (FileScanTask task : files) {
            long fileBytes = task.file().fileSizeInBytes();
            long fileRows = task.file().recordCount();

            boolean full = !current.isEmpty()
                    && (currentBytes + fileBytes > targetFileBytes
                    || currentRows + fileRows > maxRowsPerRewrite);
            if (full) {
                groups.add(current);
                current = new ArrayList<>();
                currentBytes = 0L;
                currentRows = 0L;
            }

            current.add(task);
            currentBytes += fileBytes;
            currentRows += fileRows;
        }

        if (!current.isEmpty()) {
            groups.add(current);
        }
        return groups;
    }

    private static CompactionResult rewriteGroup(Table table, List<FileScanTask> group,
                                                long plannedSnapshotId) throws Exception {
        Set<DataFile> replacing = new HashSet<>();
        List<Record> rows = new ArrayList<>();
        long bytes = 0L;

        for (FileScanTask task : group) {
            replacing.add(task.file());
            bytes += task.file().fileSizeInBytes();
            rows.addAll(readFile(table, task));
        }

        if (rows.isEmpty()) {
            return CompactionResult.nothingToDo("group held no rows");
        }

        List<DataFile> written = IcebergAppender.writeFiles(table, rows);
        if (written.isEmpty()) {
            return CompactionResult.nothingToDo("nothing written");
        }

        // Replaces a known set of files with another in one snapshot, and fails if any file it is
        // replacing has already gone. An overwrite by row filter would instead delete whatever
        // matched at commit time, silently discarding a concurrent append into the same partition.
        table.newRewrite()
                // Without this the commit validates against the entire ancestry, and a table whose
                // older snapshots have been expired -- which is what the expiry above does on every
                // pass -- cannot produce that ancestry at all. It fails with "cannot determine
                // history between starting snapshot null and the last known ancestor", so
                // compaction silently never worked on any table maintenance had already touched.
                .validateFromSnapshot(plannedSnapshotId)
                .rewriteFiles(replacing, new HashSet<>(written))
                .commit();

        return new CompactionResult(1, replacing.size(), written.size(), bytes, "compacted");
    }

    /**
     * Reads exactly one data file back as generic records.
     *
     * <p>Necessary because {@code IcebergGenerics} reads a table or a filtered subset of it and
     * cannot be pointed at a file set -- using it here would read the whole table per group and,
     * combined with a rewrite that replaces only the group's files, would duplicate every row it
     * read from a file it did not replace.
     *
     * <p>The constants map is the part that is easy to miss. Iceberg is entitled to omit
     * identity-partition columns from the data file and serve them folded from the partition tuple,
     * so a raw Parquet read can return them as null. Passing the tuple as reader constants is what
     * the engine's own generic reader does, and it makes the round trip lossless whether or not the
     * writer stored those columns.
     */
    private static List<Record> readFile(Table table, FileScanTask task) throws Exception {
        Schema schema = table.schema();
        InputFile input = table.io().newInputFile(task.file().path().toString());
        Map<Integer, ?> constants =
                PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);

        List<Record> rows = new ArrayList<>((int) Math.min(task.file().recordCount(), 1 << 16));
        try (CloseableIterable<Record> reader = Parquet.read(input)
                .project(schema)
                .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema, constants))
                .build()) {
            // Deliberately not reuseContainers(): the records outlive the iteration.
            for (Record record : reader) {
                rows.add(record);
            }
        }
        return rows;
    }

    /**
     * Identity of a rewrite group: the partition spec the file was written under, plus its partition
     * tuple.
     *
     * <p>Rendered as a string rather than using {@code StructLike} as a map key. Partition tuples
     * from different specs are different types and comparing them is not meaningful, and the
     * rendered form is also what makes the diagnostic note readable when nothing turns out to be
     * eligible.
     */
    private static String groupKey(FileScanTask task) {
        StructLike partition = task.file().partition();
        StringBuilder key = new StringBuilder().append(task.spec().specId()).append(':');
        for (int field = 0; field < partition.size(); field++) {
            key.append(partition.get(field, Object.class)).append('/');
        }
        return key.toString();
    }

    private static int countSnapshots(Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }
}
