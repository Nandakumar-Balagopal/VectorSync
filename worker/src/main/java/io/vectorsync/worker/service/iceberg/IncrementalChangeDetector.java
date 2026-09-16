package io.vectorsync.worker.service.iceberg;

import io.vectorsync.common.dto.TableConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Detects what changed in a source table using Iceberg's own file-level metadata, and hands back
 * the data files that have to be read.
 *
 * <p>This replaces diffing two fully materialized snapshots in Java. That approach cost two
 * complete table scans and held both versions of the table in heap to discover a one-row change;
 * its cost was a function of table size rather than change size. Iceberg already records which
 * files a commit added, so the work of finding changes belongs in the manifests: planning a scan
 * reads metadata only, and the only data decoded afterwards is the files that actually changed.
 *
 * <p>The honest limitation, stated up front because the failure mode is silent data loss:
 * {@code newIncrementalAppendScan} reports <em>added data files from append snapshots only</em>.
 * Iceberg's own implementation walks the snapshot range and keeps just the commits whose operation
 * is {@code append}. Everything else is skipped without complaint -- a row-level update or delete,
 * whether written copy-on-write as an {@code overwrite} or merge-on-read as delete files, produces
 * no tasks at all. Consuming that empty result as "nothing changed" would quietly leave stale
 * vectors serving deleted rows forever. So {@link #assess} inspects the snapshot range first and
 * {@link #incrementalWork} refuses to run on a range it cannot faithfully describe.
 */
@Service
@Slf4j
public class IncrementalChangeDetector {

    /**
     * Returned by {@link #currentSnapshotId} and {@link #currentSequenceNumber} when the table has
     * no current snapshot. Iceberg never assigns 0 as a snapshot id, so it is unambiguous as a
     * "nothing has been committed yet" marker.
     */
    public static final long NO_SNAPSHOT = 0L;

    /** Cap on the row-count hint used to presize a record list, so a huge file cannot OOM on the guess alone. */
    private static final int MAX_PRESIZE = 8192;

    private final IcebergTableService icebergTableService;

    public IncrementalChangeDetector(IcebergTableService icebergTableService) {
        this.icebergTableService = icebergTableService;
    }

    /** What a caller is allowed to do with a snapshot range. */
    public enum ScanVerdict {

        /** Every commit in the range is an append (or a data-preserving rewrite); incremental scan is exact. */
        INCREMENTAL_SAFE,

        /**
         * The range contains commits that removed or rewrote rows. An append scan cannot see them,
         * so the affected partitions must be re-enumerated from the end snapshot and compared
         * against the content map.
         */
        RECONCILE_REQUIRED,

        /**
         * The start snapshot is no longer reachable in table history, usually because it expired.
         * Incremental scanning has no starting point and the table must be re-anchored with a
         * backfill.
         */
        REANCHOR_REQUIRED
    }

    /**
     * Verdict on a snapshot range, plus the evidence behind it.
     *
     * <p>{@code affectedPartitions} holds partition paths (the {@code col=value} form Iceberg uses
     * in file layouts) that a reconcile has to cover. An <em>empty</em> set alongside
     * {@link ScanVerdict#RECONCILE_REQUIRED} means the whole table: the table is unpartitioned, the
     * partition spec evolved inside the range so paths from different specs are not comparable, or
     * the manifests could not be read. Narrowing a reconcile on an unknown partition set would skip
     * real changes, so the ambiguous case deliberately costs more rather than less --
     * {@link #wholeTableReconcile()} names that case so a caller does not have to infer it.
     */
    public record ScanAssessment(ScanVerdict verdict,
                                 String reason,
                                 List<Long> destructiveSnapshotIds,
                                 Set<String> affectedPartitions) {

        public boolean incrementalSafe() {
            return verdict == ScanVerdict.INCREMENTAL_SAFE;
        }

        public boolean reconcileRequired() {
            return verdict == ScanVerdict.RECONCILE_REQUIRED;
        }

        public boolean reanchorRequired() {
            return verdict == ScanVerdict.REANCHOR_REQUIRED;
        }

        /** True when a reconcile is needed and cannot be narrowed to a known set of partitions. */
        public boolean wholeTableReconcile() {
            return reconcileRequired() && affectedPartitions.isEmpty();
        }
    }

    /**
     * Classifies the snapshot range {@code (fromSnapshotExclusive, toSnapshotInclusive]} before any
     * work is planned. Reads snapshot metadata only.
     *
     * <p>Call this first on every sync. It is the only place that can tell an append-only range
     * (cheap, exact) apart from one containing deletes (needs a reconcile) or a lost starting point
     * (needs a backfill), and each of those demands a different action from the caller.
     */
    public ScanAssessment assess(TableConfig config, long fromSnapshotExclusive, long toSnapshotInclusive) {
        return assess(loadTable(config), fromSnapshotExclusive, toSnapshotInclusive);
    }

    /**
     * Enumerates every data file live at a pinned snapshot: the full initial materialization, and
     * the recovery path whenever incremental detection cannot be trusted.
     *
     * <p>Pinned deliberately. The anchor is fixed by the caller so that the files enumerated here
     * and the watermark later recorded in the content map describe the same version of the source,
     * even though the table keeps moving while the backfill runs.
     */
    public List<SourceFileWork> backfillWork(TableConfig config, long anchorSnapshotId) {
        return backfillWork(config, anchorSnapshotId, Set.of());
    }

    /**
     * Backfill restricted to a set of partition paths, which is how a
     * {@link ScanVerdict#RECONCILE_REQUIRED} range is repaired without re-reading the whole table.
     *
     * <p>An empty or null {@code partitionPaths} means every partition, matching
     * {@link ScanAssessment#wholeTableReconcile()}. Filtering happens on planned tasks rather than
     * as a scan predicate because a partition path is a rendered string, not an expression; the
     * cost stays at the metadata level either way.
     */
    public List<SourceFileWork> backfillWork(TableConfig config,
                                             long anchorSnapshotId,
                                             Set<String> partitionPaths) {
        Table table = loadTable(config);
        Snapshot anchor = requireSnapshot(table, config, anchorSnapshotId);
        // The configured name, not Iceberg's catalog-qualified table.name(): it is the value keyed
        // on in the content map everywhere else, and two spellings of one table would resolve as
        // two unrelated sources.
        String sourceTable = config.getTableName();
        boolean filtered = partitionPaths != null && !partitionPaths.isEmpty();

        List<SourceFileWork> work = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .useSnapshot(anchorSnapshotId)
                .planFiles()) {

            for (FileScanTask task : tasks) {
                if (filtered && !matchesPartition(table, task.file(), partitionPaths)) {
                    continue;
                }
                work.add(toWork(sourceTable, task, anchor, true));
            }
        } catch (Exception e) {
            throw new IllegalStateException(String.format(
                    "Failed to enumerate data files of %s at anchor snapshot %d",
                    config.getTableName(), anchorSnapshotId), e);
        }

        log.info("Backfill of {} at snapshot {} covers {} data files{}",
                config.getTableName(), anchorSnapshotId, work.size(),
                filtered ? " in " + partitionPaths.size() + " partitions" : "");
        return work;
    }

    /**
     * Data files appended in {@code (fromSnapshotExclusive, toSnapshotInclusive]}.
     *
     * <p>Defined only for a range {@link #assess} called {@link ScanVerdict#INCREMENTAL_SAFE}, and
     * throws otherwise. That is on purpose: on a range containing deletes an append scan returns a
     * plausible, non-empty, and incomplete answer, and a caller that consumed it would drop the
     * deletions with no error anywhere. Making the unsupported case impossible to ignore is the
     * whole point -- decide with {@link #assess} and route to
     * {@link #backfillWork(TableConfig, long, Set)} when it says reconcile.
     */
    public List<SourceFileWork> incrementalWork(TableConfig config,
                                                long fromSnapshotExclusive,
                                                long toSnapshotInclusive) {
        Table table = loadTable(config);
        ScanAssessment assessment = assess(table, fromSnapshotExclusive, toSnapshotInclusive);
        if (!assessment.incrementalSafe()) {
            throw new IllegalStateException(String.format(
                    "Cannot incrementally scan %s from snapshot %d to %d: %s. Call assess() first "
                            + "and route %s to backfillWork().",
                    config.getTableName(), fromSnapshotExclusive, toSnapshotInclusive,
                    assessment.reason(), assessment.verdict()));
        }

        if (fromSnapshotExclusive == toSnapshotInclusive) {
            return List.of();
        }

        Snapshot toSnapshot = requireSnapshot(table, config, toSnapshotInclusive);
        String sourceTable = config.getTableName();
        Map<Long, Snapshot> appendsBySequenceNumber =
                appendsBySequenceNumber(table, fromSnapshotExclusive, toSnapshotInclusive);

        List<SourceFileWork> work = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newIncrementalAppendScan()
                .fromSnapshotExclusive(fromSnapshotExclusive)
                .toSnapshot(toSnapshotInclusive)
                .planFiles()) {

            for (FileScanTask task : tasks) {
                work.add(toWork(sourceTable, task, originOf(task, appendsBySequenceNumber, toSnapshot), false));
            }
        } catch (Exception e) {
            throw new IllegalStateException(String.format(
                    "Failed to plan incremental append scan of %s from snapshot %d to %d",
                    config.getTableName(), fromSnapshotExclusive, toSnapshotInclusive), e);
        }

        log.info("Incremental scan of {} from snapshot {} to {} found {} appended data files",
                config.getTableName(), fromSnapshotExclusive, toSnapshotInclusive, work.size());
        return work;
    }

    /** Current snapshot id of the source table, or {@link #NO_SNAPSHOT} when nothing is committed. */
    public long currentSnapshotId(TableConfig config) {
        Snapshot current = loadTable(config).currentSnapshot();
        return current == null ? NO_SNAPSHOT : current.snapshotId();
    }

    /**
     * Sequence number of the current snapshot, which is the only monotonically increasing version
     * counter Iceberg exposes and therefore the watermark to record for a completed sync.
     *
     * <p>Returns {@link #NO_SNAPSHOT} for an empty table, and also for a format-version-1 table,
     * which pins every sequence number at 0. Ordering derived data from a v1 source therefore has to
     * fall back on commit timestamps, which is exactly the tiebreaker the content map applies.
     */
    public long currentSequenceNumber(TableConfig config) {
        Snapshot current = loadTable(config).currentSnapshot();
        return current == null ? NO_SNAPSHOT : current.sequenceNumber();
    }

    /** A source table version: one snapshot and the sequence number that orders it. */
    public record SourceVersion(long snapshotId, long sequenceNumber) {

        public boolean isEmpty() {
            return snapshotId == NO_SNAPSHOT;
        }
    }

    /**
     * Snapshot id and sequence number read from a single table load.
     *
     * <p>Callers must use this rather than pairing {@link #currentSnapshotId} with
     * {@link #currentSequenceNumber}. Those are two independent catalog loads, so a commit landing
     * between them pairs snapshot S with the sequence number of S+1. That mismatched pair then gets
     * stamped into every content-map row of the pass and recorded as the watermark, which makes the
     * derived data claim to cover a version it never read -- and because the watermark is ahead, the
     * rows it missed are never revisited.
     */
    public SourceVersion currentVersion(TableConfig config) {
        Snapshot current = loadTable(config).currentSnapshot();
        return current == null
                ? new SourceVersion(NO_SNAPSHOT, NO_SNAPSHOT)
                : new SourceVersion(current.snapshotId(), current.sequenceNumber());
    }

    /**
     * Reads one data file, decoding only the requested columns.
     *
     * <p>Iceberg 1.5.0 has no public single-file reader for generic records, so the scan is scoped
     * down to the file instead: plan the pinned snapshot and keep the task whose path matches.
     * Planning touches manifests only, and the resulting {@link FileScanTask} is what makes the read
     * correct rather than merely convenient. It carries the file's partition tuple, from which
     * identity-partition columns are folded in as constants -- those columns are not physically
     * stored in the Parquet file, so opening the file blind reads them back as null -- and it
     * carries the delete files that apply, which are what turn written rows into live rows.
     *
     * <p>The projection is the point of the method. Embedding two columns of a fifty-column table
     * should decode two columns; decoding all of them was the waste this design removes. An empty or
     * null column list reads the full row.
     *
     * <p>Records are returned in the projected schema. Columns added to the table after
     * {@code work.snapshotId()} read back null rather than failing, because Iceberg resolves columns
     * by field id against whatever the file actually contains.
     */
    public List<Record> readFile(TableConfig config, SourceFileWork work, List<String> projectedColumns) {
        Table table = loadTable(config);
        requireSnapshot(table, config, work.snapshotId());
        Schema projection = projection(table.schema(), projectedColumns);

        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .useSnapshot(work.snapshotId())
                .planFiles()) {

            for (FileScanTask task : tasks) {
                if (task.file().path().toString().equals(work.dataFilePath())) {
                    // planFiles() yields exactly one task per data file -- splitting only happens in
                    // planTasks() -- so the first match is the whole file and there is nothing to
                    // gain by planning the remaining manifests.
                    return readTask(table, task, projection);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(String.format(
                    "Failed to read data file %s of %s at snapshot %d",
                    work.dataFilePath(), config.getTableName(), work.snapshotId()), e);
        }

        throw new IllegalStateException(String.format(
                "Data file %s is not present in %s at snapshot %d; the work item is stale and its "
                        + "source range must be re-planned",
                work.dataFilePath(), config.getTableName(), work.snapshotId()));
    }

    private ScanAssessment assess(Table table, long fromSnapshotExclusive, long toSnapshotInclusive) {
        if (table.snapshot(toSnapshotInclusive) == null) {
            return new ScanAssessment(ScanVerdict.REANCHOR_REQUIRED, String.format(
                    "end snapshot %d is not in the history of %s", toSnapshotInclusive, table.name()),
                    List.of(), Set.of());
        }

        if (fromSnapshotExclusive == toSnapshotInclusive) {
            return new ScanAssessment(ScanVerdict.INCREMENTAL_SAFE,
                    "no commits since the last sync", List.of(), Set.of());
        }

        // Matches the check Iceberg applies inside fromSnapshotExclusive: the start snapshot itself
        // may already be expired, but it must still appear as the parent of some retained ancestor
        // of the end snapshot. Anything else means history has been cut or has diverged, and an
        // incremental scan would silently start from the wrong place.
        if (!SnapshotUtil.isParentAncestorOf(table, toSnapshotInclusive, fromSnapshotExclusive)) {
            return new ScanAssessment(ScanVerdict.REANCHOR_REQUIRED, String.format(
                    "snapshot %d is no longer an ancestor of %d in %s (expired or rewritten history)",
                    fromSnapshotExclusive, toSnapshotInclusive, table.name()),
                    List.of(), Set.of());
        }

        List<Long> destructive = new ArrayList<>();
        Set<String> affected = new LinkedHashSet<>();
        boolean partitionsResolved = true;

        for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(
                table, toSnapshotInclusive, fromSnapshotExclusive)) {
            if (!isDestructive(table, snapshot)) {
                continue;
            }
            destructive.add(snapshot.snapshotId());
            partitionsResolved &= collectAffectedPartitions(table, snapshot, affected);
        }

        if (destructive.isEmpty()) {
            return new ScanAssessment(ScanVerdict.INCREMENTAL_SAFE,
                    "every commit in range is an append or a data-preserving rewrite",
                    List.of(), Set.of());
        }

        return new ScanAssessment(ScanVerdict.RECONCILE_REQUIRED, String.format(
                "%d commit(s) in range changed rows in a way an append scan cannot see",
                destructive.size()),
                List.copyOf(destructive),
                partitionsResolved ? Set.copyOf(affected) : Set.of());
    }

    /**
     * Whether a commit changed rows in a way an append scan cannot report.
     *
     * <p>The test is an allowlist rather than a list of known-bad operations, and it mirrors the
     * append scan's own allowlist: Iceberg keeps the commits whose operation is exactly
     * {@code append} and drops every other commit from the plan without complaint. So anything this
     * method does not positively recognize is invisible to the scan and has to count as destructive.
     * That covers an operation this version of Iceberg has no constant for, and a {@code null}
     * operation -- legal on a snapshot written without a summary, and the case where Iceberg's own
     * {@code appendsBetween} throws a bare NullPointerException.
     *
     * <p>{@code replace} is the single exception: it is a compaction that rewrites files with
     * identical contents, so the commit the scan skips contains no rows that were not already
     * reported. An {@code append} commit cannot carry delete files, but the delete manifests written
     * by a commit are checked anyway, because that is the property that actually matters and it
     * costs one manifest-list read.
     */
    private boolean isDestructive(Table table, Snapshot snapshot) {
        String operation = snapshot.operation();
        if (!DataOperations.APPEND.equals(operation) && !DataOperations.REPLACE.equals(operation)) {
            return true;
        }

        try {
            for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
                Long writtenBy = manifest.snapshotId();
                if (writtenBy != null && writtenBy == snapshot.snapshotId()) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            // Unreadable metadata is treated as destructive. A reconcile that turns out to have been
            // unnecessary wastes compute; assuming "append only" on unknown metadata loses data.
            log.warn("Cannot inspect delete manifests of snapshot {} on {}, assuming it changed rows: {}",
                    snapshot.snapshotId(), table.name(), e.getMessage());
            return true;
        }
    }

    /**
     * Adds the partitions a destructive commit touched to {@code affected}. Returns false when the
     * set cannot be trusted to be complete, which promotes the reconcile to the whole table.
     *
     * <p>Added data files count as touched, not just removed ones. A copy-on-write MERGE or UPDATE
     * commits as a single {@code overwrite} that both removes and writes files, and the rows it
     * wrote are as invisible to an append scan as the rows it removed. Their partitions are not
     * always the same partitions: a MERGE that inserts into an untouched partition, or an update to
     * a partition column that moves a row, writes where nothing was removed. Narrowing the reconcile
     * to the removal side alone would leave those new rows unembedded with nothing to report it.
     * Reading the adds costs no extra I/O -- Iceberg caches both sides of a snapshot's data file
     * changes from one pass over the manifests it wrote.
     */
    private boolean collectAffectedPartitions(Table table, Snapshot snapshot, Set<String> affected) {
        try {
            boolean resolved = true;
            for (ContentFile<?> removed : snapshot.removedDataFiles(table.io())) {
                resolved &= addPartition(table, removed, affected);
            }
            for (ContentFile<?> added : snapshot.addedDataFiles(table.io())) {
                resolved &= addPartition(table, added, affected);
            }
            for (ContentFile<?> deletes : snapshot.addedDeleteFiles(table.io())) {
                resolved &= addPartition(table, deletes, affected);
            }
            return resolved;
        } catch (Exception e) {
            log.warn("Cannot determine partitions touched by snapshot {} on {}, widening reconcile "
                            + "to the whole table: {}",
                    snapshot.snapshotId(), table.name(), e.getMessage());
            return false;
        }
    }

    private boolean addPartition(Table table, ContentFile<?> file, Set<String> affected) {
        PartitionSpec spec = table.specs().get(file.specId());
        if (spec == null || spec.isUnpartitioned()) {
            return false;
        }
        // A rendered partition path is only a usable filter while the table has exactly one spec.
        // Paths are rendered per file from that file's own spec, so once an older spec is in play a
        // path collected here can fail to match a file that holds the same logical partition -- and
        // that is a miss in the reconcile, not just a mismatch. Whole-table is the safe answer.
        if (table.specs().size() > 1) {
            return false;
        }

        affected.add(spec.partitionToPath(file.partition()));
        return true;
    }

    /**
     * Indexes the append commits in range by sequence number so each planned file can be attributed
     * to the commit that added it. A file's data sequence number is, by definition, the sequence
     * number of the snapshot that first wrote it, and the plan does not expose the snapshot directly.
     *
     * <p>Zero is excluded on both sides: format-version-1 tables leave every sequence number at 0,
     * where the mapping would attribute files to an arbitrary commit instead of failing over to the
     * end-snapshot default.
     */
    private Map<Long, Snapshot> appendsBySequenceNumber(Table table, long fromExclusive, long toInclusive) {
        Map<Long, Snapshot> bySequenceNumber = new HashMap<>();
        for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(table, toInclusive, fromExclusive)) {
            if (snapshot.sequenceNumber() > 0L) {
                bySequenceNumber.put(snapshot.sequenceNumber(), snapshot);
            }
        }
        return bySequenceNumber;
    }

    private Snapshot originOf(FileScanTask task, Map<Long, Snapshot> bySequenceNumber, Snapshot fallback) {
        Long dataSequenceNumber = task.file().dataSequenceNumber();
        if (dataSequenceNumber == null || dataSequenceNumber <= 0L) {
            return fallback;
        }

        // Falling back to the end snapshot keeps provenance monotone rather than absent: the rows are
        // certainly visible as of that version, which is the watermark the sync records anyway.
        return bySequenceNumber.getOrDefault(dataSequenceNumber, fallback);
    }

    private SourceFileWork toWork(String sourceTable, FileScanTask task, Snapshot origin, boolean fromBackfillAnchor) {
        return SourceFileWork.builder()
                .sourceTable(sourceTable)
                .dataFilePath(task.file().path().toString())
                .recordCount(task.file().recordCount())
                .fileSizeInBytes(task.file().fileSizeInBytes())
                .snapshotId(origin.snapshotId())
                .sequenceNumber(origin.sequenceNumber())
                .committedAtMillis(origin.timestampMillis())
                .fromBackfillAnchor(fromBackfillAnchor)
                .build();
    }

    private List<Record> readTask(Table table, FileScanTask task, Schema projection) {
        if (task.file().format() != FileFormat.PARQUET) {
            throw new UnsupportedOperationException(String.format(
                    "Source file %s is %s; only Parquet source files can be read (no Avro or ORC "
                            + "reader is on the worker classpath)",
                    task.file().path(), task.file().format()));
        }

        // The delete filter may need columns the caller did not ask for -- row positions for
        // positional deletes, the equality columns for equality deletes -- so the file is read at
        // requiredSchema() and trimmed back to the projection afterwards.
        GenericDeleteFilter deleteFilter =
                new GenericDeleteFilter(table.io(), task, table.schema(), projection);
        Schema readSchema = deleteFilter.requiredSchema();
        Map<Integer, ?> partitionConstants =
                PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);
        InputFile input = table.io().newInputFile(task.file().path().toString());

        List<Record> records = new ArrayList<>((int) Math.min(task.file().recordCount(), MAX_PRESIZE));
        try (CloseableIterable<Record> raw = Parquet.read(input)
                .project(readSchema)
                .withNameMapping(nameMapping(table))
                // Container reuse is deliberately off: every row is retained in the returned list,
                // and a reused container would leave the whole list aliasing the final row.
                .createReaderFunc(fileSchema ->
                        GenericParquetReaders.buildReader(readSchema, fileSchema, partitionConstants))
                .split(task.start(), task.length())
                .build();
             CloseableIterable<Record> live = deleteFilter.filter(raw)) {

            boolean trimNeeded = readSchema.columns().size() != projection.columns().size();
            for (Record record : live) {
                records.add(trimNeeded ? trim(record, projection) : record);
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to read data file " + task.file().path(), e);
        }

        return records;
    }

    /**
     * The table's default name mapping, or null when it has none (the usual case, and what the
     * reader already assumes).
     *
     * <p>Required for tables whose files were imported rather than written by Iceberg -- the
     * {@code add_files} path over an existing Parquet dataset, which registers files that carry no
     * Iceberg field ids. Read without a mapping, such a file falls back to assigning ids by column
     * position, which agrees with the table's field ids only for a schema that never evolved: after
     * a dropped or reordered column, positional ids feed one column's bytes to another field, so a
     * projection of two columns silently embeds the wrong text. The mapping is what resolves those
     * columns by name instead.
     */
    private static NameMapping nameMapping(Table table) {
        String json = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        return json == null ? null : NameMappingParser.fromJson(json);
    }

    private static Record trim(Record record, Schema projection) {
        GenericRecord trimmed = GenericRecord.create(projection);
        for (Types.NestedField field : projection.columns()) {
            trimmed.setField(field.name(), record.getField(field.name()));
        }
        return trimmed;
    }

    /**
     * Builds the read projection, rejecting unknown column names.
     *
     * <p>{@code Schema.select} silently drops names it does not recognize, so a typo would produce
     * an empty projection and a page of blank records that look like empty source rows rather than a
     * configuration error.
     */
    private static Schema projection(Schema schema, Collection<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return schema;
        }

        List<String> unknown = columns.stream()
                .filter(column -> schema.findField(column) == null)
                .toList();
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException(
                    "Source table has no column(s) " + unknown + "; available: " + schema.columns().stream()
                            .map(Types.NestedField::name)
                            .toList());
        }

        return schema.select(columns);
    }

    private boolean matchesPartition(Table table, ContentFile<?> file, Set<String> partitionPaths) {
        PartitionSpec spec = table.specs().get(file.specId());
        if (spec == null || spec.isUnpartitioned()) {
            // A file whose partition cannot be rendered cannot be ruled out either. Reading it when
            // the reconcile did not need it wastes work; skipping it when it was needed loses rows.
            return true;
        }
        return partitionPaths.contains(spec.partitionToPath(file.partition()));
    }

    /**
     * The anchor snapshot is gone, so incremental progress from it is impossible.
     *
     * <p>Typed rather than a plain {@code IllegalStateException} because the caller has to
     * distinguish it from a transient failure: this condition never resolves on its own, so retrying
     * it every cycle consumes the cycle forever while the materialization looks merely unlucky.
     */
    public static class ReanchorRequiredException extends IllegalStateException {
        public ReanchorRequiredException(String message) {
            super(message);
        }
    }

    private Snapshot requireSnapshot(Table table, TableConfig config, long snapshotId) {
        Snapshot snapshot = table.snapshot(snapshotId);
        if (snapshot == null) {
            throw new ReanchorRequiredException(String.format(
                    "Snapshot %d is no longer in the history of %s; the sync must be re-anchored "
                            + "with a backfill against a current snapshot",
                    snapshotId, config.getTableName()));
        }
        return snapshot;
    }

    private Table loadTable(TableConfig config) {
        try {
            return icebergTableService.loadTable(config);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Source table " + config.getTableName() + " is not available", e);
        }
    }
}
