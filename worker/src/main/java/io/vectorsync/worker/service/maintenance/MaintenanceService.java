package io.vectorsync.worker.service.maintenance;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.vectorsync.format.maintenance.TableMaintenance;
import io.vectorsync.format.maintenance.TableMaintenance.CompactionResult;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs {@link TableMaintenance} over every derived table.
 *
 * <p>Enumerates the vector namespace rather than naming tables. The namespace holds only tables this
 * system created -- Tier 1's content map and embedding store, a Tier 2 projection per
 * {@code (source_table, config_id)}, and Tier 3's clustered, centroid and coverage tables -- and
 * the Tier 2 names are built from user-supplied table names and configuration ids, so a hardcoded
 * list would silently skip exactly the tables that grow fastest.
 *
 * <p>Source tables are never touched. They belong to whoever registered them, and this system's
 * whole claim is that it derives from them without modifying them; compacting one would break that
 * for a housekeeping benefit that is not ours to take.
 *
 * <p>Single-flight. A full compaction pass reads and rewrites real data, and two concurrent passes
 * over one table would mostly waste work: whichever commits second finds its files already replaced
 * and fails validation. The gate is a process-local flag, which is the right scope -- the commit
 * validation is what makes it safe across processes, and this only avoids the obvious self-inflicted
 * case.
 */
@Service
@Slf4j
public class MaintenanceService {

    private final IcebergCatalogService catalogService;
    private final MeterRegistry meters;
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    /**
     * How long snapshot history is kept.
     *
     * <p>Not arbitrary housekeeping: this is the window in which an as-of read can still reproduce
     * a past vector set, and expiry also deletes the data files those snapshots referenced. Six
     * hours comfortably outlives any derive pass while still bounding {@code metadata.json}, which
     * matters more here than usual because the catalog runs with caching disabled and re-parses it
     * on every load.
     */
    @Value("${vectorsync.maintenance.snapshot-retain-hours:6}")
    private long snapshotRetainHours;

    /** Floor on retained snapshots, so a quiet table keeps a usable history regardless of age. */
    @Value("${vectorsync.maintenance.min-snapshots:10}")
    private int minSnapshots;

    @Value("${vectorsync.maintenance.target-file-bytes:134217728}")
    private long targetFileBytes;

    @Value("${vectorsync.maintenance.min-files-to-compact:5}")
    private int minFilesToCompact;

    /**
     * Skip a table a writer has touched this recently.
     *
     * <p>The reason this exists rather than being left to Iceberg's commit retry: both sides
     * compare-and-set, and only one loses well. Maintenance losing costs a tick; the derive path
     * losing costs one of a work item's three attempts, and three losses in a row is a DEGRADED
     * materialization. Housekeeping must never be able to do that, so it yields unconditionally.
     */
    @Value("${vectorsync.maintenance.quiet-period-seconds:30}")
    private long quietPeriodSeconds;

    public MaintenanceService(IcebergCatalogService catalogService, MeterRegistry meters) {
        this.catalogService = catalogService;
        this.meters = meters;
    }

    /**
     * @param table          derived table this describes
     * @param snapshotsAfter snapshots remaining, so a caller can see the bound taking effect
     */
    public record TableReport(String table,
                              int snapshotsBefore,
                              int snapshotsAfter,
                              int snapshotsExpired,
                              boolean manifestsRewritten,
                              int filesBefore,
                              int filesAfter,
                              int groupsCompacted,
                              long bytesRewritten,
                              String note) {
    }

    public record MaintenanceReport(boolean ran,
                                    boolean compacted,
                                    int tablesVisited,
                                    int totalSnapshotsExpired,
                                    int totalFilesRemoved,
                                    long totalBytesRewritten,
                                    long elapsedMillis,
                                    List<TableReport> tables,
                                    String note) {
    }

    /**
     * Expires snapshots and coalesces manifests on every derived table, and optionally rewrites
     * small data files.
     *
     * <p>Compaction is separate because the cost difference is not incremental: the first two
     * operations touch metadata only and take milliseconds, while a rewrite reads and writes the
     * data. A deployment that wants its metadata bounded on a tight timer and its data rewritten
     * rarely -- which is most of them -- needs them on different schedules.
     */
    public MaintenanceReport run(boolean compact) {
        if (!running.compareAndSet(false, true)) {
            return new MaintenanceReport(false, compact, 0, 0, 0, 0L, 0L, List.of(),
                    "a maintenance pass is already running in this worker");
        }

        long startedAt = System.currentTimeMillis();
        try {
            List<TableIdentifier> identifiers = derivedTables();
            if (identifiers.isEmpty()) {
                return new MaintenanceReport(true, compact, 0, 0, 0, 0L,
                        System.currentTimeMillis() - startedAt, List.of(),
                        "namespace " + vectorNamespace + " holds no derived tables yet");
            }

            List<TableReport> reports = new ArrayList<>(identifiers.size());
            int expired = 0;
            int filesRemoved = 0;
            long bytesRewritten = 0L;

            for (TableIdentifier identifier : identifiers) {
                TableReport report = maintainOne(identifier, compact);
                if (report == null) {
                    continue;
                }
                reports.add(report);
                expired += report.snapshotsExpired();
                filesRemoved += Math.max(0, report.filesBefore() - report.filesAfter());
                bytesRewritten += report.bytesRewritten();
            }

            long elapsed = System.currentTimeMillis() - startedAt;
            log.info("Maintenance pass over {} tables in {} ms: {} snapshots expired, "
                            + "{} fewer data files, {} MiB rewritten",
                    reports.size(), elapsed, expired, filesRemoved,
                    bytesRewritten / (1024 * 1024));

            return new MaintenanceReport(true, compact, reports.size(), expired, filesRemoved,
                    bytesRewritten, elapsed, reports, "ok");
        } finally {
            running.set(false);
        }
    }

    private TableReport maintainOne(TableIdentifier identifier, boolean compact) {
        Table table;
        try {
            table = catalogService.getCatalog().loadTable(identifier);
        } catch (Exception e) {
            // A table can disappear between listing and loading, and a listing is not a lock.
            log.debug("Skipping {}: {}", identifier, e.getMessage());
            return null;
        }

        int snapshotsBefore = countSnapshots(table);
        int filesBefore = countDataFiles(table);

        // Before anything commits. A busy table is reported rather than silently skipped, so the
        // status output explains why a fragmented table was left alone.
        if (TableMaintenance.isBusy(table, Duration.ofSeconds(quietPeriodSeconds))) {
            return new TableReport(identifier.name(), snapshotsBefore, snapshotsBefore, 0, false,
                    filesBefore, filesBefore, 0, 0L,
                    "skipped: committed to within the last " + quietPeriodSeconds
                            + "s, so a writer is active");
        }

        CompactionResult compaction = CompactionResult.nothingToDo("compaction not requested");
        if (compact) {
            compaction = TableMaintenance.compactDataFiles(
                    table, targetFileBytes, minFilesToCompact);
            if (compaction.didWork()) {
                table.refresh();
            }
        }

        // Manifests after compaction, not before: a rewrite replaces files and therefore writes
        // new manifests, so coalescing first would immediately be undone.
        boolean manifests = TableMaintenance.rewriteManifests(table);
        if (manifests) {
            table.refresh();
        }

        // Expiry last, so the snapshots the two rewrites just created are the ones being retained
        // and the files they replaced become eligible for deletion in this same pass.
        int expired = TableMaintenance.expireSnapshots(
                table, Duration.ofHours(snapshotRetainHours), minSnapshots);
        table.refresh();

        int filesAfter = countDataFiles(table);
        publish(identifier, expired, compaction, filesBefore, filesAfter);

        return new TableReport(identifier.name(), snapshotsBefore, countSnapshots(table), expired,
                manifests, filesBefore, filesAfter, compaction.groupsCompacted(),
                compaction.bytesRewritten(), compaction.note());
    }

    /**
     * Tagged by table, which is bounded here in a way most tags in this system are not: derived
     * table names come from the namespace listing, so the cardinality is the number of
     * materializations rather than anything a request can inflate.
     */
    private void publish(TableIdentifier identifier, int expired, CompactionResult compaction,
                         int filesBefore, int filesAfter) {
        String table = identifier.name();
        Counter.builder("vectorsync.maintenance.snapshots.expired")
                .description("Snapshots expired from derived tables")
                .tag("table", table).register(meters).increment(expired);
        Counter.builder("vectorsync.maintenance.files.removed")
                .description("Data files eliminated by compaction")
                .tag("table", table).register(meters)
                .increment(Math.max(0, filesBefore - filesAfter));
        Counter.builder("vectorsync.maintenance.bytes.rewritten")
                .description("Bytes read and written again by compaction; the cost of the operation")
                .tag("table", table).register(meters).increment(compaction.bytesRewritten());
    }

    /** Current file and snapshot counts per derived table, without changing anything. */
    public Map<String, Object> status() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("namespace", vectorNamespace);
        out.put("running", running.get());
        out.put("snapshotRetainHours", snapshotRetainHours);
        out.put("targetFileBytes", targetFileBytes);

        List<Map<String, Object>> tables = new ArrayList<>();
        for (TableIdentifier identifier : derivedTables()) {
            try {
                Table table = catalogService.getCatalog().loadTable(identifier);
                int files = countDataFiles(table);
                long bytes = 0L;
                long records = 0L;
                try (var tasks = table.newScan().planFiles()) {
                    for (var task : tasks) {
                        bytes += task.file().fileSizeInBytes();
                        records += task.file().recordCount();
                    }
                }

                Map<String, Object> view = new LinkedHashMap<>();
                view.put("table", identifier.name());
                view.put("snapshots", countSnapshots(table));
                view.put("dataFiles", files);
                view.put("records", records);
                view.put("bytes", bytes);
                // The number that says whether compaction is needed. Fragmentation shows up here
                // long before it shows up in a query timing.
                view.put("avgRecordsPerFile", files == 0 ? 0L : records / files);
                view.put("avgFileBytes", files == 0 ? 0L : bytes / files);
                tables.add(view);
            } catch (Exception e) {
                tables.add(Map.of("table", identifier.name(), "error", String.valueOf(e.getMessage())));
            }
        }
        out.put("tables", tables);
        return out;
    }

    private List<TableIdentifier> derivedTables() {
        try {
            return new ArrayList<>(
                    catalogService.getCatalog().listTables(Namespace.of(vectorNamespace)));
        } catch (Exception e) {
            log.debug("Could not list namespace {}: {}", vectorNamespace, e.getMessage());
            return List.of();
        }
    }

    private static int countSnapshots(Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private static int countDataFiles(Table table) {
        if (table.currentSnapshot() == null) {
            return 0;
        }
        int count = 0;
        try (var tasks = table.newScan().planFiles()) {
            for (var ignored : tasks) {
                count++;
            }
        } catch (Exception e) {
            return -1;
        }
        return count;
    }
}
