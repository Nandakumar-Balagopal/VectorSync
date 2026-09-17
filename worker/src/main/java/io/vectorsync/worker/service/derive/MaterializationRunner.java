package io.vectorsync.worker.service.derive;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.format.derive.ProjectionBuilder;
import io.vectorsync.worker.client.DerivationControlClient;
import io.vectorsync.worker.client.DerivationControlClient.LeasedItem;
import io.vectorsync.worker.client.DerivationControlClient.Materialization;
import io.vectorsync.worker.client.DerivationControlClient.QueueDepth;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import io.vectorsync.worker.service.iceberg.IncrementalChangeDetector;
import io.vectorsync.worker.service.iceberg.SourceFileWork;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.data.Record;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Closes the loop: plans work for each materialization, drains it through the queue, publishes the
 * serving projection, and advances the watermark.
 *
 * <p>This is the component the system was missing. Every piece around it existed and none of them
 * were connected: the queue had no consumer, the projection builder was never called, and the only
 * thing running on a schedule was the snapshot-diffing path the rest of this work replaced. A
 * correct component that nothing invokes is indistinguishable from an absent one.
 *
 * <p>Execution is pull-based. The worker leases a bounded batch, derives it, and reports each item
 * individually, which is what makes progress durable at file granularity and lets several workers
 * share one materialization without coordinating. The previous entry point ran a whole backfill
 * synchronously inside an HTTP request thread, so a large table was bounded by a proxy timeout and
 * lost all of its progress when that timeout fired.
 *
 * <p>Ordering within a cycle matters and is not arbitrary: derive, then project, then advance. The
 * projection is rebuilt only from a queue that drained with zero failures, and the watermark moves
 * only after the projection it describes exists. Reversing either step publishes a version that
 * cannot be served or claims coverage that was never materialized.
 */
@Service
@Slf4j
public class MaterializationRunner {

    private final DerivationControlClient control;
    private final IncrementalChangeDetector detector;
    private final DeriveService deriveService;
    private final IcebergCatalogService catalogService;
    private final ContentHashIndex hashIndex;
    private final DeriveMetricsRegistry metrics;
    private final ReconcileService reconcileService;

    /**
     * Lease owner. Defaults to host and pid rather than a constant, because the fence in the queue
     * compares owner strings: every replica calling itself "worker-1" makes the fence inert, so a
     * stale worker could report on an item that had been reassigned to a peer.
     */
    @Value("${vectorsync.runner.owner:}")
    private String configuredOwner;

    private String owner;

    /**
     * Configuration groups derived concurrently within one cycle.
     *
     * <p>Defaults to 1, so the shipped behaviour is exactly the sequential cycle this replaced. The
     * deliverable here is that the knob exists and that raising it is checked, not that the default
     * changed -- raising it is a deployment decision with a hard prerequisite, below.
     */
    @Value("${vectorsync.runner.parallelism:1}")
    private int parallelism;

    private ExecutorService executor;

    /**
     * Resolves the lease owner and builds the cycle pool.
     *
     * <p>Both belong here rather than in the constructor because {@link #parallelism} and
     * {@link #configuredOwner} are field injected: in the constructor the int is still 0, and a pool
     * sized from it would either be rejected outright or -- worse, if it were a semaphore -- block
     * on the first acquire forever, behind the scheduler's re-entrancy skip, stopping derivation
     * permanently with nothing logged above DEBUG.
     */
    @jakarta.annotation.PostConstruct
    void startUp() {
        if (configuredOwner != null && !configuredOwner.isBlank()) {
            owner = configuredOwner;
        } else {
            String host;
            try {
                host = java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                host = "unknown-host";
            }
            owner = host + ":" + ProcessHandle.current().pid();
        }
        log.info("Derivation runner lease owner: {}", owner);

        int threads = Math.max(1, parallelism);
        if (threads > 1) {
            // Refusing to start, rather than warning. HadoopCatalog has no atomic commit: two
            // writers can both succeed and one silently wins, which is data loss with no exception
            // and no way to notice after the fact. IcebergCatalogConfig already refuses Hadoop on
            // object storage for the same reason; this is the second half of that check, because
            // parallelism is what turns a single-writer deployment into a multi-writer one.
            String catalogType = catalogService.config().type();
            if ("hadoop".equals(catalogType)) {
                throw new IllegalStateException(String.format(
                        "vectorsync.runner.parallelism is %d but the catalog type is hadoop, which "
                                + "has no atomic commit: two concurrent commits can both succeed and "
                                + "one is silently lost. Either set parallelism to 1, or move to a "
                                + "catalog with real commit semantics (jdbc, rest, hive, glue, "
                                + "nessie).", parallelism));
            }
            log.info("Derivation runner parallelism {} on a {} catalog", threads, catalogType);
        }

        executor = Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable, "derive-cycle-" + POOL_THREADS.incrementAndGet());
            // Daemon so a hung derive cannot keep a shutting-down worker alive.
            thread.setDaemon(true);
            return thread;
        });
    }

    @jakarta.annotation.PreDestroy
    void shutDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static final java.util.concurrent.atomic.AtomicInteger POOL_THREADS =
            new java.util.concurrent.atomic.AtomicInteger();

    /** Files leased per cycle. Bounds heap and keeps a crash cheap; it is not a throughput knob. */
    @Value("${vectorsync.runner.lease-batch:8}")
    private int leaseBatch;

    @Value("${vectorsync.runner.lease-seconds:900}")
    private long leaseSeconds;

    /** Ceiling on lease batches per materialization per cycle, so one backfill cannot hold a cycle. */
    @Value("${vectorsync.runner.max-batches-per-cycle:32}")
    private int maxBatchesPerCycle;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    @Value("${vectorsync.runner.publish-projection:true}")
    private boolean publishProjection;

    /**
     * Whether a range containing deletes or overwrites is reconciled automatically.
     *
     * <p>On by default, because the alternative is the materialization stopping. The refusal it
     * replaces was correct but terminal, and a reconcile that is correct is strictly better than a
     * halt an operator has to notice. A deployment that would rather be told than repaired can turn
     * it off and get the old behaviour exactly.
     */
    @Value("${vectorsync.runner.reconcile-enabled:true}")
    private boolean reconcileEnabled;

    public MaterializationRunner(DerivationControlClient control,
                                 IncrementalChangeDetector detector,
                                 DeriveService deriveService,
                                 IcebergCatalogService catalogService,
                                 ContentHashIndex hashIndex,
                                 DeriveMetricsRegistry metrics,
                                 ReconcileService reconcileService) {
        this.control = control;
        this.detector = detector;
        this.deriveService = deriveService;
        this.catalogService = catalogService;
        this.hashIndex = hashIndex;
        this.metrics = metrics;
        this.reconcileService = reconcileService;
    }

    public record CycleReport(int materializationsSeen,
                              int filesEnqueued,
                              int filesProcessed,
                              int filesFailed,
                              int projectionsPublished,
                              int watermarksAdvanced) {
    }

    /**
     * One pass over every runnable materialization.
     *
     * <p>Failures are isolated per materialization for the same reason the table loop is: a stable
     * iteration order means one broken entry would otherwise starve every entry behind it forever.
     */
    public CycleReport runCycle() {
        List<Materialization> runnable = control.runnable();
        if (runnable.isEmpty()) {
            return new CycleReport(0, 0, 0, 0, 0, 0);
        }

        // Grouped by configuration id, and the group -- not the materialization -- is the unit of
        // concurrency. Two materializations that share a configId address the same Tier-1 content
        // space, so deriving them at once means both probe the dedup record before either has
        // written, both miss, and both pay for the same inference. That breaks the one-row-per-
        // content invariant Tier 1 exists to hold and falsifies the equal-inference claim
        // ReproducibilityTest asserts. Groups run in parallel; members of a group run in sequence.
        //
        // Note configId deliberately excludes the source table, so a group is frequently more than
        // one materialization rather than rarely.
        Map<String, List<Materialization>> byConfig = new LinkedHashMap<>();
        for (Materialization materialization : runnable) {
            byConfig.computeIfAbsent(configIdOf(materialization), key -> new ArrayList<>())
                    .add(materialization);
        }

        List<Callable<CycleReport>> tasks = new ArrayList<>(byConfig.size());
        for (List<Materialization> group : byConfig.values()) {
            tasks.add(() -> {
                CycleReport groupReport = new CycleReport(0, 0, 0, 0, 0, 0);
                for (Materialization materialization : group) {
                    groupReport = add(groupReport, runOne(materialization));
                }
                return groupReport;
            });
        }

        CycleReport total = new CycleReport(0, 0, 0, 0, 0, 0);
        try {
            // invokeAll rather than a semaphore or a stream: it bounds concurrency at the pool size,
            // joins before returning so no work outlives the cycle, cannot leak a permit on an
            // exception, and cannot be rejected. It also makes two concurrent drains of one
            // materialization impossible by construction, which is what lets the lease owner stay
            // per-process instead of becoming per-thread churn in a VARCHAR(128).
            for (Future<CycleReport> future : executor.invokeAll(tasks)) {
                try {
                    total = add(total, future.get());
                } catch (ExecutionException e) {
                    // runOne catches everything, so reaching here means the group wrapper itself
                    // broke. Logged rather than rethrown: one group must not void the cycle's report.
                    log.error("A materialization group failed outside its own handler: {}",
                            e.getCause() == null ? e.toString() : e.getCause().toString(), e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Derivation cycle interrupted; {} groups were in flight", tasks.size());
        }

        return new CycleReport(runnable.size(), total.filesEnqueued(), total.filesProcessed(),
                total.filesFailed(), total.projectionsPublished(), total.watermarksAdvanced());
    }

    /**
     * One materialization's plan, drain and publish, with every failure contained.
     *
     * <p>Extracted from the cycle loop unchanged. Failures are isolated per materialization for the
     * same reason the table loop is: a stable iteration order means one broken entry would otherwise
     * starve every entry behind it forever. Containing them here rather than at the group level also
     * keeps a thrown exception from cancelling the peers in its own group.
     */
    private CycleReport runOne(Materialization materialization) {
        try {
            int enqueued = plan(materialization);
            int[] result = drain(materialization);
            int projected = publish(materialization, result[0]) ? 1 : 0;
            return new CycleReport(0, enqueued, result[0], result[1], projected, projected);
        } catch (IncrementalChangeDetector.ReanchorRequiredException e) {
            // Never resolves by retrying: the anchor snapshot has been expired or the source
            // table was replaced. Parked with the reason so it stops consuming a cycle and an
            // operator can re-admit it, rather than throwing every interval forever.
            log.warn("Materialization {} ({}) needs re-anchoring: {}",
                    materialization.getId(), materialization.getSourceTable(), e.getMessage());
            control.markDegraded(materialization.getId(), e.getMessage());
        } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
            // The source table is gone. Also terminal without intervention.
            log.warn("Materialization {} ({}) has no source table; parking it",
                    materialization.getId(), materialization.getSourceTable());
            control.markDegraded(materialization.getId(),
                    "source table no longer exists: " + e.getMessage());
        } catch (Exception e) {
            log.error("Materialization {} ({}) failed this cycle: {}",
                    materialization.getId(), materialization.getSourceTable(), e.getMessage(), e);
        }
        return new CycleReport(0, 0, 0, 0, 0, 0);
    }

    private static CycleReport add(CycleReport left, CycleReport right) {
        return new CycleReport(
                left.materializationsSeen() + right.materializationsSeen(),
                left.filesEnqueued() + right.filesEnqueued(),
                left.filesProcessed() + right.filesProcessed(),
                left.filesFailed() + right.filesFailed(),
                left.projectionsPublished() + right.projectionsPublished(),
                left.watermarksAdvanced() + right.watermarksAdvanced());
    }

    /**
     * Grouping key. Falls back to the materialization id when the spec cannot produce a
     * configuration id, which keeps an unreadable entry in its own group rather than silently
     * sharing one with every other broken entry.
     */
    private static String configIdOf(Materialization materialization) {
        try {
            MaterializationSpec spec = materialization.getSpec();
            if (spec != null && spec.configId() != null) {
                return spec.configId();
            }
        } catch (Exception e) {
            log.warn("Could not derive a config id for materialization {}; isolating it: {}",
                    materialization.getId(), e.getMessage());
        }
        return "unkeyed:" + materialization.getId();
    }

    /**
     * Plans the files this materialization still owes and hands them to the queue.
     *
     * <p>Enqueue is idempotent on (materialization, file, snapshot), so replanning every cycle is
     * safe and is how a planner crash heals itself. The cost is one metadata plan per cycle, which
     * is far cheaper than tracking planning progress separately and risking it diverging from the
     * queue.
     */
    private int plan(Materialization materialization) {
        MaterializationSpec spec = materialization.getSpec();
        TableConfig config = toTableConfig(spec);

        boolean backfilled = materialization.getIncrementalWatermark() > 0;
        if (!backfilled) {
            QueueDepth depth = control.depth(materialization.getId());

            // Replan only when the queue holds nothing for this materialization. Keying on the
            // watermark instead meant replanning on every cycle for the entire backfill, because
            // markLive only sets it once the queue drains: a 10,000-file table replanned 1,250
            // times and re-sent 10,000 descriptors each time -- roughly 12.5M no-op upserts to do
            // 10,000 files of work, with the enqueue round trips, not derivation, setting the
            // wall-clock floor.
            if (depth != null && (depth.getPending() > 0 || depth.getLeased() > 0)) {
                return 0;
            }
            if (depth != null && depth.getDone() > 0 && depth.getFailed() == 0) {
                // Everything planned has been derived; publish() advances from here.
                return 0;
            }

            control.beginBackfill(materialization.getId());
            List<SourceFileWork> work = detector.backfillWork(config, materialization.getAnchorSnapshotId());
            int inserted = control.enqueue(materialization.getId(), work, "BACKFILL");
            if (inserted > 0) {
                log.info("Materialization {} backfill: {} of {} files newly queued",
                        materialization.getId(), inserted, work.size());
            }
            return inserted;
        }

        IncrementalChangeDetector.SourceVersion current = detector.currentVersion(config);
        if (current.isEmpty() || current.snapshotId() == materialization.getAnchorSnapshotId()) {
            return 0;
        }

        // An append scan cannot see deletes or overwrites. Advancing anyway would move the watermark
        // past changes that were never materialized, so the materialization is parked with a reason
        // instead. An operator reconciles or re-anchors; the system does not guess.
        IncrementalChangeDetector.ScanAssessment assessment =
                detector.assess(config, materialization.getAnchorSnapshotId(), current.snapshotId());
        if (!assessment.incrementalSafe()) {
            if (assessment.reconcileRequired() && reconcileEnabled) {
                return reconcile(materialization, spec, config, current);
            }
            log.warn("Materialization {} needs a reconcile: {}", materialization.getId(),
                    assessment.verdict());
            control.markDegraded(materialization.getId(),
                    "incremental scan unsafe: " + assessment.verdict()
                            + " (deletes or overwrites between snapshots require a reconcile)");
            return 0;
        }

        List<SourceFileWork> work = detector.incrementalWork(
                config, materialization.getAnchorSnapshotId(), current.snapshotId());
        return control.enqueue(materialization.getId(), work, "INCREMENTAL");
    }

    /**
     * Repairs a range an append scan cannot describe: re-derive the source at the new version, then
     * sweep away whatever no longer exists.
     *
     * <p><b>Why this is one operation and not two steps.</b> The obvious shape -- enqueue the files,
     * let {@code drain} process them, then sweep in {@code publish} -- cannot be made safe here, and
     * the reason is worth stating because it is not obvious. {@code publish} begins
     * {@code if (processedThisCycle == 0 && live) return false;}, and {@code enqueue} is idempotent
     * on {@code (materializationId, dataFilePath, snapshotId)} <em>including rows already DONE</em>.
     * So a crash between the derive and the sweep leaves a cycle that enqueues nothing, drains
     * nothing, and returns before sweeping -- permanently. The materialization would read LIVE and
     * healthy while serving deleted rows, which is strictly worse than the loud refusal this
     * replaces.
     *
     * <p>So the sweep runs here, in the same call that did the derive, before anything advances. If
     * this method throws or the sweep refuses, no watermark moves and the range is re-assessed next
     * cycle -- the same range, with the same verdict, arriving here again. Failure is a retry rather
     * than a gap.
     *
     * <p>The sweep is deliberately ordered after the derive. A row that was rewritten rather than
     * removed must already have its new content-map version committed before the sweep asks which
     * keys exist, or the sweep and the derive would disagree about the same row.
     *
     * @return files enqueued, so the caller's accounting is unchanged
     */
    private int reconcile(Materialization materialization,
                          MaterializationSpec spec,
                          TableConfig config,
                          IncrementalChangeDetector.SourceVersion target) {
        log.info("Materialization {} reconciling to snapshot {} (sequence {})",
                materialization.getId(), target.snapshotId(), target.sequenceNumber());

        // Pinned to the target version, not "current". The re-derive and the sweep have to describe
        // the same source version or a commit landing between them tombstones rows the derive never
        // looked at.
        List<SourceFileWork> work = detector.backfillWork(config, target.snapshotId());
        int enqueued = control.enqueue(materialization.getId(), work, "BACKFILL");

        int[] drained = drain(materialization);
        if (drained[1] > 0) {
            control.markDegraded(materialization.getId(),
                    drained[1] + " files failed during a reconcile; the sweep was not attempted "
                            + "because tombstoning against a partial re-derive would retire rows "
                            + "whose new version had not been written");
            return enqueued;
        }

        ReconcileService.SweepResult swept = reconcileService.sweep(
                spec, config, target.snapshotId(), target.sequenceNumber(),
                System.currentTimeMillis());

        if (!swept.complete()) {
            log.warn("Materialization {} sweep refused: {}", materialization.getId(), swept.note());
            control.markDegraded(materialization.getId(), "reconcile sweep refused: " + swept.note());
            return enqueued;
        }

        log.info("Materialization {} reconciled: {} of {} mapped rows retired",
                materialization.getId(), swept.tombstoned(), swept.liveBefore());
        return enqueued;
    }

    /** @return {processed, failed} */
    private int[] drain(Materialization materialization) {
        MaterializationSpec spec = materialization.getSpec();
        TableConfig config = toTableConfig(spec);
        List<String> projection = projectedColumns(spec);

        int processed = 0;
        int failed = 0;

        int batches = 0;
        List<LeasedItem> batch = control.lease(owner, leaseBatch, leaseSeconds, materialization.getId());
        while (!batch.isEmpty()) {
            for (LeasedItem item : batch) {
                // The lease is scoped to this materialization, so a mismatch means the control
                // plane and the worker disagree about what was claimed. Skipping without reporting
                // is deliberate: reporting a failure would spend a retry from this item's budget for
                // a problem that is not the item's fault, and the lease expiring returns it intact.
                if (!materialization.getId().equals(item.getMaterializationId())) {
                    log.error("Lease for {} returned an item belonging to {}; leaving it to expire",
                            materialization.getId(), item.getMaterializationId());
                    continue;
                }

                try {
                    SourceFileWork file = new SourceFileWork(
                            item.getSourceTable(), item.getDataFilePath(), item.getRecordCount(),
                            0L, item.getSnapshotId(), item.getSequenceNumber(),
                            item.getCommittedAtMillis(), "BACKFILL".equals(item.getKind()));

                    List<Record> rows = detector.readFile(config, file, projection);
                    DeriveResult result = rows.isEmpty()
                            ? null
                            : deriveService.derive(spec, rows, file.snapshotId(),
                                    file.sequenceNumber(), file.committedAtMillis());

                    // Recorded regardless of outcome: a pass that failed is exactly the one an
                    // operator needs counted, and a registry that only sees successes reports a
                    // healthy dedup rate for a materialization that is not progressing.
                    metrics.record(spec.getSourceTable(), spec.configId(), result);

                    if (result == null || result.complete()) {
                        // The hashes this pass durably wrote are reported WITH the completion, so
                        // the control plane records them in the same transaction. Only once that
                        // transaction is confirmed may they enter the local cache: caching on the
                        // strength of a local write is what made the previous dedup set claim
                        // vectors whose commit had not survived.
                        List<DerivationControlClient.EmbeddedContent> embedded = result == null
                                ? List.of()
                                : result.written().stream()
                                        .map(w -> new DerivationControlClient.EmbeddedContent(
                                                spec.modelVersion(), spec.configId(),
                                                w.contentHash(), w.embeddingDim()))
                                        .toList();

                        if (control.complete(item.getId(), embedded)) {
                            hashIndex.cacheConfirmed(
                                    embedded.stream()
                                            .map(DerivationControlClient.EmbeddedContent::contentHash)
                                            .toList(),
                                    spec.modelVersion(), spec.configId());
                            processed++;
                        } else {
                            // Neither the completion nor the dedup records landed. The lease will
                            // expire and the item is redone; the vectors are already durable, so the
                            // retry re-embeds them, which is wasteful and correct.
                            log.warn("Completion for {} was not confirmed; leaving it to expire",
                                    item.getDataFilePath());
                            failed++;
                        }
                    } else {
                        control.fail(item.getId(), owner,
                                result.failed() + " rows did not materialize");
                        failed++;
                    }
                } catch (Exception e) {
                    control.fail(item.getId(), owner, String.valueOf(e.getMessage()));
                    failed++;
                    log.error("File {} failed: {}", item.getDataFilePath(), e.getMessage());
                }
            }

            // Keep draining, but with a ceiling so one large backfill cannot hold the cycle for an
            // unbounded time while other materializations wait. Leasing one batch per cycle instead
            // made the scheduler interval the throughput limit: a 10,000-file table needed 1,250
            // cycles, a floor of over five hours at a 15-second interval regardless of derivation
            // speed.
            batches++;
            if (batches >= maxBatchesPerCycle) {
                break;
            }
            batch = control.lease(owner, leaseBatch, leaseSeconds, materialization.getId());
        }

        return new int[]{processed, failed};
    }

    /**
     * Rebuilds the serving projection and advances the watermark, but only from a clean queue.
     *
     * <p>Requires that this cycle actually derived something, or that the materialization has not
     * reached LIVE yet. Queue counters are cumulative, so "drained with done > 0" stays true forever
     * once a backfill finishes: keying on it rebuilt the whole projection on every tick, which is an
     * unbounded overwrite of a table nothing had changed.
     *
     * @param processedThisCycle files this runner completed in this cycle
     * @return true when the watermark advanced
     */
    private boolean publish(Materialization materialization, int processedThisCycle) {
        boolean live = "LIVE".equals(materialization.getState());
        if (processedThisCycle == 0 && live) {
            return false;
        }

        QueueDepth depth = control.depth(materialization.getId());
        if (depth == null || !depth.drained()) {
            return false;
        }

        if (!depth.complete()) {
            // Drained but with terminal failures. Those files will never be dispatched again, so
            // publishing here would advertise coverage that is permanently missing rows.
            log.warn("Materialization {} drained with {} failed files; not advancing",
                    materialization.getId(), depth.getFailed());
            control.markDegraded(materialization.getId(),
                    depth.getFailed() + " files exhausted their retries; a reconcile is required");
            return false;
        }

        if (depth.getDone() == 0) {
            return false;
        }

        MaterializationSpec spec = materialization.getSpec();
        long watermark = detector.currentVersion(toTableConfig(spec)).sequenceNumber();

        if (publishProjection) {
            ProjectionBuilder.Projection projection = ProjectionBuilder.build(
                    catalogService.getCatalog(), vectorNamespace, spec);
            if (projection.unresolvedRows() > 0) {
                // Mappings whose vector is missing from the store. The projection is publishable but
                // incomplete, and silence here would look identical to a healthy rebuild.
                log.warn("Projection for {} left {} rows unresolved", materialization.getId(),
                        projection.unresolvedRows());
            }
            log.info("Published projection {} for {}: {} rows, {} vectors, dim {}",
                    projection.tableIdentifier(), materialization.getId(),
                    projection.rowsWritten(), projection.distinctVectors(), projection.embeddingDim());
        }

        boolean advanced = control.markLive(materialization.getId(), watermark);
        if (!advanced) {
            // The projection is published but the control plane did not record the watermark. Safe
            // in that direction -- the next cycle republishes and retries -- but it must not be
            // counted as progress, which is what the report is for.
            log.warn("Projection for {} published but the watermark was not recorded; will retry",
                    materialization.getId());
        }
        return advanced;
    }

    private static TableConfig toTableConfig(MaterializationSpec spec) {
        return TableConfig.builder()
                .tableName(spec.getSourceTable())
                .embeddingColumns(spec.getEmbeddingColumns())
                .modelName(spec.getModelName())
                .embeddingVersion(spec.getEmbeddingVersion())
                .enabled(true)
                .build();
    }

    /** Key columns plus embedded columns. Reading the rest to assemble two of them is the waste. */
    private static List<String> projectedColumns(MaterializationSpec spec) {
        LinkedHashSet<String> columns = new LinkedHashSet<>(spec.getKeyColumns());
        columns.addAll(spec.getEmbeddingColumns());
        return new ArrayList<>(columns);
    }
}
