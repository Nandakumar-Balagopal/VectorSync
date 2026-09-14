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
import java.util.LinkedHashSet;
import java.util.List;

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

    @Value("${vectorsync.runner.owner:worker-1}")
    private String owner;

    /** Files leased per cycle. Bounds heap and keeps a crash cheap; it is not a throughput knob. */
    @Value("${vectorsync.runner.lease-batch:8}")
    private int leaseBatch;

    @Value("${vectorsync.runner.lease-seconds:900}")
    private long leaseSeconds;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    @Value("${vectorsync.runner.publish-projection:true}")
    private boolean publishProjection;

    public MaterializationRunner(DerivationControlClient control,
                                 IncrementalChangeDetector detector,
                                 DeriveService deriveService,
                                 IcebergCatalogService catalogService) {
        this.control = control;
        this.detector = detector;
        this.deriveService = deriveService;
        this.catalogService = catalogService;
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

        int enqueued = 0;
        int processed = 0;
        int failed = 0;
        int projected = 0;
        int advanced = 0;

        for (Materialization materialization : runnable) {
            try {
                enqueued += plan(materialization);
                int[] result = drain(materialization);
                processed += result[0];
                failed += result[1];

                if (publish(materialization, result[0])) {
                    projected++;
                    advanced++;
                }
            } catch (Exception e) {
                log.error("Materialization {} ({}) failed this cycle: {}",
                        materialization.getId(), materialization.getSourceTable(), e.getMessage(), e);
            }
        }

        return new CycleReport(runnable.size(), enqueued, processed, failed, projected, advanced);
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

    /** @return {processed, failed} */
    private int[] drain(Materialization materialization) {
        MaterializationSpec spec = materialization.getSpec();
        TableConfig config = toTableConfig(spec);
        List<String> projection = projectedColumns(spec);

        int processed = 0;
        int failed = 0;

        List<LeasedItem> batch = control.lease(owner, leaseBatch, leaseSeconds);
        while (!batch.isEmpty()) {
            for (LeasedItem item : batch) {
                // The queue is global, so a lease can hand back work for a different
                // materialization. Deriving it with this spec would embed rows under the wrong
                // configuration, so it is released rather than guessed at.
                if (!materialization.getId().equals(item.getMaterializationId())) {
                    control.fail(item.getId(), owner,
                            "leased to a runner iterating a different materialization; requeued");
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

                    if (result == null || result.complete()) {
                        control.complete(item.getId());
                        processed++;
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

            // One batch per cycle per materialization: the loop yields so that a long backfill does
            // not starve the other materializations this cycle still has to visit.
            break;
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

        control.markLive(materialization.getId(), watermark);
        return true;
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
