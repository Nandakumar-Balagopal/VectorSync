package io.vectorsync.worker.service.derive;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.worker.service.iceberg.IncrementalChangeDetector;
import io.vectorsync.worker.service.iceberg.SourceFileWork;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.data.Record;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Drives a materialization from source files to Tier-1 writes, one file at a time.
 *
 * <p>File-at-a-time is the load-bearing decision, not an implementation detail. The replaced design
 * read whole snapshots into memory to diff them, so its cost and its peak heap both scaled with
 * table size regardless of how little had changed; a large table simply could not be processed.
 * Here the unit of work is an Iceberg data file, which bounds memory to one file and makes progress
 * durable at file granularity: a crash resumes at the next file rather than restarting the table.
 *
 * <p>Failures are per file. A file that fails is counted and the run continues, because the
 * alternative -- abandoning the pass -- means one unreadable file indefinitely blocks every file
 * behind it. The watermark only advances when every file in the pass succeeded, so a partial pass
 * is retried rather than silently skipped.
 */
@Service
@Slf4j
public class DeriveOrchestrationService {

    private final IncrementalChangeDetector detector;
    private final DeriveService deriveService;

    private final DeriveMetricsRegistry metrics;

    public DeriveOrchestrationService(IncrementalChangeDetector detector,
                                      DeriveService deriveService,
                                      DeriveMetricsRegistry metrics) {
        this.metrics = metrics;
        this.detector = detector;
        this.deriveService = deriveService;
    }

    /**
     * @param filesProcessed files that produced Tier-1 writes
     * @param filesFailed    files that could not be read or embedded; the pass is incomplete
     * @param complete       true only when every file succeeded, which is the condition for
     *                       advancing a watermark
     */
    public record PassResult(String sourceTable,
                             String configId,
                             String modelVersion,
                             long snapshotId,
                             long sequenceNumber,
                             int filesProcessed,
                             int filesFailed,
                             int rowsProcessed,
                             int chunksProcessed,
                             int distinctHashes,
                             int cacheHits,
                             int inferenceCalls,
                             long elapsedMillis,
                             boolean complete) {

        public double inferenceAvoidedRate() {
            return chunksProcessed == 0 ? 0.0 : 1.0 - ((double) inferenceCalls / chunksProcessed);
        }
    }

    /** Full backfill over a pinned snapshot: the reproducible baseline a materialization starts from. */
    public PassResult backfill(MaterializationSpec spec, TableConfig config, long startedAtMillis) {
        // One read for both halves of the version. Reading them separately lets a commit land in
        // between and pairs a snapshot with the next snapshot's sequence number.
        IncrementalChangeDetector.SourceVersion version = detector.currentVersion(config);
        return run(spec, config, detector.backfillWork(config, version.snapshotId()),
                version.snapshotId(), version.sequenceNumber(), startedAtMillis);
    }

    /**
     * Incremental pass over files added since {@code fromSnapshotExclusive}.
     *
     * <p>Refuses to proceed when the detector reports that the range contains deletes or overwrites,
     * which an append scan cannot see. Returning an empty successful pass there would advance the
     * watermark past changes that were never materialized -- the failure mode is silent and
     * permanent, so it is surfaced as an incomplete pass instead.
     */
    public PassResult incremental(MaterializationSpec spec,
                                  TableConfig config,
                                  long fromSnapshotExclusive,
                                  long startedAtMillis) {
        IncrementalChangeDetector.SourceVersion version = detector.currentVersion(config);
        long to = version.snapshotId();
        long sequence = version.sequenceNumber();

        if (to == fromSnapshotExclusive) {
            return new PassResult(spec.getSourceTable(), spec.configId(), spec.modelVersion(),
                    to, sequence, 0, 0, 0, 0, 0, 0, 0, 0L, true);
        }

        IncrementalChangeDetector.ScanAssessment assessment =
                detector.assess(config, fromSnapshotExclusive, to);
        if (!assessment.incrementalSafe()) {
            log.warn("Incremental pass for {} refused: {}. A reconcile or re-anchor is required.",
                    spec.getSourceTable(), assessment.verdict());
            return new PassResult(spec.getSourceTable(), spec.configId(), spec.modelVersion(),
                    to, sequence, 0, 1, 0, 0, 0, 0, 0, 0L, false);
        }

        return run(spec, config, detector.incrementalWork(config, fromSnapshotExclusive, to),
                to, sequence, startedAtMillis);
    }

    private PassResult run(MaterializationSpec spec,
                           TableConfig config,
                           List<SourceFileWork> work,
                           long snapshotId,
                           long sequenceNumber,
                           long startedAtMillis) {
        List<String> projection = projectedColumns(spec);

        int filesProcessed = 0;
        int filesFailed = 0;
        int rows = 0;
        int chunks = 0;
        int distinct = 0;
        int hits = 0;
        int calls = 0;

        for (SourceFileWork file : work) {
            try {
                List<Record> sourceRows = detector.readFile(config, file, projection);
                if (sourceRows.isEmpty()) {
                    filesProcessed++;
                    continue;
                }

                // Recorded here as well as in MaterializationRunner. Only the runner recorded
                // before, so a pass driven through this path produced no meters at all -- an
                // operator running a one-off derive got counters that silently stayed at zero, and
                // /actuator/metrics listed no vectorsync meters whatsoever until the scheduler
                // happened to do work. Same shape as the dedup record this path also skipped: a
                // thinner entry point quietly bypassing a cross-cutting concern.
                DeriveResult result = deriveService.derive(
                        spec, sourceRows, file.snapshotId(), file.sequenceNumber(),
                        file.committedAtMillis());

                metrics.record(spec.getSourceTable(), spec.configId(), result);

                rows += result.rowsProcessed();
                chunks += result.chunksProcessed();
                distinct += result.distinctHashes();
                hits += result.cacheHits();
                calls += result.inferenceCalls();

                if (result.complete()) {
                    filesProcessed++;
                } else {
                    filesFailed++;
                }
            } catch (Exception e) {
                filesFailed++;
                log.error("File {} of {} failed: {}", file.dataFilePath(), spec.getSourceTable(),
                        e.getMessage());
            }
        }

        // Elapsed time is passed in rather than read here so the caller owns the clock; this class
        // stays free of wall-clock reads, which keeps it deterministic under test.
        long elapsed = Math.max(0L, System.currentTimeMillis() - startedAtMillis);

        PassResult result = new PassResult(spec.getSourceTable(), spec.configId(), spec.modelVersion(),
                snapshotId, sequenceNumber, filesProcessed, filesFailed, rows, chunks, distinct,
                hits, calls, elapsed, filesFailed == 0);

        log.info("Pass over {} ({} files, {} failed): {} rows, {} chunks, {} inference calls, "
                        + "{} avoided, {}ms",
                spec.getSourceTable(), filesProcessed, filesFailed, rows, chunks, calls,
                String.format("%.1f%%", result.inferenceAvoidedRate() * 100), elapsed);
        return result;
    }

    /**
     * Columns actually needed from the source: the key and the embedded text, nothing else.
     *
     * <p>The replaced implementation read every column of every row to assemble two of them, and
     * then treated a change in any unread column as a reason to re-embed. Projection here is why a
     * price update on a product table costs nothing.
     */
    private static List<String> projectedColumns(MaterializationSpec spec) {
        LinkedHashSet<String> columns = new LinkedHashSet<>(spec.getKeyColumns());
        columns.addAll(spec.getEmbeddingColumns());
        return new ArrayList<>(columns);
    }
}
