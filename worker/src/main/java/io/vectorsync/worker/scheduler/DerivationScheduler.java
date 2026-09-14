package io.vectorsync.worker.scheduler;

import io.vectorsync.worker.service.derive.MaterializationRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drives {@link MaterializationRunner} on a fixed delay.
 *
 * <p>This replaces {@code SyncScheduler} as the scheduled derivation path. That one drove
 * {@code SyncOrchestrationService}, whose change detection loaded both snapshots of a table fully
 * into memory and diffed them in Java -- two complete scans and twice the table resident in heap to
 * discover a single changed row -- and wrote the old row-keyed vector table that nothing in the
 * content-addressed design reads. Leaving both schedulers enabled would run two derivation
 * implementations against the same source tables, writing to different destinations, with neither
 * aware of the other's watermark.
 *
 * <p>Re-entrancy is guarded rather than relied on. {@code fixedDelay} does not overlap invocations
 * on its own scheduler thread, but the guard also covers a manual trigger arriving mid-cycle, and
 * two concurrent cycles would lease the same items twice and double-count every metric.
 */
@Component
@Slf4j
@ConditionalOnProperty(name = "vectorsync.runner.enabled", havingValue = "true", matchIfMissing = true)
public class DerivationScheduler {

    private final MaterializationRunner runner;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DerivationScheduler(MaterializationRunner runner) {
        this.runner = runner;
    }

    @Scheduled(fixedDelayString = "${vectorsync.runner.interval-ms:15000}")
    public void run() {
        if (!running.compareAndSet(false, true)) {
            log.debug("Previous derivation cycle still running; skipping this tick");
            return;
        }

        try {
            MaterializationRunner.CycleReport report = runner.runCycle();
            if (report.materializationsSeen() == 0) {
                log.debug("No runnable materializations");
                return;
            }

            // Logged at info only when something happened, so an idle deployment stays quiet and a
            // busy one is legible.
            if (report.filesEnqueued() + report.filesProcessed() + report.filesFailed()
                    + report.watermarksAdvanced() > 0) {
                log.info("Derivation cycle over {} materializations: {} files queued, {} processed, "
                                + "{} failed, {} projections published, {} watermarks advanced",
                        report.materializationsSeen(), report.filesEnqueued(),
                        report.filesProcessed(), report.filesFailed(),
                        report.projectionsPublished(), report.watermarksAdvanced());
            }
        } catch (Exception e) {
            // Never propagate: an exception out of a @Scheduled method kills the cadence for the
            // lifetime of the process, which would silently stop all derivation.
            log.error("Derivation cycle failed; retrying on the next interval: {}", e.getMessage(), e);
        } finally {
            running.set(false);
        }
    }
}
