package io.vectorsync.worker.scheduler;

import io.vectorsync.worker.service.maintenance.MaintenanceService;
import io.vectorsync.worker.service.maintenance.MaintenanceService.MaintenanceReport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs maintenance on a timer, with metadata upkeep on a tighter cycle than data rewrites.
 *
 * <p>Two cadences because the two costs differ by orders of magnitude. Expiring snapshots and
 * coalescing manifests is metadata-only and takes milliseconds, and it needs to be frequent: this
 * worker commits once per derive pass, so an unattended table accumulates snapshots as fast as it
 * derives, and the {@code metadata.json} holding them is re-parsed on every catalog load because
 * caching is deliberately off. Rewriting data files reads and writes the data itself, so it runs
 * rarely and is the operation worth being able to disable independently.
 *
 * <p>Enabled by default, unlike the Tier 3 scheduler. The asymmetry is deliberate: an unmaintained
 * table gets monotonically slower until it is unusable, so leaving this off is not a neutral
 * default the way declining to build an experimental index is. It is still switchable for a
 * deployment that runs compaction from its own Spark maintenance jobs, which many lakehouse
 * installations already do.
 */
@Component
@ConditionalOnProperty(name = "vectorsync.maintenance.scheduler.enabled",
        havingValue = "true", matchIfMissing = true)
@Slf4j
public class MaintenanceScheduler {

    private final MaintenanceService maintenance;

    /**
     * Ticks since the last compaction, counted rather than timed.
     *
     * <p>A second {@code @Scheduled} method would be the obvious way to get a slower cadence, and
     * the wrong one: the two passes would overlap on the tables they touch and one would find its
     * files already replaced. Deriving the slow cadence from the fast one means compaction always
     * happens inside a metadata pass, so there is exactly one writer.
     */
    private final AtomicLong ticks = new AtomicLong();

    /** Metadata passes between data rewrites. Twelve five-minute ticks is hourly by default. */
    @Value("${vectorsync.maintenance.compact-every-n-ticks:12}")
    private long compactEveryNTicks;

    @Value("${vectorsync.maintenance.compact-enabled:true}")
    private boolean compactEnabled;

    public MaintenanceScheduler(MaintenanceService maintenance) {
        this.maintenance = maintenance;
    }

    @Scheduled(fixedDelayString = "${vectorsync.maintenance.interval-ms:300000}",
            initialDelayString = "${vectorsync.maintenance.initial-delay-ms:60000}")
    public void maintain() {
        try {
            long tick = ticks.incrementAndGet();
            boolean compact = compactEnabled
                    && compactEveryNTicks > 0
                    && tick % compactEveryNTicks == 0;

            MaintenanceReport report = maintenance.run(compact);
            if (!report.ran()) {
                log.debug("Maintenance tick skipped: {}", report.note());
                return;
            }
            if (report.totalSnapshotsExpired() > 0 || report.totalFilesRemoved() > 0) {
                log.info("Maintenance tick {}: {} tables, {} snapshots expired, {} fewer files{}",
                        tick, report.tablesVisited(), report.totalSnapshotsExpired(),
                        report.totalFilesRemoved(), compact ? " (with compaction)" : "");
            }
        } catch (Exception e) {
            // A scheduler that throws stops being rescheduled, and maintenance failing is not a
            // reason to stop maintaining: the next tick may well succeed.
            log.warn("Maintenance tick failed: {}", e.getMessage(), e);
        }
    }
}
