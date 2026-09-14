package io.vectorsync.controlplane.scheduler;

import io.vectorsync.controlplane.service.WorkQueueService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Returns work items whose lease expired to the queue.
 *
 * <p>Without this the queue is durable but not resumable, which is a distinction that only shows up
 * during a failure: a worker that crashes mid-file leaves its items LEASED forever, and because a
 * leased item is invisible to {@code lease()}, those files are never retried and never reported as
 * failed. The backfill simply stops short and looks finished. {@code reclaimExpiredLeases} existed
 * but nothing called it, and a {@code @Scheduled} method is inert unless scheduling is enabled
 * somewhere in the context -- which it was not.
 *
 * <p>The interval only needs to be well under the lease duration; reclaiming late costs latency,
 * reclaiming a live lease would cost correctness, and that decision belongs to the lease expiry
 * timestamp rather than to this schedule.
 */
@Component
@Slf4j
public class LeaseReaper {

    private final WorkQueueService workQueue;

    public LeaseReaper(WorkQueueService workQueue) {
        this.workQueue = workQueue;
    }

    @Scheduled(fixedDelayString = "${vectorsync.queue.reclaim-interval-ms:30000}")
    public void reclaim() {
        try {
            int reclaimed = workQueue.reclaimExpiredLeases();
            if (reclaimed > 0) {
                log.info("Reclaimed {} work items from expired leases", reclaimed);
            }
        } catch (Exception e) {
            // Swallowed deliberately: a failure here must not kill the scheduler thread, or the
            // queue silently loses its only recovery mechanism for the lifetime of the process.
            log.error("Lease reclaim failed; will retry on the next interval: {}", e.getMessage());
        }
    }
}
