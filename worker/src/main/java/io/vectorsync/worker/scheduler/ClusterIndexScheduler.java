package io.vectorsync.worker.scheduler;

import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.worker.client.DerivationControlClient;
import io.vectorsync.worker.client.DerivationControlClient.Materialization;
import io.vectorsync.worker.service.derive.ClusterIndexService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Keeps the clustered index following Tier 1 without an operator asking it to.
 *
 * <p>This could not have been written before the coverage digest existed. Every invocation of
 * {@code build} refit the whole scope unconditionally, so a scheduler would have re-run k-means over
 * the entire corpus on every tick regardless of whether anything changed -- burning a full refit to
 * discover there was nothing to do. With coverage recorded, an unchanged scope is a digest
 * comparison and two metadata reads, so calling this on a timer costs almost nothing in the common
 * case. Cheapness in the no-op case is what makes automation safe here, not the automation itself.
 *
 * <p><b>Scopes come from the control plane, not from Tier 3.</b> {@code ClusterIndexService} can
 * enumerate scopes, but only by reading the centroid table -- so it finds scopes that have already
 * been built at least once and can never discover one that should exist. That is circular: it reads
 * the answer off its own output, and a newly admitted materialization would never get an index. The
 * authority on what ought to exist is {@code materializations}, so that is what drives this.
 *
 * <p><b>Cluster count is derived, not configured.</b> The build endpoint defaults to 32, which is
 * defensible for a human who will look at the result and adjust. It is not defensible on a timer:
 * NFCorpus needed 53% of the table at 16 clusters for the quality it reached at 3.8% with 64, so a
 * fixed count maintains an index that prunes nothing while still charging for refits -- worse than
 * having no index at all. {@link ClusterIndexService#suggestedClusters} sizes it from the corpus.
 *
 * <p>Off by default. Tier 3 is the experimental tier, a refit is the most expensive thing this
 * worker can do, and enabling it should be a decision rather than an inheritance.
 */
@Component
@ConditionalOnProperty(name = "vectorsync.cluster.scheduler.enabled", havingValue = "true")
@Slf4j
public class ClusterIndexScheduler {

    private final DerivationControlClient control;
    private final ClusterIndexService clusterIndex;

    /**
     * Deliberately much slower than the derivation cycle's 15s.
     *
     * <p>A no-op tick is cheap but not free -- it reads the coverage row and plans two scans per
     * scope -- and Tier 3 is a serving convenience rather than a correctness requirement, so there
     * is nothing to gain from chasing Tier 1 closely. Five minutes also keeps a scope that is
     * growing steadily from refitting more often than its data justifies.
     */
    public ClusterIndexScheduler(DerivationControlClient control, ClusterIndexService clusterIndex) {
        this.control = control;
        this.clusterIndex = clusterIndex;
    }

    /** Scopes to refresh per tick, so one large warehouse cannot occupy the scheduler thread. */
    @Value("${vectorsync.cluster.scheduler.max-scopes-per-tick:8}")
    private int maxScopesPerTick;

    @Scheduled(fixedDelayString = "${vectorsync.cluster.scheduler.interval-ms:300000}")
    public void refresh() {
        try {
            List<Materialization> live = control.list("LIVE");
            if (live.isEmpty()) {
                return;
            }

            int refreshed = 0;
            int reused = 0;
            int appended = 0;
            int refitted = 0;

            for (Materialization materialization : live) {
                if (refreshed >= maxScopesPerTick) {
                    log.debug("Cluster refresh stopped at {} scopes this tick", refreshed);
                    break;
                }

                MaterializationSpec spec = materialization.getSpec();
                if (spec == null) {
                    continue;
                }

                try {
                    // Sized from what Tier 1 actually holds for this scope rather than from a
                    // constant. A scope with nothing derived yet returns 0 and is skipped: building
                    // an index over no vectors would commit an empty scope and, worse, record
                    // coverage for it, which a later tick would then treat as up to date.
                    long contents = clusterIndex.canonicalCount(
                            spec.modelVersion(), spec.configId());
                    if (contents == 0) {
                        continue;
                    }

                    ClusterIndexService.BuildReport report = clusterIndex.build(
                            spec.getSourceTable(), spec.modelVersion(), spec.configId(),
                            ClusterIndexService.suggestedClusters(contents));

                    refreshed++;
                    if (report.reused()) {
                        reused++;
                    } else if (report.incremental()) {
                        appended++;
                    } else {
                        refitted++;
                    }
                } catch (Exception e) {
                    // Per-scope, for the same reason the derivation cycle isolates per
                    // materialization: one unbuildable scope must not starve every scope behind it.
                    log.warn("Cluster refresh failed for {} / {}: {}",
                            materialization.getSourceTable(),
                            spec.configId(), e.getMessage());
                }
            }

            if (refitted > 0 || appended > 0) {
                log.info("Cluster refresh: {} scopes ({} unchanged, {} appended, {} refitted)",
                        refreshed, reused, appended, refitted);
            }
        } catch (Exception e) {
            // Never propagate: an exception out of a @Scheduled method kills the cadence for the
            // life of the process, which would silently stop every later refresh.
            log.error("Cluster refresh tick failed: {}", e.getMessage(), e);
        }
    }
}
