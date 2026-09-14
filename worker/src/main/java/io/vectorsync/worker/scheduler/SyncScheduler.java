package io.vectorsync.worker.scheduler;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.worker.client.ControlApiClient;
import io.vectorsync.worker.service.SyncOrchestrationService;
import io.vectorsync.worker.service.VectorStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.List;

/**
 * The legacy row-keyed sync path, off by default.
 *
 * <p>Superseded by {@link DerivationScheduler}. Its change detection diffs two fully-materialized
 * snapshots in memory, and it writes the old row-keyed vector table that the content-addressed
 * design does not read. Running both schedulers means two derivation implementations over the same
 * sources, writing to different destinations, neither aware of the other's watermark -- so this is
 * opt-in via {@code vectorsync.legacy-sync.enabled} and exists only for comparison and for
 * deployments still serving from the old table.
 */
@Component
@Slf4j
@ConditionalOnProperty(name = "vectorsync.legacy-sync.enabled", havingValue = "true")
public class SyncScheduler {

    private final SyncOrchestrationService syncOrchestrationService;
    private final VectorStoreService vectorStoreService;
    private final ControlApiClient controlApiClient;

    public SyncScheduler(SyncOrchestrationService syncOrchestrationService,
                         VectorStoreService vectorStoreService,
                         ControlApiClient controlApiClient) {
        this.syncOrchestrationService = syncOrchestrationService;
        this.vectorStoreService = vectorStoreService;
        this.controlApiClient = controlApiClient;
    }

    /**
     * Syncs every enabled table, isolating failures to the table that caused them.
     *
     * <p>The loop used to sit inside a single try/catch while {@code syncTable} rethrows, so the
     * first table that failed aborted the cycle and every table after it was skipped. Table order
     * is stable, so one permanently broken table starved all of its successors indefinitely --
     * silently, because the log line looked like a single table's error.
     */
    @Scheduled(fixedDelayString = "${worker.sync.interval:30000}")
    public void syncAllTables() {
        log.debug("Starting scheduled sync cycle");

        List<TableConfig> tableConfigs;
        try {
            tableConfigs = fetchTableConfigs();
        } catch (Exception e) {
            log.error("Could not fetch table configs; skipping this cycle: {}", e.getMessage());
            return;
        }

        if (tableConfigs.isEmpty()) {
            log.debug("No table configs found");
            return;
        }

        int synced = 0;
        int failed = 0;
        for (TableConfig config : tableConfigs) {
            if (!config.isEnabled()) {
                continue;
            }
            try {
                syncOrchestrationService.syncTable(config);
                synced++;
            } catch (Exception e) {
                failed++;
                log.error("Sync failed for table {}; continuing with the rest of the cycle: {}",
                        config.getTableName(), e.getMessage(), e);
            }
        }

        // Deliberately not reporting a total vector count here: that is a full scan of the vector
        // table, and it ran on every cycle purely to produce a log line.
        if (failed > 0) {
            log.warn("Sync cycle completed: {} tables synced, {} failed", synced, failed);
        } else {
            log.info("Sync cycle completed: {} tables synced", synced);
        }
    }

    private List<TableConfig> fetchTableConfigs() {
        return controlApiClient.getTableConfigs();
    }
}
