package io.vectorsync.worker.scheduler;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.worker.client.ControlApiClient;
import io.vectorsync.worker.service.SyncOrchestrationService;
import io.vectorsync.worker.service.VectorStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
@Slf4j
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

    @Scheduled(fixedDelayString = "${worker.sync.interval:30000}")
    public void syncAllTables() {
        try {
            log.debug("Starting scheduled sync cycle");

            List<TableConfig> tableConfigs = fetchTableConfigs();
            if (tableConfigs.isEmpty()) {
                log.debug("No table configs found");
                return;
            }

            for (TableConfig config : tableConfigs) {
                if (config.isEnabled()) {
                    syncOrchestrationService.syncTable(config);
                }
            }

            log.info("Sync cycle completed. Total vectors in store: {}", vectorStoreService.getVectorCount());
        } catch (Exception e) {
            log.error("Error in sync scheduler: {}", e.getMessage());
        }
    }

    private List<TableConfig> fetchTableConfigs() {
        return controlApiClient.getTableConfigs();
    }
}
