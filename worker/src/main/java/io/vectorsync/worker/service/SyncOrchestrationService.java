package io.vectorsync.worker.service;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.worker.client.ControlApiClient;
import io.vectorsync.worker.client.SyncStateResponse;
import io.vectorsync.worker.service.iceberg.CdcResult;
import io.vectorsync.worker.service.iceberg.IcebergCdcService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@Slf4j
public class SyncOrchestrationService {

    private final CDCService cdcService;
    private final VectorStoreService vectorStoreService;
    private final ControlApiClient controlApiClient;
    private final IcebergCdcService icebergCdcService;

    public SyncOrchestrationService(CDCService cdcService,
                                    VectorStoreService vectorStoreService,
                                    ControlApiClient controlApiClient,
                                    IcebergCdcService icebergCdcService) {
        this.cdcService = cdcService;
        this.vectorStoreService = vectorStoreService;
        this.controlApiClient = controlApiClient;
        this.icebergCdcService = icebergCdcService;
    }

    public void syncTable(TableConfig tableConfig) {
        SyncStateResponse syncState = controlApiClient.getSyncState(tableConfig.getTableId());
        Long lastSnapshotId = syncState != null ? syncState.getLastSnapshotId() : null;
        syncTableInternal(tableConfig, lastSnapshotId, true);
    }

    public void syncTableForceFull(TableConfig tableConfig) {
        syncTableInternal(tableConfig, null, true);
    }

    /**
     * Materializes one table up to its current snapshot.
     *
     * <p>The watermark advances only when every change event for the snapshot materialized and the
     * vector write committed. Advancing on partial success silently loses those changes forever,
     * since the next cycle diffs from the advanced watermark.
     */
    private void syncTableInternal(TableConfig tableConfig, Long lastSnapshotId, boolean updateSyncState) {
        log.info("Starting sync for table: {}", tableConfig.getTableName());

        try {
            CdcResult cdcResult = icebergCdcService.detectChanges(tableConfig, lastSnapshotId);
            List<ChangeEvent> changeEvents = cdcResult.getChangeEvents();
            Long currentSnapshotId = cdcResult.getCurrentSnapshotId();

            if (changeEvents.isEmpty()) {
                log.debug("No changes detected for table: {}", tableConfig.getTableName());
                advance(tableConfig, currentSnapshotId, updateSyncState);
                return;
            }

            log.info("Detected {} changes in table: {}", changeEvents.size(), tableConfig.getTableName());

            CDCService.MaterializationResult result = cdcService.processChangeEvents(tableConfig, changeEvents);

            if (!result.records().isEmpty()) {
                vectorStoreService.writeVectors(result.records());
                log.info("Written {} vectors for table: {}", result.records().size(), tableConfig.getTableName());
            }

            if (result.complete()) {
                advance(tableConfig, currentSnapshotId, updateSyncState);
            } else {
                log.warn("Holding sync watermark for {} at {}: {} of {} change events failed to materialize",
                        tableConfig.getTableName(), lastSnapshotId, result.failed(), changeEvents.size());
            }
        } catch (Exception e) {
            log.error("Error syncing table {}: {}", tableConfig.getTableName(), e.getMessage(), e);
            throw new RuntimeException("Failed to sync table: " + tableConfig.getTableName(), e);
        }
    }

    private void advance(TableConfig tableConfig, Long currentSnapshotId, boolean updateSyncState) {
        if (updateSyncState && currentSnapshotId != null) {
            controlApiClient.updateSyncState(tableConfig.getTableId(), currentSnapshotId, Instant.now());
        }
    }
}
