package io.vectorsync.worker.service;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.common.dto.VectorRecord;
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

    private void syncTableInternal(TableConfig tableConfig, Long lastSnapshotId, boolean updateSyncState) {
        log.info("Starting sync for table: {}", tableConfig.getTableName());

        try {
            CdcResult cdcResult = icebergCdcService.detectChanges(tableConfig, lastSnapshotId);
            List<io.vectorsync.common.dto.ChangeEvent> changeEvents = cdcResult.getChangeEvents();

            if (changeEvents.isEmpty()) {
                log.debug("No changes detected for table: {}", tableConfig.getTableName());
                if (updateSyncState && cdcResult.getCurrentSnapshotId() != null) {
                    controlApiClient.updateSyncState(
                            tableConfig.getTableId(),
                            cdcResult.getCurrentSnapshotId(),
                            Instant.now()
                    );
                }
                return;
            }

            log.info("Detected {} changes in table: {}", changeEvents.size(), tableConfig.getTableName());

            List<VectorRecord> vectorRecords = cdcService.processChangeEvents(tableConfig, changeEvents);

            if (!vectorRecords.isEmpty()) {
                vectorStoreService.writeVectors(vectorRecords);
                log.info("Written {} vectors for table: {}", vectorRecords.size(), tableConfig.getTableName());

                if (updateSyncState && cdcResult.getCurrentSnapshotId() != null) {
                    controlApiClient.updateSyncState(
                            tableConfig.getTableId(),
                            cdcResult.getCurrentSnapshotId(),
                            Instant.now()
                    );
                }
            }
        } catch (Exception e) {
            log.error("Error syncing table {}: {}", tableConfig.getTableName(), e.getMessage(), e);
            throw new RuntimeException("Failed to sync table: " + tableConfig.getTableName(), e);
        }
    }
}
