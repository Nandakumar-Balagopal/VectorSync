package io.vectorsync.controlplane.service;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.controlplane.entity.SyncStateEntity;
import io.vectorsync.controlplane.entity.TableConfigEntity;
import io.vectorsync.controlplane.repository.SyncStateRepository;
import io.vectorsync.controlplane.repository.TableConfigRepository;
import io.vectorsync.controlplane.service.iceberg.VectorSyncAdminService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TableConfigService {

    private final TableConfigRepository tableConfigRepository;
    private final SyncStateRepository syncStateRepository;
    private final VectorSyncAdminService vectorSyncAdminService;

    public TableConfigService(TableConfigRepository tableConfigRepository,
                              SyncStateRepository syncStateRepository,
                              VectorSyncAdminService vectorSyncAdminService) {
        this.tableConfigRepository = tableConfigRepository;
        this.syncStateRepository = syncStateRepository;
        this.vectorSyncAdminService = vectorSyncAdminService;
    }

    public TableConfig registerTable(TableConfig config) {
        log.info("Registering table: {}", config.getTableName());

        try {
            String tableId = UUID.randomUUID().toString();

            TableConfigEntity entity = TableConfigEntity.builder()
                    .tableId(tableId)
                    .catalog(config.getCatalog())
                    .tableName(config.getTableName())
                    .embeddingColumns(config.getEmbeddingColumns() == null ? "" : String.join(",", config.getEmbeddingColumns()))
                    .modelName(config.getModelName())
                    .embeddingVersion(config.embeddingVersionOrDefault())
                    .enabled(config.isEnabled())
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            tableConfigRepository.save(entity);

            SyncStateEntity syncState = SyncStateEntity.builder()
                    .tableId(tableId)
                    .lastSnapshotId(null)
                    .lastSyncAt(Instant.now())
                    .build();

            syncStateRepository.save(syncState);

            log.info("Table registered with ID: {}", tableId);
            return toDto(entity);
        } catch (DataIntegrityViolationException e) {
            String errorMsg = String.format("Table '%s' in catalog '%s' is already registered",
                config.getTableName(), config.getCatalog());
            log.error(errorMsg, e);
            throw new IllegalArgumentException(errorMsg, e);
        } catch (Exception e) {
            log.error("Failed to register table {}: {}", config.getTableName(), e.getMessage(), e);
            throw new RuntimeException("Failed to register table: " + e.getMessage(), e);
        }
    }

    public Optional<TableConfig> getTableConfig(String tableId) {
        return tableConfigRepository.findById(tableId).map(this::toDto);
    }

    /**
     * Repoints a table at a new embedding version.
     *
     * <p>Resets the sync watermark, because a new embedding version is a fresh materialization of
     * data the worker has already seen. Without the reset, incremental CDC would find no source
     * changes and the new version would never be produced. Existing versions are untouched, so
     * both coexist and the new one can be evaluated before promotion.
     */
    public Optional<TableConfig> setEmbeddingVersion(String tableId, String embeddingVersion) {
        if (embeddingVersion == null || embeddingVersion.isBlank()) {
            throw new IllegalArgumentException("embeddingVersion is required");
        }

        return tableConfigRepository.findById(tableId).map(entity -> {
            String previous = entity.getEmbeddingVersion();
            entity.setEmbeddingVersion(embeddingVersion);
            entity.setUpdatedAt(Instant.now());
            tableConfigRepository.save(entity);

            if (!embeddingVersion.equals(previous)) {
                syncStateRepository.findById(tableId).ifPresent(state -> {
                    state.setLastSnapshotId(null);
                    state.setLastSyncAt(Instant.now());
                    syncStateRepository.save(state);
                });
                log.info("Table {} embedding version {} -> {}; sync watermark reset for re-materialization",
                        tableId, previous, embeddingVersion);
            }

            return toDto(entity);
        });
    }

    public Optional<TableConfig> setEnabled(String tableId, boolean enabled) {
        return tableConfigRepository.findById(tableId).map(entity -> {
            entity.setEnabled(enabled);
            entity.setUpdatedAt(Instant.now());
            tableConfigRepository.save(entity);
            return toDto(entity);
        });
    }

    public List<TableConfig> getAllTableConfigs() {
        return tableConfigRepository.findAll()
                .stream()
                .map(this::toDto)
                .collect(Collectors.toList());
    }

    public Optional<SyncStateEntity> getSyncState(String tableId) {
        return syncStateRepository.findById(tableId);
    }

    public void updateSyncState(String tableId, Long snapshotId, Instant syncTime) {
        SyncStateEntity syncState = syncStateRepository.findById(tableId)
                .orElse(SyncStateEntity.builder().tableId(tableId).build());

        syncState.setLastSnapshotId(snapshotId);
        syncState.setLastSyncAt(syncTime == null ? Instant.now() : syncTime);

        syncStateRepository.save(syncState);
    }

    public boolean deleteTable(String tableId, boolean deleteEmbeddings) {
        Optional<TableConfigEntity> entity = tableConfigRepository.findById(tableId);
        if (entity.isEmpty()) {
            return false;
        }

        if (deleteEmbeddings) {
            vectorSyncAdminService.deleteEmbeddingsForTable(entity.get().getTableName());
        }

        syncStateRepository.findById(tableId).ifPresent(syncStateRepository::delete);
        tableConfigRepository.delete(entity.get());
        return true;
    }

    private TableConfig toDto(TableConfigEntity entity) {
        List<String> columns = entity.getEmbeddingColumns() == null || entity.getEmbeddingColumns().isBlank()
            ? List.of()
            : Arrays.stream(entity.getEmbeddingColumns().split(","))
            .map(String::trim)
            .collect(Collectors.toList());

        return TableConfig.builder()
                .tableId(entity.getTableId())
                .catalog(entity.getCatalog())
                .tableName(entity.getTableName())
                .embeddingColumns(columns)
                .modelName(entity.getModelName())
                .embeddingVersion(entity.getEmbeddingVersion())
                .enabled(entity.isEnabled())
                .createdAt(entity.getCreatedAt())
                .build();
    }
}
