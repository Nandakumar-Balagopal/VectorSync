package io.vectorsync.controlplane.service;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.controlplane.entity.SyncStateEntity;
import io.vectorsync.controlplane.entity.TableConfigEntity;
import io.vectorsync.controlplane.repository.SyncStateRepository;
import io.vectorsync.controlplane.repository.TableConfigRepository;
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

    public TableConfigService(TableConfigRepository tableConfigRepository,
                              SyncStateRepository syncStateRepository) {
        this.tableConfigRepository = tableConfigRepository;
        this.syncStateRepository = syncStateRepository;
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
        return setEmbedding(tableId, null, embeddingVersion);
    }

    /**
     * Repoints a table at a new embedding model and/or version.
     *
     * <p>A real migration changes the model, not just a version label, so both are settable. The
     * sync watermark resets whenever either changes, because the new combination is a fresh
     * materialization of data the worker has already seen -- without the reset, incremental CDC
     * finds no source changes and the new embeddings are never produced. Existing combinations
     * are untouched, so they coexist and the candidate can be evaluated before promotion.
     */
    public Optional<TableConfig> setEmbedding(String tableId, String modelName, String embeddingVersion) {
        boolean changingModel = modelName != null && !modelName.isBlank();
        boolean changingVersion = embeddingVersion != null && !embeddingVersion.isBlank();
        if (!changingModel && !changingVersion) {
            throw new IllegalArgumentException("modelName or embeddingVersion is required");
        }

        return tableConfigRepository.findById(tableId).map(entity -> {
            String previous = entity.getModelName() + ":" + entity.getEmbeddingVersion();

            if (changingModel) {
                entity.setModelName(modelName);
            }
            if (changingVersion) {
                entity.setEmbeddingVersion(embeddingVersion);
            }
            entity.setUpdatedAt(Instant.now());
            tableConfigRepository.save(entity);

            String current = entity.getModelName() + ":" + entity.getEmbeddingVersion();
            if (!current.equals(previous)) {
                syncStateRepository.findById(tableId).ifPresent(state -> {
                    state.setLastSnapshotId(null);
                    state.setLastSyncAt(Instant.now());
                    syncStateRepository.save(state);
                });
                log.info("Table {} embedding {} -> {}; sync watermark reset for re-materialization",
                        tableId, previous, current);
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
            // Refused rather than ignored. Vectors are keyed by (content_hash, model_version,
            // config_id) and deduplicated across every table, so "delete this table's embeddings"
            // has no well-defined meaning: the rows backing this table are the same rows backing
            // any other table whose text hashed identically, and removing them would blank vectors
            // belonging to datasets unrelated to this one. Because the store is what makes
            // re-embedding cheap, the damage would only surface as a search returning nothing.
            //
            // Retiring a materialization is the supported operation. It withdraws this
            // configuration's mapping and leaves shared content alone, and its purge flag marks
            // rows for the reclaim sweeper -- the only component with the global view needed to
            // decide that content is referenced by nothing.
            throw new IllegalArgumentException(
                    "deleteEmbeddings is not supported: vectors are content-addressed and shared "
                            + "across tables, so deleting them per table would remove other "
                            + "datasets' vectors. Retire the materialization instead "
                            + "(POST /api/materializations/{id}/retire).");
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
