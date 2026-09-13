package io.vectorsync.controlplane.controller;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.controlplane.service.TableConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/tables")
@Slf4j
public class TableController {

    private final TableConfigService tableConfigService;

    public TableController(TableConfigService tableConfigService) {
        this.tableConfigService = tableConfigService;
    }

    @PostMapping("/register")
    public ResponseEntity<?> registerTable(@RequestBody TableConfig config) {
        log.info("Received table registration request: {}", config.getTableName());
        try {
            TableConfig registered = tableConfigService.registerTable(config);
            return ResponseEntity.status(HttpStatus.CREATED).body(registered);
        } catch (IllegalArgumentException e) {
            log.warn("Table registration failed - duplicate: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "Duplicate table", "message", e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to register table {}: {}", config.getTableName(), e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Registration failed", "message", e.getMessage()));
        }
    }

    @GetMapping
    public ResponseEntity<List<TableConfig>> getAllTables() {
        return ResponseEntity.ok(tableConfigService.getAllTableConfigs());
    }

    @GetMapping("/{tableId}")
    public ResponseEntity<TableConfig> getTableConfig(@PathVariable("tableId") String tableId) {
        return tableConfigService.getTableConfig(tableId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{tableId}/status")
    public ResponseEntity<SyncStatusResponse> getTableStatus(@PathVariable("tableId") String tableId) {
        return tableConfigService.getTableConfig(tableId)
                .map(config -> {
                    var syncState = tableConfigService.getSyncState(tableId);
                    SyncStatusResponse response = SyncStatusResponse.builder()
                            .tableId(tableId)
                            .tableName(config.getTableName())
                            .enabled(config.isEnabled())
                            .lastSnapshotId(syncState.map(s -> s.getLastSnapshotId()).orElse(null))
                            .lastSyncAt(syncState.map(s -> s.getLastSyncAt()).orElse(null))
                            .build();
                    return ResponseEntity.ok(response);
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Updates a registered table. Changing {@code embeddingVersion} starts a migration: the new
     * version is materialized alongside the existing one rather than replacing it.
     */
    @PutMapping("/{tableId}")
    public ResponseEntity<?> updateTable(@PathVariable("tableId") String tableId,
                                         @RequestBody TableConfig update) {
        try {
            Optional<TableConfig> result = Optional.empty();

            if (update.getEmbeddingVersion() != null && !update.getEmbeddingVersion().isBlank()) {
                result = tableConfigService.setEmbeddingVersion(tableId, update.getEmbeddingVersion());
            }

            return result
                    .or(() -> tableConfigService.getTableConfig(tableId))
                    .<ResponseEntity<?>>map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.notFound().build());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Failed to update table {}: {}", tableId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Update failed", "message", String.valueOf(e.getMessage())));
        }
    }

    @DeleteMapping("/{tableId}")
    public ResponseEntity<?> deleteTable(
            @PathVariable("tableId") String tableId,
            @RequestParam(value = "deleteEmbeddings", defaultValue = "false") boolean deleteEmbeddings) {
        log.info("Received delete request for table ID: {}, deleteEmbeddings: {}", tableId, deleteEmbeddings);
        try {
            boolean deleted = tableConfigService.deleteTable(tableId, deleteEmbeddings);
            if (deleted) {
                return ResponseEntity.ok(Map.of("message", "Table deleted successfully", "tableId", tableId));
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            log.error("Failed to delete table {}: {}", tableId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Delete failed", "message", e.getMessage()));
        }
    }
}
