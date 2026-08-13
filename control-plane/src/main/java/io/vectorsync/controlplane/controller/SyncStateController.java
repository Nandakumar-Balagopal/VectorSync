package io.vectorsync.controlplane.controller;

import io.vectorsync.controlplane.service.TableConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/sync-state")
@Slf4j
public class SyncStateController {

    private final TableConfigService tableConfigService;

    public SyncStateController(TableConfigService tableConfigService) {
        this.tableConfigService = tableConfigService;
    }

    @GetMapping("/{tableId}")
    public ResponseEntity<SyncStateResponse> getSyncState(@PathVariable("tableId") String tableId) {
        return tableConfigService.getSyncState(tableId)
                .map(state -> ResponseEntity.ok(SyncStateResponse.builder()
                        .tableId(tableId)
                        .lastSnapshotId(state.getLastSnapshotId())
                        .lastSyncAt(state.getLastSyncAt())
                        .build()))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{tableId}")
    public ResponseEntity<Void> updateSyncState(@PathVariable("tableId") String tableId,
                                                @RequestBody SyncStateUpdateRequest request) {
        if (request == null) {
            return ResponseEntity.badRequest().build();
        }

        log.debug("Updating sync state for {} to snapshot {}", tableId, request.getLastSnapshotId());
        tableConfigService.updateSyncState(tableId, request.getLastSnapshotId(), request.getLastSyncAt());
        return ResponseEntity.ok().build();
    }
}
