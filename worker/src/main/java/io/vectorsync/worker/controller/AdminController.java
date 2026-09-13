package io.vectorsync.worker.controller;

import io.vectorsync.common.Constants;
import io.vectorsync.worker.service.iceberg.IcebergTableService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Explicit, operator-initiated destructive operations.
 *
 * <p>Embeddings are derived data and can always be rebuilt from source plus model, but dropping
 * them must be a deliberate act. Earlier code silently dropped the vector table whenever it found
 * an unexpected partition spec, which is incompatible with treating the table as a system of
 * record.
 */
@RestController
@RequestMapping("/api/admin")
@Slf4j
public class AdminController {

    private final IcebergTableService icebergTableService;

    public AdminController(IcebergTableService icebergTableService) {
        this.icebergTableService = icebergTableService;
    }

    /**
     * Drops the vector table so the next sync recreates it at the current format version. Sync
     * state is not reset here: a full re-materialization is triggered by the caller re-registering
     * the table or forcing a full sync.
     */
    @PostMapping("/vector-table/rebuild")
    public ResponseEntity<Map<String, Object>> rebuildVectorTable() {
        log.warn("Operator requested vector table rebuild; dropping existing vectors");
        boolean dropped = icebergTableService.dropVectorTable();

        return ResponseEntity.ok(Map.of(
                "dropped", dropped,
                "formatVersion", Constants.VECTOR_FORMAT_VERSION,
                "message", dropped
                        ? "Vector table dropped. Run a full sync to re-materialize embeddings."
                        : "No vector table existed. Run a sync to create it."));
    }
}
