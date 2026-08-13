package io.vectorsync.worker.controller;

import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.worker.service.VectorStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/vectors")
@Slf4j
public class VectorController {

    private final VectorStoreService vectorStoreService;

    public VectorController(VectorStoreService vectorStoreService) {
        this.vectorStoreService = vectorStoreService;
    }

    @GetMapping
    public ResponseEntity<List<VectorRecord>> getAllVectors() {
        List<VectorRecord> vectors = vectorStoreService.getAllVectors();
        log.debug("Returning {} vectors", vectors.size());
        return ResponseEntity.ok(vectors);
    }

    @GetMapping("/count")
    public ResponseEntity<Long> getVectorCount() {
        long count = vectorStoreService.getVectorCount();
        return ResponseEntity.ok(count);
    }

    @GetMapping("/table/{tableName}")
    public ResponseEntity<List<VectorRecord>> getVectorsByTable(@PathVariable("tableName") String tableName) {
        List<VectorRecord> vectors = vectorStoreService.getVectorsByTable(tableName);
        return ResponseEntity.ok(vectors);
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Worker is healthy");
    }
}
