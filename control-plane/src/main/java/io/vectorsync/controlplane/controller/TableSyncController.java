package io.vectorsync.controlplane.controller;

import io.vectorsync.controlplane.entity.DiscoveredTableEntity;
import io.vectorsync.controlplane.entity.SyncJobEntity;
import io.vectorsync.controlplane.repository.DiscoveredTableRepository;
import io.vectorsync.controlplane.repository.SyncJobRepository;
import io.vectorsync.controlplane.service.IcebergTableDiscoveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/tables")
@RequiredArgsConstructor
@Slf4j
public class TableSyncController {
    
    private final IcebergTableDiscoveryService discoveryService;
    private final SyncJobRepository syncJobRepository;
    private final DiscoveredTableRepository discoveredTableRepository;
    
    /**
     * Trigger table sync from S3
     */
    @PostMapping("/sync")
    public ResponseEntity<SyncResponse> syncTables(@RequestBody SyncRequest request) {
        log.info("Received sync request for catalog: {} at path: {}", 
                 request.getCatalogName(), request.getS3Path());
        
        // Validate request
        if (request.getCatalogName() == null || request.getCatalogName().isEmpty()) {
            return ResponseEntity.badRequest().body(
                    SyncResponse.builder()
                            .success(false)
                            .message("Catalog name is required")
                            .build());
        }
        
        if (request.getS3Path() == null || request.getS3Path().isEmpty()) {
            return ResponseEntity.badRequest().body(
                    SyncResponse.builder()
                            .success(false)
                            .message("S3 path is required")
                            .build());
        }
        
        // Generate job ID
        String jobId = UUID.randomUUID().toString();
        
        // Create Hadoop configuration
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.access.key", request.getAwsAccessKey());
        hadoopConf.set("fs.s3a.secret.key", request.getAwsSecretKey());
        hadoopConf.set("fs.s3a.endpoint", request.getAwsEndpoint() != null ?
                request.getAwsEndpoint() : "s3.amazonaws.com");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        
        // Set region if provided (optional, will be auto-detected from endpoint if not provided)
        if (request.getAwsRegion() != null && !request.getAwsRegion().isEmpty()) {
            hadoopConf.set("fs.s3a.region", request.getAwsRegion());
        }
        
        // Start async discovery
        discoveryService.discoverAndSyncTables(
                jobId,
                request.getCatalogName(),
                request.getS3Path(),
                request.isSyncExistingTables(),
                request.isRegisterNewTables(),
                request.getCreatedBy(),
                hadoopConf
        );
        
        return ResponseEntity.accepted().body(
                SyncResponse.builder()
                        .success(true)
                        .jobId(jobId)
                        .message("Table sync started")
                        .build());
    }
    
    /**
     * Get sync job status
     */
    @GetMapping("/sync/status/{jobId}")
    public ResponseEntity<SyncJobEntity> getSyncStatus(@PathVariable String jobId) {
        return syncJobRepository.findById(jobId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * Get all sync jobs
     */
    @GetMapping("/sync/jobs")
    public ResponseEntity<List<SyncJobEntity>> getAllSyncJobs() {
        List<SyncJobEntity> jobs = syncJobRepository.findAllByOrderByStartedAtDesc();
        return ResponseEntity.ok(jobs);
    }
    
    /**
     * Get discovered tables for a sync job
     */
    @GetMapping("/sync/{jobId}/tables")
    public ResponseEntity<List<DiscoveredTableEntity>> getDiscoveredTables(@PathVariable String jobId) {
        List<DiscoveredTableEntity> tables = discoveredTableRepository.findBySyncJobId(jobId);
        return ResponseEntity.ok(tables);
    }
    
    /**
     * Get all discovered tables
     */
    @GetMapping("/discovered")
    public ResponseEntity<List<DiscoveredTableEntity>> getAllDiscoveredTables(
            @RequestParam(required = false) Boolean registered) {
        
        List<DiscoveredTableEntity> tables;
        if (registered != null) {
            tables = discoveredTableRepository.findByRegistered(registered);
        } else {
            tables = discoveredTableRepository.findAll();
        }
        return ResponseEntity.ok(tables);
    }
    
    /**
     * Get discovered table by UUID
     */
    @GetMapping("/discovered/{uuid}")
    public ResponseEntity<DiscoveredTableEntity> getDiscoveredTable(@PathVariable String uuid) {
        return discoveredTableRepository.findByUuid(uuid)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * Mark discovered table as registered
     */
    @PutMapping("/discovered/{uuid}/register")
    public ResponseEntity<DiscoveredTableEntity> markAsRegistered(@PathVariable String uuid) {
        return discoveredTableRepository.findByUuid(uuid)
                .map(table -> {
                    table.setRegistered(true);
                    table.setRegisteredAt(java.time.LocalDateTime.now());
                    discoveredTableRepository.save(table);
                    return ResponseEntity.ok(table);
                })
                .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * Delete discovered table
     */
    @DeleteMapping("/discovered/{uuid}")
    public ResponseEntity<Void> deleteDiscoveredTable(@PathVariable String uuid) {
        return discoveredTableRepository.findByUuid(uuid)
                .map(table -> {
                    discoveredTableRepository.delete(table);
                    return ResponseEntity.ok().<Void>build();
                })
                .orElse(ResponseEntity.notFound().build());
    }
    
    // Request/Response DTOs
    
    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class SyncRequest {
        private String catalogName;
        private String s3Path;
        private boolean syncExistingTables;
        private boolean registerNewTables;
        private String createdBy;
        private String awsAccessKey;
        private String awsSecretKey;
        private String awsEndpoint;
        private String awsRegion; // Optional: AWS region (e.g., "us-east-1", "us-west")
    }
    
    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class SyncResponse {
        private boolean success;
        private String jobId;
        private String message;
    }
}

// Made with Bob
