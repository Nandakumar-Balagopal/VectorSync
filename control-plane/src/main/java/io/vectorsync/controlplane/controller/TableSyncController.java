package io.vectorsync.controlplane.controller;

import io.vectorsync.controlplane.entity.DiscoveredTableEntity;
import io.vectorsync.controlplane.entity.SyncJobEntity;
import io.vectorsync.controlplane.repository.DiscoveredTableRepository;
import io.vectorsync.controlplane.repository.SyncJobRepository;
import io.vectorsync.controlplane.service.IcebergTableDiscoveryService;
import io.vectorsync.controlplane.service.iceberg.IcebergCatalogService;
import io.vectorsync.format.catalog.IcebergCatalogConfig;
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
    private final IcebergCatalogService catalogService;
    
    /**
     * Trigger table sync from S3
     */
    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }

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
        
        // Credentials and the endpoint they are sent to are ONE group, taken entirely from the
        // request or entirely from configuration. Never mixed.
        //
        // Mixing them was a credential-exfiltration hole. An earlier version filled in missing
        // credentials from the control plane's own configuration -- convenient, so the dashboard
        // need not handle secrets -- while still honoring a request-supplied endpoint. A caller who
        // sent {"awsEndpoint":"http://attacker/"} and no credentials therefore made the control
        // plane authenticate to an arbitrary host using the warehouse's own long-lived keys, in
        // plaintext. The endpoint decides where the secret travels, so whoever supplies the
        // endpoint must supply the secret.
        IcebergCatalogConfig configured = catalogService.config();
        boolean requestHasCredentials =
                isPresent(request.getAwsAccessKey()) || isPresent(request.getAwsSecretKey());
        boolean requestHasEndpoint =
                isPresent(request.getAwsEndpoint()) || isPresent(request.getAwsRegion());

        String accessKey;
        String secretKey;
        String endpoint;
        String region;

        if (requestHasCredentials) {
            if (!isPresent(request.getAwsAccessKey()) || !isPresent(request.getAwsSecretKey())) {
                return ResponseEntity.badRequest().body(SyncResponse.builder()
                        .success(false)
                        .message("Supply both awsAccessKey and awsSecretKey, or neither")
                        .build());
            }
            accessKey = request.getAwsAccessKey();
            secretKey = request.getAwsSecretKey();
            endpoint = firstNonBlank(request.getAwsEndpoint(), configured.getS3Endpoint());
            region = firstNonBlank(request.getAwsRegion(), configured.getS3Region());
        } else {
            if (requestHasEndpoint) {
                return ResponseEntity.badRequest().body(SyncResponse.builder()
                        .success(false)
                        .message("awsEndpoint and awsRegion may only be set together with "
                                + "awsAccessKey and awsSecretKey. Directing the control plane's own "
                                + "credentials at a caller-chosen endpoint would disclose them.")
                        .build());
            }
            accessKey = configured.getS3AccessKey();
            secretKey = configured.getS3SecretKey();
            endpoint = configured.getS3Endpoint();
            region = configured.getS3Region();
        }

        if (!isPresent(accessKey) || !isPresent(secretKey)) {
            return ResponseEntity.badRequest().body(
                    SyncResponse.builder()
                            .success(false)
                            .message("No object-store credentials: supply awsAccessKey and "
                                    + "awsSecretKey, or configure AWS_S3_ACCESS_KEY and "
                                    + "AWS_S3_SECRET_KEY on the control plane")
                            .build());
        }

        String jobId = UUID.randomUUID().toString();

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.access.key", accessKey);
        hadoopConf.set("fs.s3a.secret.key", secretKey);
        hadoopConf.set("fs.s3a.endpoint", endpoint == null ? "s3.amazonaws.com" : endpoint);
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        // Follows the endpoint rather than being pinned off. Hardcoding it disabled TLS even for a
        // real https endpoint, so credentials crossed the network in the clear.
        boolean plaintext = endpoint != null && endpoint.startsWith("http://");
        hadoopConf.set("fs.s3a.connection.ssl.enabled", String.valueOf(!plaintext));

        if (region != null) {
            hadoopConf.set("fs.s3a.region", region);
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
    
    private static String firstNonBlank(String preferred, String fallback) {
        if (preferred != null && !preferred.isBlank()) {
            return preferred;
        }
        return fallback == null || fallback.isBlank() ? null : fallback;
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
