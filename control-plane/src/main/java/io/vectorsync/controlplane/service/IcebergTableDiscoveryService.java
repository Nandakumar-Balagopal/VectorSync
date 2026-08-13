package io.vectorsync.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vectorsync.controlplane.entity.DiscoveredTableEntity;
import io.vectorsync.controlplane.entity.SyncJobEntity;
import io.vectorsync.controlplane.repository.DiscoveredTableRepository;
import io.vectorsync.controlplane.repository.SyncJobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
@RequiredArgsConstructor
public class IcebergTableDiscoveryService {
    
    private final DiscoveredTableRepository discoveredTableRepository;
    private final SyncJobRepository syncJobRepository;
    private final ObjectMapper objectMapper;
    
    private static final int THREAD_POOL_SIZE = 4;
    
    /**
     * Asynchronously discover and sync Iceberg tables from S3
     */
    @Async
    @Transactional
    public void discoverAndSyncTables(String jobId, String catalogName, String s3Path,
                                     boolean syncExistingTables, boolean registerNewTables,
                                     String createdBy, Configuration hadoopConf) {
        
        log.info("Starting table discovery job: {} for catalog: {} at path: {}", 
                 jobId, catalogName, s3Path);
        
        SyncJobEntity syncJob = SyncJobEntity.builder()
                .jobId(jobId)
                .catalogName(catalogName)
                .s3Path(s3Path)
                .status(SyncJobEntity.SyncStatus.RUNNING)
                .syncExistingTables(syncExistingTables)
                .registerNewTables(registerNewTables)
                .createdBy(createdBy)
                .startedAt(LocalDateTime.now())
                .tablesDiscovered(0)
                .tablesRegistered(0)
                .tablesUpdated(0)
                .tablesFailed(0)
                .build();
        
        syncJobRepository.save(syncJob);
        
        try {
            // Step 1: Discover all Iceberg metadata files
            Map<Path, FileStatus> metadataFiles = discoverMetadataFiles(s3Path, hadoopConf);
            log.info("Discovered {} metadata files for job: {}", metadataFiles.size(), jobId);
            
            syncJob.setTablesDiscovered(metadataFiles.size());
            syncJobRepository.save(syncJob);
            
            // Step 2: Process metadata files in parallel
            List<DiscoveredTableEntity> discoveredTables = processMetadataFiles(
                    metadataFiles, catalogName, jobId, s3Path, hadoopConf);
            
            // Step 3: Save discovered tables
            discoveredTableRepository.saveAll(discoveredTables);
            
            // Step 4: Update sync job status
            syncJob.setStatus(SyncJobEntity.SyncStatus.COMPLETED);
            syncJob.setCompletedAt(LocalDateTime.now());
            syncJob.setTablesRegistered(discoveredTables.size());
            syncJobRepository.save(syncJob);
            
            log.info("Table discovery job completed: {}. Discovered {} tables", 
                     jobId, discoveredTables.size());
            
        } catch (Exception e) {
            log.error("Table discovery job failed: {}", jobId, e);
            syncJob.setStatus(SyncJobEntity.SyncStatus.FAILED);
            syncJob.setCompletedAt(LocalDateTime.now());
            syncJob.setErrorMessage(e.getMessage());
            syncJob.setErrorDetails(getStackTrace(e));
            syncJobRepository.save(syncJob);
        }
    }
    
    /**
     * Discover all Iceberg metadata.json files in S3
     */
    private Map<Path, FileStatus> discoverMetadataFiles(String s3Path, Configuration hadoopConf) 
            throws Exception {
        
        Map<Path, FileStatus> metadataFiles = new HashMap<>();
        Path rootPath = new Path(s3Path);
        
        try (FileSystem fs = FileSystem.get(new URI(s3Path), hadoopConf)) {
            // Recursively list all files
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(rootPath, true);
            
            while (files.hasNext()) {
                LocatedFileStatus fileStatus = files.next();
                Path filePath = fileStatus.getPath();
                
                // Check if it's an Iceberg metadata file
                if (isIcebergMetadataFile(filePath)) {
                    // Keep only the latest metadata file per table directory
                    Path tableDir = filePath.getParent();
                    metadataFiles.merge(tableDir, fileStatus,
                            (existing, newFile) -> 
                                newFile.getModificationTime() > existing.getModificationTime()
                                    ? newFile : existing);
                }
            }
        }
        
        return metadataFiles;
    }
    
    /**
     * Check if file is an Iceberg metadata file
     */
    private boolean isIcebergMetadataFile(Path path) {
        String pathStr = path.toString();
        return pathStr.contains("/metadata/") && 
               pathStr.endsWith(".metadata.json");
    }
    
    /**
     * Process metadata files in parallel
     */
    private List<DiscoveredTableEntity> processMetadataFiles(
            Map<Path, FileStatus> metadataFiles, String catalogName, String jobId,
            String rootPath, Configuration hadoopConf) {
        
        List<DiscoveredTableEntity> discoveredTables = Collections.synchronizedList(new ArrayList<>());
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        
        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            for (Map.Entry<Path, FileStatus> entry : metadataFiles.entrySet()) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        DiscoveredTableEntity table = extractTableInfo(
                                entry.getValue(), catalogName, jobId, rootPath, hadoopConf);
                        if (table != null) {
                            discoveredTables.add(table);
                        }
                    } catch (Exception e) {
                        log.error("Failed to process metadata file: {}", 
                                 entry.getValue().getPath(), e);
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            // Wait for all futures to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            
        } finally {
            executorService.shutdown();
        }
        
        return discoveredTables;
    }
    
    /**
     * Extract table information from metadata file
     */
    private DiscoveredTableEntity extractTableInfo(FileStatus metadataFile, String catalogName,
                                                   String jobId, String rootPath,
                                                   Configuration hadoopConf) throws Exception {
        
        String metadataLocation = metadataFile.getPath().toString();
        log.debug("Processing metadata file: {}", metadataLocation);
        
        // Create S3FileIO with proper S3 properties
        Map<String, String> s3Properties = new HashMap<>();
        
        // Get region from config or extract from endpoint
        String region = hadoopConf.get("fs.s3a.region");
        if (region == null || region.isEmpty()) {
            region = "us-east-1"; // Default region
        }
        
        String endpoint = hadoopConf.get("fs.s3a.endpoint");
        if (endpoint != null && !endpoint.isEmpty()) {
            // If endpoint doesn't start with http/https, add https://
            if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
                endpoint = "https://" + endpoint;
            }
            s3Properties.put("s3.endpoint", endpoint);
            
            // Try to extract region from endpoint if not explicitly provided
            if (hadoopConf.get("fs.s3a.region") == null) {
                if (endpoint.contains(".amazonaws.com")) {
                    // AWS S3 format: s3.us-west-2.amazonaws.com
                    String[] parts = endpoint.split("\\.");
                    if (parts.length >= 3) {
                        region = parts[1]; // Extract region from s3.<region>.amazonaws.com
                    }
                } else if (endpoint.contains(".cloud-object-storage.")) {
                    // IBM COS format: s3.us-west.cloud-object-storage.appdomain.cloud
                    String[] parts = endpoint.split("\\.");
                    if (parts.length >= 2) {
                        region = parts[1]; // Extract region from s3.<region>.cloud-object-storage...
                    }
                }
            }
        }
        
        // Set AWS region - required by AWS SDK v2
        s3Properties.put("client.region", region);
        log.debug("Using AWS region: {} for S3FileIO", region);
        
        s3Properties.put("s3.access-key-id", hadoopConf.get("fs.s3a.access.key"));
        s3Properties.put("s3.secret-access-key", hadoopConf.get("fs.s3a.secret.key"));
        s3Properties.put("s3.path-style-access", hadoopConf.get("fs.s3a.path.style.access", "true"));
        
        FileIO fileIO = new org.apache.iceberg.aws.s3.S3FileIO();
        fileIO.initialize(s3Properties);
        
        try {
            TableMetadata metadata = TableMetadataParser.read(fileIO, metadataLocation);
            
            // Extract table information
            String location = metadata.location();
            if (location == null || location.isEmpty()) {
                log.warn("Skipping metadata file with empty location: {}", metadataLocation);
                return null;
            }
            
            // Parse location to extract schema and table names
            String[] locationParts = location.split("/");
            if (locationParts.length < 2) {
                log.warn("Invalid location format: {}", location);
                return null;
            }
            
            String tableName = locationParts[locationParts.length - 1].toLowerCase();
            String schemaName = extractSchemaName(location, rootPath);
            String warehouseUrl = extractWarehouseUrl(location);
            
            // Extract statistics from snapshot
            Long totalRecords = null;
            Long totalFiles = null;
            Long totalSize = null;
            
            Snapshot currentSnapshot = metadata.currentSnapshot();
            if (currentSnapshot != null && currentSnapshot.summary() != null) {
                Map<String, String> summary = currentSnapshot.summary();
                totalRecords = parseLong(summary.get("total-records"));
                totalFiles = parseLong(summary.get("total-data-files"));
                totalSize = parseLong(summary.get("total-files-size"));
            }
            
            // Build discovered table entity
            return DiscoveredTableEntity.builder()
                    .tableName(tableName)
                    .schemaName(schemaName)
                    .location(location)
                    .warehouseUrl(warehouseUrl)
                    .metadataLocation(metadataLocation)
                    .uuid(metadata.uuid())
                    .schemaJson(SchemaParser.toJson(metadata.schema()))
                    .partitionSpecJson(metadata.spec() != null ?
                            PartitionSpecParser.toJson(metadata.spec()) : null)
                    .propertiesJson(serializeProperties(metadata.properties()))
                    .totalRecords(totalRecords)
                    .totalFiles(totalFiles)
                    .totalSize(totalSize)
                    .discoveredAt(LocalDateTime.now())
                    .registered(false)
                    .syncJobId(jobId)
                    .catalogName(catalogName)
                    .build();
        } finally {
            // Always close FileIO to prevent resource leaks
            if (fileIO != null) {
                try {
                    fileIO.close();
                } catch (Exception e) {
                    log.warn("Failed to close FileIO: {}", e.getMessage());
                }
            }
        }
    }
    
    /**
     * Extract schema name from table location
     * Example: s3a://bucket/basepath/schema/table -> "schema"
     */
    private String extractSchemaName(String location, String rootPath) {
        try {
            // Remove scheme (s3a://, s3://, etc.)
            String locationWithoutScheme = location.substring(location.indexOf("://") + 3);
            String rootWithoutScheme = rootPath.substring(rootPath.indexOf("://") + 3);
            
            // Remove root path prefix
            String relativePath = locationWithoutScheme.replace(rootWithoutScheme, "");
            if (relativePath.startsWith("/")) {
                relativePath = relativePath.substring(1);
            }
            
            // Split by "/" and get first segment
            String[] segments = relativePath.split("/");
            if (segments.length >= 2) {
                return segments[0].toLowerCase();
            } else {
                // Default to bucket-root if no schema segment
                return "default";
            }
        } catch (Exception e) {
            log.warn("Failed to extract schema name from location: {}", location, e);
            return "default";
        }
    }
    
    /**
     * Extract warehouse URL (schema location)
     * Example: s3a://bucket/basepath/schema/table -> "s3a://bucket/basepath/schema"
     */
    private String extractWarehouseUrl(String location) {
        try {
            String[] parts = location.split("/");
            StringBuilder warehouseUrl = new StringBuilder();
            
            // Rebuild URL without the last segment (table name)
            for (int i = 0; i < parts.length - 1; i++) {
                warehouseUrl.append(parts[i]);
                if (i < parts.length - 2) {
                    warehouseUrl.append("/");
                }
            }
            
            return warehouseUrl.toString();
        } catch (Exception e) {
            log.warn("Failed to extract warehouse URL from location: {}", location, e);
            return location;
        }
    }
    
    /**
     * Serialize properties map to JSON
     */
    private String serializeProperties(Map<String, String> properties) {
        try {
            return objectMapper.writeValueAsString(properties);
        } catch (Exception e) {
            log.warn("Failed to serialize properties", e);
            return "{}";
        }
    }
    
    /**
     * Parse long value safely
     */
    private Long parseLong(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * Get stack trace as string
     */
    private String getStackTrace(Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getMessage()).append("\n");
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append("\tat ").append(element.toString()).append("\n");
        }
        return sb.toString();
    }
}

// Made with Bob
