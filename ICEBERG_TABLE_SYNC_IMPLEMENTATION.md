# Iceberg Table Sync/Discovery Implementation Guide

## Overview
This document describes how to implement an Iceberg table discovery and sync feature based on the IBM MDS (Metastore) implementation. This allows users to scan S3 storage for Iceberg tables and automatically register them with their metadata.

## Key Concepts

### Sync Modes
The MDS implementation supports three sync modes:
1. **sync** - Update metadata location for existing tables only
2. **promote** - Register only new tables (don't update existing)
3. **sync_all** - Both sync existing tables AND register new tables

### Architecture Flow
```
User clicks "Sync Tables" button
    ↓
API endpoint receives sync request
    ↓
Async service scans S3 for Iceberg metadata files
    ↓
Discovers all metadata.json files recursively
    ↓
Extracts table metadata (schema, location, warehouse URL)
    ↓
Registers/updates tables in metastore
    ↓
Returns sync status to user
```

## Core Implementation Components

### 1. Discovery Phase - Scanning S3 for Iceberg Tables

**Key File**: `IcebergRegistrationConfig.java` (lines 174-222)

```java
/**
 * Scans S3 recursively to find all Iceberg metadata.json files
 * Returns a map of latest metadata files per table
 */
private void populateExistingMetadataFiles(Configuration conf, Path rootPath,
                                           Map<Path, FileStatus> existingMetadataFiles) {
    
    try (FileSystem fs = HadoopFileUtils.getFs(rootPath, conf)) {
        // Recursively list all files in S3 bucket
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(rootPath, true);
        
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            Path filePath = fileStatus.getPath();
            
            // Check if file is an Iceberg metadata file
            // Pattern: /metadata/*.metadata.json
            if (ValidateUtils.validateIcebergTableLocation(filePath.toString())) {
                // Keep only the LATEST metadata file per table directory
                existingMetadataFiles.merge(filePath.getParent(), fileStatus,
                    (existing, newFile) -> newFile.getModificationTime() > existing.getModificationTime()
                        ? newFile : existing);
            }
        }
    }
}
```

**Validation Logic**:
```java
// File must be in /metadata/ directory and end with metadata.json
Path filePath = new Path(metadataLocation);
return filePath.toString().contains("/metadata/") && 
       filePath.getName().endsWith("metadata.json");
```

### 2. Parallel Processing with Thread Pools

**Key File**: `SyncManagerImpl.java` (lines 230-282)

```java
private void icebergSync(IcebergRegistrationConfig config) {
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
    Map<String, SyncedItem> schemaSyncedItemMap = new HashMap<>();
    
    try {
        Map<Path, FileStatus> existingMetadataFiles = config.getExistingMetadataFiles();
        List<CompletableFuture<Map<String, SyncedItem>>> futures = new ArrayList<>();
        
        // Convert to list for batching
        List<Path> paths = new ArrayList<>(existingMetadataFiles.keySet());
        int batchSize = existingMetadataFiles.size() / numberOfThreads;
        
        // Process in parallel batches
        for (int i = 0; i < paths.size(); i += batchSize) {
            int end = Math.min(i + batchSize, paths.size());
            List<Path> batch = paths.subList(i, end);
            
            CompletableFuture<Map<String, SyncedItem>> future = 
                CompletableFuture.supplyAsync(() -> {
                    return updateInBatches(config, batch, config.getCatalogName());
                }, executorService);
            futures.add(future);
        }
        
        // Wait for all futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        // Merge results
        mergingFutureResultsToMap(futures, storageSchemaTableMap, schemaSyncedItemMap);
        
    } finally {
        commitTableSyncStatus(config, schemaSyncedItemMap);
        executorService.shutdown();
    }
}
```

### 3. Table Metadata Extraction

**Key File**: `SyncManagerImpl.java` (lines 426-561)

For each metadata file discovered:

```java
public Map<String, SyncedItem> updateInBatches(IcebergRegistrationConfig config, 
                                                List<Path> chunks, String catalogName) {
    for(Path path : chunks) {
        Path latestMetadataPath = existingMetadataFiles.get(path).getPath();
        
        // 1. Load Iceberg TableMetadata from metadata.json
        TableMetadata tableMetadata = loadTableMetadata(config.getConfiguration(), 
                                                        latestMetadataPath, 
                                                        config.getRootPath(), 
                                                        errorList);
        
        // 2. Extract table information
        String tableUuid = tableMetadata.uuid();
        String location = tableMetadata.location(); // e.g., s3a://bucket/schema/table
        String[] locationParts = location.split("/");
        String tableName = locationParts[locationParts.length - 1].toLowerCase();
        
        // 3. Extract schema information
        String schemaLocation = extractSchemaLocation(location, rootPath);
        String schemaName = extractSchemaName(location, rootPath);
        
        // 4. Create schema if it doesn't exist
        if (!databaseExists(schemaName) && !action.equalsIgnoreCase("sync")) {
            createDatabase(schemaName, schemaLocation, createdBy, errorList, config);
        }
        
        // 5. Register or update table based on action
        if (action.equalsIgnoreCase("sync") || action.equalsIgnoreCase("sync_all")) {
            syncOrSyncAll(catalogName, schemaName, tableName, latestMetadataPath, 
                         action, tableUuid, config, tableMetadata, syncedItem);
        } else {
            promote(catalogName, schemaName, tableName, latestMetadataPath, 
                   config, tableMetadata, syncedItem);
        }
    }
}
```

### 4. Schema Name Extraction Logic

**Key File**: `SyncManagerImpl.java` (lines 563-687)

```java
/**
 * Extracts schema name from table location
 * Example: s3a://bucket/basepath/schema/table -> "schema"
 */
private String extractSchemaName(String location, String rootPath) {
    // Remove scheme (s3a://, s3://, etc.)
    String locationWithoutScheme = location.substring(location.indexOf("://") + 3);
    String rootWithoutScheme = rootPath.substring(rootPath.indexOf("://") + 3);
    
    // Remove root path prefix
    String relativePath = locationWithoutScheme.replace(rootWithoutScheme, "");
    
    // Split by "/" and get first segment after root
    String[] segments = relativePath.split("/");
    
    if (segments.length >= 2) {
        return segments[0]; // First segment is schema name
    } else {
        return constructDefaultSchemaName(rootPath); // e.g., "bucket-root"
    }
}

/**
 * Extracts schema location (warehouse URL)
 * Example: s3a://bucket/basepath/schema/table -> "s3a://bucket/basepath/schema"
 */
private String extractSchemaLocation(String location, String rootPath) {
    String[] locationParts = location.split("/");
    
    // Remove last segment (table name) to get schema location
    StringBuilder schemaLocation = new StringBuilder();
    for (int i = 0; i < locationParts.length - 1; i++) {
        schemaLocation.append(locationParts[i]);
        if (i < locationParts.length - 2) {
            schemaLocation.append("/");
        }
    }
    
    return schemaLocation.toString();
}
```

### 5. Table Registration/Update

**Key File**: `SyncManagerImpl.java` (lines 689-749)

```java
/**
 * Sync or register table based on existence
 */
private void syncOrSyncAll(String catalogName, String schemaName, String tableName,
                          Path latestMetadataPath, String action, String tableUuid,
                          IcebergRegistrationConfig config, TableMetadata tableMetadata,
                          SyncedItem syncedItem) {
    
    if (tableExists(schemaName, tableName)) {
        // Table exists - UPDATE metadata location
        LOG.info("Table {}.{} exists. Updating metadata location to: {}", 
                 schemaName, tableName, latestMetadataPath);
        
        updateTableMetadataLocation(config, schemaName, tableName, latestMetadataPath);
        setExtraSyncInfo(tableName, syncedItem, false); // Mark as updated
        
    } else if (action.equalsIgnoreCase("sync_all")) {
        // Table doesn't exist and sync_all mode - REGISTER new table
        LOG.info("Table {}.{} doesn't exist. Registering as new table.", 
                 schemaName, tableName);
        
        // Convert Iceberg metadata to MDS table format
        Table mdsTable = IcebergSyncUtils.convertIcebergTableMetadataToMDSTable(
            catalogName, tableMetadata, schemaName, tableName, 
            latestMetadataPath, ibmlhPrincipal);
        
        mdsService.createTable(mdsTable);
        setExtraSyncInfo(tableName, syncedItem, true); // Mark as newly created
    }
}
```

### 6. Metadata Conversion

**Key File**: `IcebergSyncUtils.java` (lines 83-312)

```java
/**
 * Converts Iceberg TableMetadata to internal table format
 */
public static Table convertIcebergTableMetadataToMDSTable(
        String catalogName, TableMetadata tableMetadata, 
        String schemaName, String tableName, 
        Path latestMetadataPath, IBMLHPrincipal principal) {
    
    // Create base table object
    Table tbl = newMdsTable(tableMetadata, schemaName, tableName, principal);
    tbl.setCatName(catalogName);
    
    // Set storage descriptor (schema, location, SerDe info)
    tbl.setSd(getStorageDescriptor(tableMetadata, hiveEngineEnabled));
    
    // Extract snapshot summary (row count, file count, size)
    Map<String, String> summary = Optional.ofNullable(tableMetadata.currentSnapshot())
        .map(Snapshot::summary)
        .orElseGet(ImmutableMap::of);
    
    // Set table parameters (metadata_location, uuid, schema, partition spec, etc.)
    setMdsTableParameters(latestMetadataPath.toString(), tbl, tableMetadata, 
                         new HashSet<>(), hiveEngineEnabled, summary, catalogName);
    
    return tbl;
}

/**
 * Sets comprehensive table parameters from Iceberg metadata
 */
public static void setMdsTableParameters(String newMetadataLocation, Table tbl,
                                        TableMetadata metadata, Set<String> obsoleteProps,
                                        boolean hiveEngineEnabled, Map<String, String> summary,
                                        String catalogName) {
    Map<String, String> parameters = new HashMap<>();
    
    // Copy Iceberg properties
    metadata.properties().forEach((key, value) -> {
        parameters.put(key, value);
    });
    
    // Set critical metadata
    parameters.put("table_type", "ICEBERG");
    parameters.put("metadata_location", newMetadataLocation);
    parameters.put("uuid", metadata.uuid());
    parameters.put("catalog", catalogName);
    
    // Set statistics from snapshot summary
    if (summary.get("total-data-files") != null) {
        parameters.put("numFiles", summary.get("total-data-files"));
    }
    if (summary.get("total-records") != null) {
        parameters.put("numRows", summary.get("total-records"));
    }
    if (summary.get("total-files-size") != null) {
        parameters.put("totalSize", summary.get("total-files-size"));
    }
    
    // Set schema, partition spec, sort order as JSON
    setSchema(metadata, parameters);
    setPartitionSpec(metadata, parameters);
    setSortOrder(metadata, parameters);
    setSnapshotStats(metadata, parameters);
    
    tbl.setParameters(parameters);
}
```

## API Endpoints

### Sync Request Model

```java
public class IcebergSyncRequest {
    @JsonProperty("sync_existing_tables")
    private boolean syncExistingTables;
    
    @JsonProperty("register_new_tables")
    private boolean registerNewTables;
    
    @JsonProperty("catalog_name")
    private String catalogName;
    
    @JsonProperty("sync_path")
    private String syncPath; // Optional: specific path to sync
    
    @JsonProperty("created_by")
    private String createdBy;
}
```

### REST Endpoint

```java
@PutMapping("/sync")
public ResponseEntity<Void> syncAll(
        @Valid @RequestBody IcebergSyncRequest icebergSyncRequest,
        @RequestParam("bucket_name") String bucketName,
        @RequestParam("bucket_type") String bucketType,
        @RequestParam("base_path") String basePath) {
    
    // Determine action type
    String action = IcebergSyncUtils.actionType(icebergSyncRequest);
    // Returns: "sync", "promote", or "sync_all"
    
    // Execute async sync
    icebergRegistrationService.asyncRegisterSyncTable(
        icebergSyncRequest, bucketName, bucketType, basePath, accountId);
    
    return ResponseEntity.accepted().build(); // 202 Accepted
}
```

## Implementation Steps for VectorSync

### 1. Add Sync Endpoint to Control API

Create `TableSyncController.java`:
```java
@RestController
@RequestMapping("/api/tables")
public class TableSyncController {
    
    @PostMapping("/sync")
    public ResponseEntity<SyncResponse> syncTables(
            @RequestBody SyncRequest request) {
        // Trigger async sync operation
        return ResponseEntity.accepted().body(syncResponse);
    }
    
    @GetMapping("/sync/status/{jobId}")
    public ResponseEntity<SyncStatus> getSyncStatus(
            @PathVariable String jobId) {
        // Return sync job status
        return ResponseEntity.ok(status);
    }
}
```

### 2. Create Table Discovery Service

Create `IcebergTableDiscoveryService.java`:
```java
@Service
public class IcebergTableDiscoveryService {
    
    /**
     * Scan S3 for all Iceberg tables
     */
    public List<DiscoveredTable> discoverTables(String s3Path, Configuration hadoopConf) {
        List<DiscoveredTable> tables = new ArrayList<>();
        
        try (FileSystem fs = FileSystem.get(new URI(s3Path), hadoopConf)) {
            // Recursively list all files
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(
                new Path(s3Path), true);
            
            Map<Path, FileStatus> latestMetadata = new HashMap<>();
            
            while (files.hasNext()) {
                LocatedFileStatus file = files.next();
                Path filePath = file.getPath();
                
                // Check if it's an Iceberg metadata file
                if (isIcebergMetadataFile(filePath)) {
                    // Keep only latest metadata per table
                    latestMetadata.merge(filePath.getParent(), file,
                        (existing, newFile) -> 
                            newFile.getModificationTime() > existing.getModificationTime()
                                ? newFile : existing);
                }
            }
            
            // Process each discovered table
            for (Map.Entry<Path, FileStatus> entry : latestMetadata.entrySet()) {
                DiscoveredTable table = extractTableInfo(entry.getValue());
                tables.add(table);
            }
        }
        
        return tables;
    }
    
    private boolean isIcebergMetadataFile(Path path) {
        String pathStr = path.toString();
        return pathStr.contains("/metadata/") && 
               pathStr.endsWith(".metadata.json");
    }
    
    private DiscoveredTable extractTableInfo(FileStatus metadataFile) {
        // Read metadata.json
        TableMetadata metadata = TableMetadataParser.read(
            fileIO, metadataFile.getPath().toString());
        
        // Extract information
        String location = metadata.location();
        String[] parts = location.split("/");
        String tableName = parts[parts.length - 1];
        String schemaName = parts[parts.length - 2];
        
        return DiscoveredTable.builder()
            .tableName(tableName)
            .schemaName(schemaName)
            .location(location)
            .warehouseUrl(extractWarehouseUrl(location))
            .metadataLocation(metadataFile.getPath().toString())
            .uuid(metadata.uuid())
            .schema(metadata.schema())
            .partitionSpec(metadata.spec())
            .properties(metadata.properties())
            .build();
    }
}
```

### 3. Add Dashboard "Sync Tables" Button

In `Configuration.tsx`:
```typescript
const handleSyncTables = async () => {
  setIsSyncing(true);
  try {
    const response = await fetch('/api/tables/sync', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sync_existing_tables: true,
        register_new_tables: true,
        catalog_name: selectedCatalog,
        s3_path: s3BucketPath
      })
    });
    
    const result = await response.json();
    setSyncJobId(result.jobId);
    
    // Poll for status
    pollSyncStatus(result.jobId);
  } catch (error) {
    console.error('Sync failed:', error);
  } finally {
    setIsSyncing(false);
  }
};

return (
  <Button
    onClick={handleSyncTables}
    disabled={isSyncing || !s3Connected}
  >
    {isSyncing ? 'Syncing...' : 'Sync Tables from S3'}
  </Button>
);
```

### 4. Store Discovered Tables

Create `DiscoveredTableEntity.java`:
```java
@Entity
@Table(name = "discovered_tables")
public class DiscoveredTableEntity {
    @Id
    @GeneratedValue
    private Long id;
    
    private String tableName;
    private String schemaName;
    private String location;
    private String warehouseUrl;
    private String metadataLocation;
    private String uuid;
    
    @Column(columnDefinition = "TEXT")
    private String schemaJson;
    
    @Column(columnDefinition = "TEXT")
    private String partitionSpecJson;
    
    private LocalDateTime discoveredAt;
    private boolean registered;
}
```

## Key Takeaways

1. **Discovery is Recursive**: Scan entire S3 bucket/path recursively for `/metadata/*.metadata.json` files

2. **Keep Latest Only**: For each table directory, keep only the most recent metadata file based on modification time

3. **Parallel Processing**: Use thread pools to process large numbers of tables efficiently

4. **Extract Everything**: From metadata.json, extract:
   - Table name (from location path)
   - Schema name (from location path)
   - Warehouse URL (schema location)
   - UUID, schema, partition spec, properties
   - Statistics (row count, file count, size)

5. **Three Sync Modes**:
   - **sync**: Update existing tables only
   - **promote**: Register new tables only
   - **sync_all**: Both update and register

6. **Async Operation**: Sync should be asynchronous with status polling

7. **Error Handling**: Track errors per table and provide consolidated error messages

## References

- Main sync logic: `SyncManagerImpl.java`
- Discovery logic: `IcebergRegistrationConfig.java` (populateExistingMetadataFiles)
- Metadata conversion: `IcebergSyncUtils.java`
- API endpoint: `CoreController.java` or `MetadataAPIController.java`
- Request model: `IcebergSyncRequest.java`