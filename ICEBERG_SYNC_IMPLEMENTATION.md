# Iceberg Table Sync Implementation

## Overview
This document describes the Iceberg table discovery/sync feature implemented in VectorSync, based on the MDS (lakehouse-mds) reference implementation.

## Reference Implementation (MDS)
Located at: `/Users/nandakumarb/Documents/mds/lakehouse-mds/mds-rest-service/src/main/java/com/ibm/wxd/mds/rest/configuration/iceberg/IcebergRegistrationConfig.java`

### Key MDS Approach:
1. Uses Hadoop FileSystem to recursively list all files in S3
2. Filters for files matching pattern: `/metadata/*.metadata.json`
3. Keeps only the LATEST metadata file per table directory using modification time
4. Uses `fs.listFiles(rootPath, true)` for recursive listing
5. Validates paths: `contains("/metadata/") && endsWith("metadata.json")`

## VectorSync Implementation

### Architecture
```
User clicks "Sync Tables" button
    ↓
Dashboard sends request to TableSyncController
    ↓
Controller creates Hadoop Configuration with AWS credentials
    ↓
IcebergTableDiscoveryService.discoverAndSyncTables() (async)
    ↓
Scans S3 recursively for metadata files
    ↓
Processes files in parallel (4 threads)
    ↓
Extracts table metadata using Iceberg TableMetadataParser
    ↓
Saves discovered tables to database
    ↓
Updates sync job status
```

### Key Components

#### 1. Backend Entities

**DiscoveredTableEntity** (`control-api/src/main/java/io/vectorsync/controlapi/entity/DiscoveredTableEntity.java`)
- Stores discovered table metadata
- Fields: tableName, schemaName, location, warehouseUrl, metadataLocation, uuid, schema, partitions, statistics
- `registered` flag indicates if table has been registered for vectorization

**SyncJobEntity** (`control-api/src/main/java/io/vectorsync/controlapi/entity/SyncJobEntity.java`)
- Tracks sync job progress
- Fields: jobId, catalogName, s3Path, status, counters (total/processed/failed/discovered)
- Status: RUNNING, COMPLETED, FAILED

#### 2. Service Layer

**IcebergTableDiscoveryService** (`control-api/src/main/java/io/vectorsync/controlapi/service/IcebergTableDiscoveryService.java`)

Key methods:
- `discoverAndSyncTables()` - Main async method that orchestrates the sync
- `scanForMetadataFiles()` - Recursively scans S3 using Hadoop FileSystem
- `processMetadataFiles()` - Processes files in parallel using ExecutorService (4 threads)
- `extractTableInfo()` - Reads metadata using Iceberg TableMetadataParser with S3FileIO

**Differences from MDS:**
- Uses parallel processing (4 threads) for better performance
- Uses Iceberg's S3FileIO directly instead of Hadoop FileSystem for reading metadata
- Stores ALL metadata files, not just latest (can be optimized later)
- Saves discovered tables to database for UI display

#### 3. REST API

**TableSyncController** (`control-api/src/main/java/io/vectorsync/controlapi/controller/TableSyncController.java`)

Endpoints:
- `POST /api/tables/sync` - Start sync job
- `GET /api/tables/sync/{jobId}` - Get sync job status
- `GET /api/tables/discovered` - List discovered tables

Request body:
```json
{
  "catalogName": "iceberg_catalog",
  "s3Path": "s3a://bucket-name/",
  "awsAccessKey": "...",
  "awsSecretKey": "...",
  "awsEndpoint": "s3.us-east-1.amazonaws.com",
  "syncExistingTables": false,
  "registerNewTables": false,
  "createdBy": "user@example.com"
}
```

#### 4. Frontend (Dashboard)

**Configuration.tsx** (`dashboard/src/pages/Configuration.tsx`)

Features:
- "Sync Tables" button in S3 configuration section
- Real-time progress tracking with polling
- Displays: total files, processed, failed, discovered tables
- Auto-converts `s3://` to `s3a://` for Hadoop compatibility
- Shows sync status: Running, Completed, Failed

### Database Schema

**discovered_tables** table:
```sql
CREATE TABLE discovered_tables (
    id BIGSERIAL PRIMARY KEY,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    location TEXT NOT NULL,
    warehouse_url TEXT,
    metadata_location TEXT NOT NULL,
    table_uuid VARCHAR(255),
    schema_json TEXT,
    partition_spec_json TEXT,
    statistics_json TEXT,
    registered BOOLEAN DEFAULT FALSE,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(catalog_name, schema_name, table_name)
);
```

**sync_jobs** table:
```sql
CREATE TABLE sync_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(255) UNIQUE NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    s3_path TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    total_files INTEGER DEFAULT 0,
    processed_files INTEGER DEFAULT 0,
    failed_files INTEGER DEFAULT 0,
    discovered_tables INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);
```

## Current Issue and Fix

### Problem
All metadata files were failing with:
```
NullPointerException: The URI scheme of endpointOverride must not be null
```

### Root Cause
The S3FileIO expects the endpoint to be a full URI (e.g., `https://s3.us-east-1.amazonaws.com`), but:
1. We were passing just the hostname without the protocol
2. If endpoint was null, we were still setting it in the properties map

### Solution Applied
In `IcebergTableDiscoveryService.extractTableInfo()` (lines 199-214):
```java
// Only set endpoint if provided, and ensure it's a full URI
String endpoint = hadoopConf.get("fs.s3a.endpoint");
if (endpoint != null && !endpoint.isEmpty()) {
    // If endpoint doesn't start with http/https, add https://
    if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
        endpoint = "https://" + endpoint;
    }
    s3Properties.put("s3.endpoint", endpoint);
}
```

This ensures:
1. Endpoint is only set if it's not null/empty
2. Protocol (https://) is added if missing
3. AWS SDK can properly parse the endpoint URI

## Testing

### Test Scenario
1. User enters S3 credentials in Configuration page
2. Clicks "Sync Tables" button
3. System scans `s3a://rameshbucket/` recursively
4. Finds 50+ metadata files
5. Processes them in parallel
6. Extracts table information
7. Saves to `discovered_tables`
8. Updates sync job status

### Expected Results
- All metadata files processed successfully
- Tables appear in discovered_tables table
- User can see discovered tables in UI
- User can manually register tables for vectorization

## Next Steps

1. ✅ Fix S3 endpoint null issue
2. ⏳ Test sync with real S3 credentials
3. ⏳ Verify tables appear in UI dropdown
4. ⏳ Optimize to keep only latest metadata file per table (like MDS)
5. ⏳ Add error handling for malformed metadata files
6. ⏳ Add pagination for discovered tables list
7. ⏳ Add filters (by schema, table name, etc.)

## Comparison: VectorSync vs MDS

| Feature | MDS | VectorSync |
|---------|-----|------------|
| File Scanning | Hadoop FileSystem | Hadoop FileSystem |
| Metadata Reading | Hadoop FileSystem | Iceberg S3FileIO |
| Latest File Only | ✅ Yes | ❌ No (stores all) |
| Parallel Processing | ❌ No | ✅ Yes (4 threads) |
| Database Storage | ❌ No | ✅ Yes |
| UI Integration | ❌ No | ✅ Yes |
| Progress Tracking | ❌ No | ✅ Yes |
| Manual Registration | N/A | ✅ Yes |

## Configuration

### Required Environment Variables
```bash
# AWS Credentials (provided via UI)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_ENDPOINT=s3.us-east-1.amazonaws.com

# Database
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/vectorsync
SPRING_DATASOURCE_USERNAME=postgres
SPRING_DATASOURCE_PASSWORD=...
```

### Maven Dependencies
```xml
<!-- Hadoop AWS -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-aws</artifactId>
    <version>3.3.4</version>
</dependency>

<!-- AWS SDK v2 -->
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>s3</artifactId>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>sts</artifactId>
</dependency>

<!-- Iceberg -->
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-core</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-aws</artifactId>
</dependency>
```

## Conclusion

The VectorSync Iceberg table sync feature successfully replicates the MDS approach with enhancements:
- Parallel processing for better performance
- Database storage for persistence
- UI integration for user-friendly operation
- Progress tracking for transparency
- Manual registration workflow for control

The implementation is production-ready pending successful testing with real S3 credentials.