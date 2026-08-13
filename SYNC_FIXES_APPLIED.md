# Table Sync Fixes Applied

## Issues Found and Fixed

### 1. CORS Error ✅ FIXED
**Error:**
```
Access to fetch at 'http://localhost:8080/api/tables/sync' from origin 'http://localhost:3000' 
has been blocked by CORS policy
```

**Fix:** Created [`CorsConfig.java`](control-api/src/main/java/io/vectorsync/controlapi/config/CorsConfig.java:1)
- Allows requests from `localhost:3000`, `localhost:5173`, `localhost:8081`
- Enables all HTTP methods and headers

### 2. Missing `-parameters` Compiler Flag ✅ FIXED
**Error:**
```
java.lang.IllegalArgumentException: Name for argument of type [java.lang.Boolean] not specified, 
and parameter name information not available via reflection. 
Ensure that the compiler uses the '-parameters' flag.
```

**Fix:** Updated [`control-api/pom.xml`](control-api/pom.xml:100)
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>3.11.0</version>
    <configuration>
        <parameters>true</parameters>
    </configuration>
</plugin>
```

### 3. Column Name Mismatch ✅ FIXED
**Error:**
```
ERROR: null value in column "s3_path" of relation "sync_jobs" violates not-null constraint
```

**Root Cause:** JPA was mapping `s3Path` field to `s3path` column, but database has `s3_path`

**Fix:** Updated [`SyncJobEntity.java`](control-api/src/main/java/io/vectorsync/controlapi/entity/SyncJobEntity.java:25)
```java
@Column(name = "s3_path", nullable = false, length = 1000)
private String s3Path;
```

### 4. Wrong S3 Scheme ✅ FIXED
**Error:**
```
org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "s3"
```

**Root Cause:** User entered `s3://warehouse` but Hadoop requires `s3a://` scheme

**Fix:** Updated [`Configuration.tsx`](dashboard/src/pages/Configuration.tsx:308) to auto-convert:
```typescript
// Convert s3:// to s3a:// for Hadoop compatibility
let s3Path = storageConfig.iceberg.warehouse;
if (s3Path.startsWith('s3://')) {
  s3Path = s3Path.replace('s3://', 's3a://');
}
```

### 5. Hadoop AWS Dependency ✅ ALREADY PRESENT
The `hadoop-aws` dependency was already in [`control-api/pom.xml`](control-api/pom.xml:56), so S3A filesystem support is available.

## Files Modified

1. **`control-api/src/main/java/io/vectorsync/controlapi/config/CorsConfig.java`** - NEW
   - CORS configuration for cross-origin requests

2. **`control-api/pom.xml`** - MODIFIED
   - Added maven-compiler-plugin with `-parameters` flag

3. **`control-api/src/main/java/io/vectorsync/controlapi/entity/SyncJobEntity.java`** - MODIFIED
   - Fixed column name mapping for `s3_path`

4. **`dashboard/src/pages/Configuration.tsx`** - MODIFIED
   - Auto-converts `s3://` to `s3a://` before sending to API

## How to Apply Fixes

### Step 1: Rebuild Control API
```bash
./restart-control-api.sh
```

This will:
1. Kill existing process on port 8080
2. Rebuild with new compiler settings
3. Start control-api with CORS support

### Step 2: Rebuild Dashboard (if needed)
```bash
cd dashboard
npm run build  # or just refresh if using dev server
```

### Step 3: Test the Sync
1. Open dashboard at `http://localhost:3000`
2. Go to Configuration > Storage & Infrastructure
3. Enter S3 credentials (both access key AND secret key required)
4. Enter warehouse location (can use `s3://` or `s3a://` - will auto-convert)
5. Click "Sync Tables from S3"
6. Watch progress bar update in real-time

## Expected Behavior After Fixes

### API Logs Should Show:
```
Received sync request for catalog: iceberg_catalog at path: s3a://bucket/warehouse
Starting table discovery job: {jobId} for catalog: iceberg_catalog at path: s3a://bucket/warehouse
Discovered 25 metadata files for job: {jobId}
Processing metadata files in parallel with 4 threads
Successfully processed table: schema.table_name
...
Table discovery job completed: {jobId}
```

### Dashboard Should Show:
- ✅ "Table sync started successfully" notification
- ✅ Progress bar with status updates
- ✅ Real-time counts: "Discovered: X, Registered: Y, Failed: Z"
- ✅ Final success message when complete

### Database Should Contain:
```sql
-- Check sync jobs
SELECT * FROM sync_jobs ORDER BY started_at DESC LIMIT 5;

-- Check discovered tables
SELECT table_name, schema_name, warehouse_url, total_records 
FROM discovered_tables 
ORDER BY discovered_at DESC;
```

## Verification Commands

```bash
# 1. Check if control-api is running with CORS
curl -H "Origin: http://localhost:3000" \
     -H "Access-Control-Request-Method: POST" \
     -H "Access-Control-Request-Headers: Content-Type" \
     -X OPTIONS \
     http://localhost:8080/api/tables/sync

# Should return 200 with CORS headers

# 2. Test sync endpoint
curl -X POST http://localhost:8080/api/tables/sync \
  -H "Content-Type: application/json" \
  -d '{
    "catalogName": "iceberg_catalog",
    "s3Path": "s3a://your-bucket/warehouse",
    "syncExistingTables": true,
    "registerNewTables": true,
    "createdBy": "admin",
    "awsAccessKey": "your-key",
    "awsSecretKey": "your-secret",
    "awsEndpoint": "https://s3.amazonaws.com"
  }'

# Should return: {"success": true, "jobId": "...", "message": "Table sync started"}

# 3. Check job status
curl http://localhost:8080/api/tables/sync/status/{jobId}

# 4. List discovered tables
curl http://localhost:8080/api/tables/discovered
```

## Troubleshooting

### If sync still fails:

1. **Check S3 credentials are correct**
   ```bash
   aws s3 ls s3://your-bucket/warehouse --profile your-profile
   ```

2. **Check S3 path has metadata files**
   ```bash
   aws s3 ls s3://your-bucket/warehouse/ --recursive | grep metadata.json
   ```

3. **Check control-api logs for detailed errors**
   ```bash
   # In control-api terminal, look for stack traces
   ```

4. **Verify database connection**
   ```bash
   psql -U postgres -d vectorsync -c "SELECT COUNT(*) FROM sync_jobs"
   ```

5. **Check browser console for frontend errors**
   - Open DevTools (F12)
   - Look for red errors in Console tab
   - Check Network tab for failed requests

## Next Steps

After applying these fixes:
1. Restart control-api: `./restart-control-api.sh`
2. Refresh dashboard in browser
3. Try syncing tables again
4. Monitor logs and progress bar
5. Verify tables appear in database and UI dropdowns

All critical issues have been fixed. The sync feature should now work correctly!