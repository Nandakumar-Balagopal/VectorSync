# Testing Instructions for Iceberg Table Sync Feature

## Prerequisites

- S3 credentials (access key and secret key)
- S3 bucket with Iceberg tables
- Maven installed
- Node.js and npm installed

## Step 1: Build the Control API

```bash
cd control-api
mvn clean install -DskipTests
cd ..
```

## Step 2: Start the Control API

```bash
cd control-api
mvn spring-boot:run
```

Or if using Docker:
```bash
docker-compose up control-api
```

The API should start on `http://localhost:8080`

## Step 3: Start the Dashboard

```bash
cd dashboard
npm install
npm run dev
```

The dashboard should start on `http://localhost:5173`

## Step 4: Configure S3 Credentials

1. Open browser to `http://localhost:5173`
2. Navigate to **Configuration** tab
3. Click on **Storage & Infrastructure** tab
4. Open the **S3 / MinIO Storage** accordion
5. Fill in your S3 credentials:
   - **Endpoint**: Your S3 endpoint (e.g., `https://s3.amazonaws.com` or `http://localhost:9000` for MinIO)
   - **Bucket Name**: Your bucket name
   - **Access Key**: Your AWS access key
   - **Secret Key**: Your AWS secret key
   - **Region**: Your AWS region (default: `us-east-1`)
6. Click **Save Changes**

## Step 5: Configure Iceberg Catalog

1. In the same **Storage & Infrastructure** tab
2. Open the **Iceberg Catalog** accordion
3. Fill in:
   - **Catalog Type**: Select your catalog type (REST, Hive, or Glue)
   - **Catalog URI**: Your catalog URI (e.g., `http://localhost:8181`)
   - **Warehouse Location**: Your S3 warehouse path (e.g., `s3a://your-bucket/warehouse`)
4. Click **Save Changes**

## Step 6: Sync Tables from S3

1. Scroll down in the **S3 / MinIO Storage** section
2. You should see a new section titled **"Iceberg Table Discovery"**
3. Click the **"Sync Tables from S3"** button
4. Watch the progress:
   - A loading indicator will appear
   - Status notifications will show progress
   - You'll see "Discovered: X tables | Registered: Y tables"
5. Wait for the completion notification

## Step 7: Verify Tables in UI

After sync completes:

1. Navigate to the **Tables** tab
2. Check if discovered tables appear in the list
3. Try selecting a table from any dropdown
4. Verify table metadata is displayed correctly

## Step 8: Verify via API (Optional)

### Get all sync jobs
```bash
curl http://localhost:8080/api/tables/sync/jobs
```

### Get specific job status
```bash
curl http://localhost:8080/api/tables/sync/status/{jobId}
```

### Get discovered tables
```bash
curl http://localhost:8080/api/tables/discovered
```

### Get discovered tables (unregistered only)
```bash
curl http://localhost:8080/api/tables/discovered?registered=false
```

## Step 9: Check Database (Optional)

Connect to your PostgreSQL database and run:

```sql
-- Check discovered tables
SELECT 
    table_name, 
    schema_name, 
    location, 
    warehouse_url,
    total_records,
    discovered_at,
    registered
FROM discovered_tables
ORDER BY discovered_at DESC;

-- Check sync jobs
SELECT 
    job_id,
    catalog_name,
    status,
    tables_discovered,
    tables_registered,
    started_at,
    completed_at
FROM sync_jobs
ORDER BY started_at DESC;
```

## Troubleshooting

### Issue: "Sync Tables" button is disabled

**Solution**: Make sure you've entered and saved S3 credentials (access key and secret key)

### Issue: Sync fails with "Access Denied"

**Solution**: 
- Verify S3 credentials are correct
- Check IAM permissions include `s3:ListBucket` and `s3:GetObject`
- Verify bucket policy allows access

### Issue: No tables discovered

**Solution**:
- Verify warehouse path is correct
- Check that Iceberg tables exist in S3 with pattern `/metadata/*.metadata.json`
- Ensure tables are Iceberg format (not Hive or Delta)

### Issue: Sync times out

**Solution**:
- Check network connectivity to S3
- Verify S3 endpoint is accessible
- Try syncing a smaller subset of tables

### Issue: Tables not appearing in UI

**Solution**:
- Refresh the browser page
- Check browser console for errors
- Verify API endpoint `http://localhost:8080` is accessible
- Check database to confirm tables were discovered

### Issue: Build errors

**Solution**:
- Ensure Java 17+ is installed
- Run `mvn clean install` to rebuild
- Check for Lombok annotation processor in IDE settings

## Expected Results

After successful sync:

1. **Sync Job Status**: Status should be "COMPLETED"
2. **Tables Discovered**: Should match number of Iceberg tables in S3
3. **Database Records**: `discovered_tables` table should have entries
4. **UI Display**: Tables should appear in dropdowns and tables list
5. **Metadata**: Each table should have schema, location, and statistics

## Performance Benchmarks

Typical sync times:
- 10 tables: ~10 seconds
- 100 tables: ~30 seconds
- 1,000 tables: ~3 minutes

Performance varies based on:
- S3 latency
- Network bandwidth
- Metadata file sizes
- Thread pool configuration (default: 4 threads)

## Next Steps After Testing

1. Register discovered tables for vector search
2. Configure embedding models for tables
3. Start sync operations
4. Test semantic search functionality

## Support

If you encounter issues:
1. Check application logs in `control-api/logs/`
2. Check browser console for frontend errors
3. Verify database connectivity
4. Review API responses for error messages