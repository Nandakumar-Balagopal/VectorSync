# S3 Path Configuration Guide

## The Problem

You're getting this error:
```
NoSuchBucket: The specified bucket does not exist
s3a://warehouse
```

This means you entered `warehouse` as the path, but **`warehouse` is not a valid S3 bucket name**.

## How to Find Your Correct S3 Path

### Option 1: List Your S3 Buckets

```bash
# List all your S3 buckets
aws s3 ls

# Output will look like:
# 2024-01-15 10:30:00 my-data-lake
# 2024-02-20 14:45:00 iceberg-prod
# 2024-03-10 09:15:00 analytics-bucket
```

### Option 2: Check Where Your Iceberg Tables Are

```bash
# If you know where your Iceberg tables are, list that location
aws s3 ls s3://your-bucket-name/

# Example:
aws s3 ls s3://my-data-lake/
# Output might show:
# PRE warehouse/
# PRE data/
# PRE staging/
```

### Option 3: Search for Iceberg Metadata Files

```bash
# Find Iceberg tables by searching for metadata files
aws s3 ls s3://your-bucket-name/ --recursive | grep metadata.json

# Example output:
# 2024-03-15 10:00:00  12345 warehouse/db1/table1/metadata/v1.metadata.json
# 2024-03-15 11:00:00  23456 warehouse/db1/table2/metadata/v2.metadata.json
# 2024-03-15 12:00:00  34567 warehouse/db2/table3/metadata/v1.metadata.json
```

## Valid S3 Path Formats

Based on where your Iceberg tables are stored, use one of these formats:

### Format 1: Bucket + Warehouse Directory
```
s3a://my-data-lake/warehouse
```

### Format 2: Bucket + Custom Path
```
s3a://iceberg-prod/data/iceberg
```

### Format 3: Bucket + Nested Path
```
s3a://analytics-bucket/prod/warehouse/iceberg
```

## Examples

### Example 1: Tables in `my-data-lake` bucket under `warehouse/`
```
Bucket: my-data-lake
Path: warehouse/
Full S3 Path: s3a://my-data-lake/warehouse
```

### Example 2: Tables in `iceberg-prod` bucket under `data/iceberg/`
```
Bucket: iceberg-prod
Path: data/iceberg/
Full S3 Path: s3a://iceberg-prod/data/iceberg
```

### Example 3: Tables directly in bucket root
```
Bucket: my-iceberg-tables
Path: (root)
Full S3 Path: s3a://my-iceberg-tables/
```

## How to Update in Dashboard

1. Open dashboard at `http://localhost:3000`
2. Go to **Configuration** page
3. Scroll to **Iceberg Catalog** section
4. Find **Warehouse Location** field
5. Replace `s3://warehouse` or `s3a://warehouse` with your actual path
6. Example: Change to `s3a://my-data-lake/warehouse`
7. Click **Save Configuration**
8. Then click **Sync Tables from S3**

## Testing Your S3 Path

Before syncing, verify your path works:

```bash
# Test if you can list files at your path
aws s3 ls s3://your-bucket-name/your-path/

# Example:
aws s3 ls s3://my-data-lake/warehouse/

# Should show directories like:
# PRE db1/
# PRE db2/
# PRE schema1/
```

## Common Mistakes

❌ **Wrong:** `s3a://warehouse` (missing bucket name)
✅ **Correct:** `s3a://my-bucket/warehouse`

❌ **Wrong:** `warehouse` (missing s3a:// prefix)
✅ **Correct:** `s3a://my-bucket/warehouse`

❌ **Wrong:** `s3://my-bucket/warehouse` (wrong scheme)
✅ **Correct:** `s3a://my-bucket/warehouse`

## Still Not Sure?

If you're not sure what your S3 path should be, run these commands and share the output:

```bash
# 1. List all your buckets
aws s3 ls

# 2. Pick a bucket and list its contents
aws s3 ls s3://your-bucket-name/

# 3. Search for Iceberg metadata files
aws s3 ls s3://your-bucket-name/ --recursive | grep metadata.json | head -5
```

The metadata file paths will show you where your Iceberg tables are located.

For example, if you see:
```
warehouse/mydb/users/metadata/v1.metadata.json
```

Then your warehouse path is: `s3a://your-bucket-name/warehouse`