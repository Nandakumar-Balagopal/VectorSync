# End-to-End Test Plan with Real Embeddings

## Overview

This document provides a comprehensive test plan to verify VectorSync is working correctly with real embedding generation and semantic search.

**Test Duration**: ~30 minutes  
**Prerequisites**: Docker, Docker Compose, curl, jq (optional)  
**Embedding Provider**: Local Sentence Transformers (CPU-based)

---

## Table of Contents
1. [Environment Setup](#environment-setup)
2. [Service Startup](#service-startup)
3. [Health Checks](#health-checks)
4. [Data Preparation](#data-preparation)
5. [Table Registration](#table-registration)
6. [Sync Execution](#sync-execution)
7. [Semantic Search Testing](#semantic-search-testing)
8. [Verification](#verification)
9. [Cleanup](#cleanup)

---

## Environment Setup

### Step 1: Prepare Environment File

Create `.env` file with real embedding service configuration:

```bash
# Copy the local configuration
cp .env.local .env

# Verify embedding service is configured for external provider
cat .env | grep EMBEDDING
```

**Expected Output:**
```
EMBEDDING_PROVIDER=external
EMBEDDING_EXTERNAL_TYPE=http
EMBEDDING_EXTERNAL_API_URL=http://embedding-service:8000/api/v1/embed
```

### Step 2: Verify Docker Resources

```bash
# Check Docker is running
docker info

# Ensure sufficient resources
# Minimum: 4GB RAM, 2 CPUs
docker system df
```

---

## Service Startup

### Step 1: Clean Previous State

```bash
# Stop and remove all containers
docker-compose down -v

# Remove old volumes (fresh start)
docker volume prune -f

# Verify cleanup
docker ps -a
docker volume ls
```

### Step 2: Start All Services

```bash
# Start with local storage and embedding service
docker-compose --profile local-storage --profile local-embedding up -d

# This starts:
# - postgres (database)
# - minio (S3 storage)
# - embedding-service (Python, Sentence Transformers)
# - control-plane (port 8080)
# - worker (port 8081)
# - search-service (port 8083)
# - dashboard (port 3000)
```

**Expected Output:**
```
✔ Container vectorsync-postgres          Started
✔ Container vectorsync-minio             Started
✔ Container vectorsync-embedding         Started
✔ Container vectorsync-control-plane     Started
✔ Container vectorsync-worker            Started
✔ Container vectorsync-search-service    Started
✔ Container vectorsync-dashboard         Started
```

### Step 3: Monitor Startup

```bash
# Watch logs (Ctrl+C to exit)
docker-compose logs -f

# Or check specific service
docker-compose logs -f embedding-service
```

**Wait for these messages:**
- `embedding-service`: "Application startup complete"
- `control-plane`: "Started ControlApiApplication"
- `worker`: "Started WorkerApplication"
- `search-service`: "Started SearchServiceApplication"

**Estimated startup time**: 2-3 minutes

---

## Health Checks

### Step 1: Check All Services

```bash
# Control Plane
curl -s http://localhost:8080/actuator/health | jq

# Worker
curl -s http://localhost:8081/api/vectors/health

# Search Service
curl -s http://localhost:8083/api/search/health

# Embedding Service
curl -s http://localhost:8000/api/v1/health | jq

# Dashboard
curl -s http://localhost:3000
```

**Expected Results:**
- All services return HTTP 200
- Health status: "UP" or "healthy"

### Step 2: Verify Database Connection

```bash
# Check control-plane can connect to PostgreSQL
docker-compose logs control-plane | grep -i "database"

# Should see: "HikariPool-1 - Start completed"
```

### Step 3: Verify S3 Connection

```bash
# Check worker can connect to MinIO
docker-compose logs worker | grep -i "s3\|minio"

# Should see successful S3 client initialization
```

### Step 4: Test Embedding Service

```bash
# Test embedding generation
curl -X POST http://localhost:8000/api/v1/embed \
  -H "Content-Type: application/json" \
  -d '{
    "texts": ["test sentence"],
    "model": "sentence-transformers/all-MiniLM-L6-v2"
  }' | jq

# Expected: 384-dimensional vector
# {
#   "embeddings": [[0.123, -0.456, ...]],
#   "model": "sentence-transformers/all-MiniLM-L6-v2",
#   "dimensions": 384
# }
```

**✅ Checkpoint**: All services healthy and responding

---

## Data Preparation

### Step 1: Seed Demo Data

```bash
# Create demo products table with sample data
curl -X POST http://localhost:8081/api/demo/seed | jq

# Expected response:
# {
#   "status": "success",
#   "tableName": "products",
#   "recordsCreated": 10,
#   "catalogName": "iceberg_data",
#   "schemaName": "default",
#   "message": "Demo products table created successfully"
# }
```

**What this does:**
1. Creates Iceberg table `iceberg_data.default.products`
2. Inserts 10 sample products with:
   - id, name, description, price, category
3. Stores in MinIO at `s3://warehouse/iceberg/default.db/products/`

### Step 2: Verify Table Creation

```bash
# Check MinIO console
open http://localhost:9001
# Login: minioadmin / minioadmin
# Navigate to: warehouse/iceberg/default.db/products/

# Or check via API
docker-compose exec worker ls -la /tmp/warehouse/iceberg/default.db/
```

**✅ Checkpoint**: Demo table created with 10 products

---

## Table Registration

### Step 1: Register Table for Sync

```bash
# Register the products table
curl -X POST http://localhost:8080/api/tables/register \
  -H "Content-Type: application/json" \
  -d '{
    "catalogName": "iceberg_data",
    "schemaName": "default",
    "tableName": "products",
    "textColumns": ["name", "description"],
    "pollInterval": 30000,
    "enabled": true
  }' | jq

# Expected response:
# {
#   "id": "...",
#   "catalogName": "iceberg_data",
#   "schemaName": "default",
#   "tableName": "products",
#   "textColumns": ["name", "description"],
#   "enabled": true,
#   "createdAt": "2024-01-01T00:00:00Z"
# }
```

### Step 2: Verify Registration

```bash
# List all registered tables
curl -s http://localhost:8080/api/tables | jq

# Should show 1 table: products
```

### Step 3: Check Initial Sync State

```bash
# Get table ID from previous response
TABLE_ID="..." # Replace with actual ID

# Check sync state
curl -s http://localhost:8080/api/tables/${TABLE_ID}/status | jq

# Expected:
# {
#   "tableId": "...",
#   "tableName": "products",
#   "enabled": true,
#   "lastSnapshotId": null,  # Not synced yet
#   "lastSyncAt": null
# }
```

**✅ Checkpoint**: Table registered and ready for sync

---

## Sync Execution

### Step 1: Trigger Initial Sync

```bash
# Run sync for all enabled tables
curl -X POST http://localhost:8081/api/demo/sync | jq

# Expected response:
# {
#   "tablesSynced": 1,
#   "vectorCount": 10
# }
```

**What happens during sync:**
1. Worker detects table has new snapshot
2. Reads 10 product records
3. Extracts text from "name" and "description" columns
4. Calls embedding service to generate vectors
5. Writes 10 vector records to Iceberg
6. Updates sync state

**Expected duration**: 10-30 seconds (depending on CPU)

### Step 2: Monitor Sync Progress

```bash
# Watch worker logs
docker-compose logs -f worker

# Look for these messages:
# - "Starting CDC for table: products"
# - "Detected X new records"
# - "Generating embeddings for X records"
# - "Writing X vectors to Iceberg"
# - "Sync completed successfully"
```

### Step 3: Verify Sync Completion

```bash
# Check vector count
curl -s http://localhost:8081/api/vectors/count | jq

# Expected: {"count": 10}

# Check sync state again
curl -s http://localhost:8080/api/tables/${TABLE_ID}/status | jq

# Expected:
# {
#   "tableId": "...",
#   "tableName": "products",
#   "enabled": true,
#   "lastSnapshotId": 123456789,  # Now populated
#   "lastSyncAt": "2024-01-01T00:00:00Z"
# }
```

### Step 4: Verify Vector Table

```bash
# Check vector table exists in MinIO
# Navigate to: warehouse/iceberg/vector/products_vectors/

# Or check via logs
docker-compose logs worker | grep "vector table"
```

**✅ Checkpoint**: 10 vectors generated and stored

---

## Semantic Search Testing

### Test 1: Search for "shoes"

```bash
curl -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "comfortable running shoes",
    "topK": 3,
    "sourceTable": "products"
  }' | jq

# Expected: Products related to footwear/shoes ranked by similarity
# {
#   "query": "comfortable running shoes",
#   "executionTimeMs": 45,
#   "totalResults": 3,
#   "results": [
#     {
#       "id": "prod-001",
#       "sourceTable": "products",
#       "similarity": 0.85,
#       "textContent": "Running Shoes - Comfortable athletic footwear...",
#       "metadata": {...}
#     },
#     ...
#   ]
# }
```

**Verify:**
- Results are ranked by similarity (highest first)
- Similarity scores are between 0 and 1
- Text content matches the query semantically

---

### Test 2: Search for "laptop"

```bash
curl -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "portable computer for work",
    "topK": 3,
    "sourceTable": "products"
  }' | jq

# Expected: Products related to laptops/computers
```

**Verify:**
- Different results than shoes query
- Semantic matching (not just keyword matching)
- "laptop" products rank higher even though query says "computer"

---

### Test 3: Search for "affordable"

```bash
curl -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "cheap budget friendly products",
    "topK": 5,
    "sourceTable": "products"
  }' | jq

# Expected: Lower-priced products ranked higher
```

**Verify:**
- Semantic understanding of "cheap" = "affordable" = "budget"
- Price-related products appear in results

---

### Test 4: Search with No Results

```bash
curl -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "quantum physics textbook",
    "topK": 3,
    "sourceTable": "products"
  }' | jq

# Expected: Low similarity scores or empty results
# (demo data doesn't have physics books)
```

**Verify:**
- System handles queries with no good matches
- Returns results with low similarity scores
- No errors or crashes

---

### Test 5: Search Without Source Table Filter

```bash
curl -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "comfortable shoes",
    "topK": 5
  }' | jq

# Expected: Searches across all tables
```

**Verify:**
- Works without sourceTable parameter
- Returns results from all available tables

---

## Verification

### Verify 1: Embedding Quality

```bash
# Compare embeddings for similar products
curl -X POST http://localhost:8000/api/v1/embed \
  -H "Content-Type: application/json" \
  -d '{
    "texts": [
      "running shoes",
      "athletic footwear",
      "laptop computer"
    ]
  }' | jq '.embeddings' > embeddings.json

# Manually verify:
# - Embeddings for "running shoes" and "athletic footwear" should be similar
# - Embedding for "laptop computer" should be different
```

### Verify 2: Search Consistency

```bash
# Run same search multiple times
for i in {1..3}; do
  echo "Run $i:"
  curl -s -X POST http://localhost:8083/api/search \
    -H "Content-Type: application/json" \
    -d '{"query":"shoes","topK":3,"sourceTable":"products"}' \
    | jq '.results[0].id'
done

# Expected: Same top result each time (deterministic)
```

### Verify 3: Performance

```bash
# Measure search latency
time curl -s -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{"query":"shoes","topK":10,"sourceTable":"products"}' \
  > /dev/null

# Expected: < 100ms for 10 vectors (brute-force)
```

### Verify 4: Index Statistics

```bash
# Get index stats
curl -s http://localhost:8083/api/search/index/stats | jq

# Expected:
# {
#   "totalVectors": 10,
#   "tables": ["products"],
#   "lastUpdated": "2024-01-01T00:00:00Z"
# }
```

**✅ Checkpoint**: All searches working with real embeddings

---

## Dashboard Testing

### Step 1: Open Dashboard

```bash
open http://localhost:3000
```

### Step 2: Verify Overview Page

**Check:**
- ✅ Shows 1 registered table
- ✅ Shows 10 vectors
- ✅ Shows sync status
- ✅ No mock data visible

### Step 3: Test Table Details

**Navigate to:** Tables → products

**Check:**
- ✅ Table configuration displayed
- ✅ Sync state shows last snapshot ID
- ✅ Text columns: name, description
- ✅ Enabled status: true

### Step 4: Test Semantic Search UI

**Navigate to:** Semantic Search

**Test:**
1. Enter query: "comfortable shoes"
2. Select table: products
3. Set top K: 5
4. Click Search

**Verify:**
- ✅ Results appear within 1-2 seconds
- ✅ Results show similarity scores
- ✅ Results show product details
- ✅ Results are ranked by similarity
- ✅ No mock data in results

### Step 5: Test Multiple Searches

**Try these queries:**
1. "laptop computer" → Should return tech products
2. "affordable items" → Should return budget products
3. "sports equipment" → Should return athletic products

**Verify:**
- ✅ Different queries return different results
- ✅ Results make semantic sense
- ✅ UI updates correctly

**✅ Checkpoint**: Dashboard working with real data

---

## Advanced Testing

### Test 1: Add More Data

```bash
# Add 5 more products manually
curl -X POST http://localhost:8081/api/demo/seed | jq

# Trigger sync
curl -X POST http://localhost:8081/api/demo/sync | jq

# Verify vector count increased
curl -s http://localhost:8081/api/vectors/count | jq
# Expected: {"count": 20}

# Search again
curl -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{"query":"shoes","topK":5,"sourceTable":"products"}' | jq

# Verify: More results available
```

### Test 2: Incremental Sync

```bash
# Check current snapshot
curl -s http://localhost:8080/api/tables/${TABLE_ID}/status | jq '.lastSnapshotId'

# Add more data (creates new snapshot)
# ... add data to Iceberg table ...

# Trigger sync again
curl -X POST http://localhost:8081/api/demo/sync | jq

# Verify: Only new records processed (incremental)
docker-compose logs worker | grep "Detected.*new records"
```

### Test 3: Search Performance

```bash
# Benchmark search with different topK values
for k in 1 5 10 20; do
  echo "TopK=$k:"
  time curl -s -X POST http://localhost:8083/api/search \
    -H "Content-Type: application/json" \
    -d "{\"query\":\"shoes\",\"topK\":$k,\"sourceTable\":\"products\"}" \
    > /dev/null
done

# Expected: Linear increase in time (brute-force)
```

### Test 4: Concurrent Searches

```bash
# Run 10 concurrent searches
for i in {1..10}; do
  curl -s -X POST http://localhost:8083/api/search \
    -H "Content-Type: application/json" \
    -d '{"query":"shoes","topK":5,"sourceTable":"products"}' &
done
wait

# Verify: All complete successfully
```

**✅ Checkpoint**: Advanced features working

---

## Cleanup

### Step 1: Stop Services

```bash
# Stop all containers
docker-compose down

# Or keep data and just stop
docker-compose stop
```

### Step 2: Clean Volumes (Optional)

```bash
# Remove all data (fresh start next time)
docker-compose down -v

# Or remove specific volumes
docker volume rm vectorsync_postgres_data
docker volume rm vectorsync_minio_data
```

### Step 3: Clean Images (Optional)

```bash
# Remove built images
docker-compose down --rmi local

# Or remove all unused images
docker image prune -a
```

---

## Troubleshooting

### Issue 1: Embedding Service Not Starting

**Symptoms:**
- Container exits immediately
- Error: "Model not found"

**Solution:**
```bash
# Check logs
docker-compose logs embedding-service

# Rebuild with no cache
docker-compose build --no-cache embedding-service

# Restart
docker-compose up -d embedding-service
```

### Issue 2: Sync Fails

**Symptoms:**
- Sync returns 0 vectors
- Worker logs show errors

**Solution:**
```bash
# Check worker logs
docker-compose logs worker | grep -i error

# Verify embedding service is reachable
docker-compose exec worker curl http://embedding-service:8000/api/v1/health

# Check S3 connection
docker-compose logs worker | grep -i "s3\|minio"
```

### Issue 3: Search Returns No Results

**Symptoms:**
- Search returns empty results
- No errors in logs

**Solution:**
```bash
# Verify vectors exist
curl -s http://localhost:8081/api/vectors/count

# Check vector table
docker-compose logs worker | grep "vector table"

# Rebuild search index
curl -X POST http://localhost:8083/api/search/index/rebuild
```

### Issue 4: Low Similarity Scores

**Symptoms:**
- All similarity scores < 0.5
- Results don't match query

**Solution:**
```bash
# Verify embedding model
curl -X POST http://localhost:8000/api/v1/embed \
  -H "Content-Type: application/json" \
  -d '{"texts":["test"]}' | jq '.model'

# Should be: "sentence-transformers/all-MiniLM-L6-v2"

# Check if embeddings are normalized
# Cosine similarity requires normalized vectors
```

---

## Success Criteria

### ✅ All Tests Pass

- [ ] All services start successfully
- [ ] All health checks return 200 OK
- [ ] Embedding service generates 384-dim vectors
- [ ] Demo data seeds 10 products
- [ ] Table registration succeeds
- [ ] Sync generates 10 vectors
- [ ] Search returns relevant results
- [ ] Similarity scores are reasonable (> 0.5 for good matches)
- [ ] Dashboard shows real data (no mocks)
- [ ] Multiple searches work consistently
- [ ] Performance is acceptable (< 100ms for 10 vectors)

### ✅ Real Embeddings Verified

- [ ] Embedding service uses Sentence Transformers
- [ ] Vectors are 384 dimensions
- [ ] Similar queries return similar results
- [ ] Different queries return different results
- [ ] Semantic matching works (not just keywords)

### ✅ End-to-End Flow Complete

```
Data Seeding → Table Registration → Sync Execution → 
Vector Generation → Vector Storage → Semantic Search → 
Results Display
```

All steps complete without errors.

---

## Next Steps

After successful testing:

1. **Scale Testing**: Add more data (100, 1000, 10000 records)
2. **Performance Testing**: Measure latency at different scales
3. **Integration Testing**: Test with real Iceberg tables
4. **Production Deployment**: Deploy to AWS/Azure
5. **Monitoring**: Set up Prometheus + Grafana
6. **Optimization**: Implement Lucene + HNSW for > 100K vectors

---

## Conclusion

This test plan verifies:
- ✅ Real embedding generation (not mocks)
- ✅ Semantic search with actual similarity
- ✅ Complete end-to-end workflow
- ✅ Dashboard with real data
- ✅ Production-ready system

**Estimated Test Time**: 30 minutes  
**Success Rate**: Should be 100% if all services configured correctly