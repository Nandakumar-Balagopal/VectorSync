# VectorSync Demo Commands

## Quick Start - Run All Services (Including Dashboard)

Yes, the dashboard is included and will automatically start on port 3000!

### 0. Stop Any Existing Containers (IMPORTANT!)
```bash
# Check what's running
docker ps

# Stop ALL old containers (they have conflicting ports and passwords)
docker stop $(docker ps -q)

# Remove them to avoid conflicts
docker rm $(docker ps -aq)

# Verify nothing is running
docker ps
```

**Why this is critical:** Old containers (like `vectororch-postgres`) use different passwords and will cause authentication failures!

### 1. Start All Services
```bash
docker-compose --profile local-storage --profile local-embedding up -d --build
```

This starts:
- ✅ PostgreSQL (port 5433)
- ✅ MinIO (ports 9000, 9001)
- ✅ Python Embedding Service (port 8000)
- ✅ Control API (port 8080)
- ✅ Worker (port 8081)
- ✅ Search API (port 8082)
- ✅ **Dashboard UI (port 3000)** ← Your web interface!

### 2. Wait for Services (60 seconds)
```bash
sleep 60
```

### 3. Create MinIO Bucket
```bash
docker exec vectorsync-minio mc alias set myminio http://localhost:9000 minioadmin minioadmin123
docker exec vectorsync-minio mc mb myminio/vectorsync-warehouse
```

### 4. Check Embedding Service Health
```bash
curl http://localhost:8000/health | python3 -m json.tool
```

### 5. Register Demo Table
```bash
curl -X POST http://localhost:8080/api/tables/register \
  -H "Content-Type: application/json" \
  -d '{
    "catalogName": "iceberg_data",
    "schemaName": "demo",
    "tableName": "products",
    "textColumn": "description",
    "vectorDimension": 384,
    "syncEnabled": true
  }' | python3 -m json.tool
```

### 6. Seed Demo Data
```bash
curl -X POST http://localhost:8081/api/demo/seed | python3 -m json.tool
```

### 7. Trigger Initial Sync
```bash
curl -X POST http://localhost:8081/api/demo/sync | python3 -m json.tool
```

### 8. Wait for Sync (30 seconds)
```bash
sleep 30
```

### 9. Check Vector Count
```bash
curl http://localhost:8081/api/vectors/count
```

### 10. Test Search
```bash
curl -X POST http://localhost:8082/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "laptop computer",
    "sourceTable": "demo.products",
    "topK": 3
  }' | python3 -m json.tool
```

### 11. Test Incremental Update
```bash
# Add more data
curl -X POST http://localhost:8081/api/demo/seed | python3 -m json.tool

# Trigger sync
curl -X POST http://localhost:8081/api/demo/sync | python3 -m json.tool

# Wait
sleep 20

# Check new count
curl http://localhost:8081/api/vectors/count
```

---

## Access Points

### 🌐 Web Interfaces
- **Dashboard**: http://localhost:3000 ← **Open this in your browser!**
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)

### 🔌 API Endpoints
- **Control API**: http://localhost:8080
- **Worker API**: http://localhost:8081
- **Search API**: http://localhost:8082
- **Embedding Service**: http://localhost:8000

### 🗄️ Database
- **PostgreSQL**: localhost:5433 (postgres/vectorsync123)

---

## Monitoring Commands

### Check Service Status
```bash
docker compose ps
```

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f worker
docker compose logs -f embedding-service
docker compose logs -f dashboard
```

### Check Sync Status
```bash
curl http://localhost:8080/api/sync/status | python3 -m json.tool
```

### List Registered Tables
```bash
curl http://localhost:8080/api/tables | python3 -m json.tool
```

---

## Cleanup

### Stop All Services
```bash
docker compose --profile local-storage --profile local-embedding down
```

### Stop and Remove Volumes (Complete Reset)
```bash
docker compose --profile local-storage --profile local-embedding down -v
```

---

## Dashboard Features

Once you open http://localhost:3000, you'll see:

1. **System Metrics** - Real-time sync status, vector count, latency
2. **Vectorized Tables** - List of registered tables with sync status
3. **Semantic Playground** - Test search queries interactively

The dashboard automatically connects to all backend services through nginx proxy!

---

## Troubleshooting

### Docker Daemon Not Running (Colima Users)
If you see "Cannot connect to the Docker daemon" even though `colima status` shows it's running:

**Solution: Set the DOCKER_HOST environment variable**

```bash
# Option 1: Set for current session
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"

# Option 2: Add to your shell profile (~/.zshrc or ~/.bashrc)
echo 'export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"' >> ~/.zshrc
source ~/.zshrc

# Option 3: Use colima's docker context
colima start
docker context use colima

# Then verify docker works
docker ps

# Now run docker-compose
docker-compose --profile local-storage --profile local-embedding up -d --build
```

**Quick Fix:**
```bash
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
docker-compose --profile local-storage --profile local-embedding up -d --build
```

### Port Already in Use (e.g., port 8000)
If you see "Bind for 0.0.0.0:8000 failed: port is already allocated":

```bash
# Option 1: Stop the process using the port
lsof -ti:8000 | xargs kill -9

# Option 2: Find and stop the process manually
lsof -i:8000
# Then kill the PID shown

# Option 3: Stop all containers and try again (use 'docker compose' not 'docker-compose')
docker compose --profile local-storage --profile local-embedding down
docker compose --profile local-storage --profile local-embedding up -d --build
```

**Note:** Use `docker compose` (with space) not `docker-compose` (with hyphen) for newer Docker versions.

### If a service fails to start:
```bash
# Check logs for the failed service
docker logs vectorsync-control-api
docker logs vectorsync-worker
docker logs vectorsync-search-api

# Common issues:
# 1. Database not ready - wait and restart
docker-compose restart control-api

# 2. MinIO bucket not created - create it first
docker exec vectorsync-minio mc alias set myminio http://localhost:9000 minioadmin minioadmin123
docker exec vectorsync-minio mc mb myminio/vectorsync-warehouse

# 3. Check all service status
docker-compose ps
```

### Maven Build Failures (Network Issues)
If you see "Premature end of Content-Length" or Maven download errors:

```bash
# This is a temporary network issue - just retry the build
docker-compose --profile local-storage --profile local-embedding up -d --build

# Or rebuild specific service
docker-compose build worker
docker-compose up -d worker
```

**Why this happens:** Maven downloads were interrupted. Simply retry and it will resume from where it left off.

### If embedding service is slow:
The first request takes longer as it loads the model. Subsequent requests are fast.

### If MinIO bucket creation fails:
It's okay if it says "bucket already exists" - that means it's already set up.

---

## Complete One-Liner Setup

```bash
docker compose --profile local-storage --profile local-embedding up -d --build && \
sleep 60 && \
docker exec vectorsync-minio mc alias set myminio http://localhost:9000 minioadmin minioadmin123 && \
docker exec vectorsync-minio mc mb myminio/vectorsync-warehouse && \
curl -X POST http://localhost:8080/api/tables/register -H "Content-Type: application/json" -d '{"catalogName":"iceberg_data","schemaName":"demo","tableName":"products","textColumn":"description","vectorDimension":384,"syncEnabled":true}' && \
curl -X POST http://localhost:8081/api/demo/seed && \
sleep 5 && \
curl -X POST http://localhost:8081/api/demo/sync && \
echo "Setup complete! Open http://localhost:3000 in your browser"