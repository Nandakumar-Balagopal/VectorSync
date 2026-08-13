# VectorSync Deployment Guide

This guide explains how to deploy VectorSync in different configurations based on your infrastructure and requirements.

## Table of Contents

1. [Deployment Profiles](#deployment-profiles)
2. [Configuration Files](#configuration-files)
3. [Deployment Scenarios](#deployment-scenarios)
4. [Profile Reference](#profile-reference)
5. [Troubleshooting](#troubleshooting)

---

## Deployment Profiles

VectorSync uses Docker Compose profiles to conditionally deploy services based on your configuration. This allows you to:

- **Use local services** (MinIO, embedding service) for development
- **Use external services** (AWS S3, OpenAI API) for production
- **Mix and match** based on your needs

### Available Profiles

| Profile | Service | When to Use |
|---------|---------|-------------|
| `local-storage` | MinIO | When you want local S3-compatible storage |
| `local-embedding` | Python Embedding Service | When you want local embedding generation |
| (none) | Core Services Only | When using external S3 and SaaS embeddings |

---

## Configuration Files

We provide three pre-configured `.env` templates for common scenarios:

### 1. `.env.local` - Full Local Stack
**Use Case:** Local development, testing, no external dependencies

```bash
# Copy the local configuration
cp .env.local .env

# Start all services including MinIO and embedding service
docker-compose --profile local-storage --profile local-embedding up -d
```

**What runs:**
- ✅ PostgreSQL (metadata)
- ✅ MinIO (local S3)
- ✅ Python Embedding Service (sentence-transformers)
- ✅ Control API
- ✅ Worker
- ✅ Search API

**Embedding Model:** `all-MiniLM-L6-v2` (384 dimensions)

---

### 2. `.env.openai` - Full SaaS Stack
**Use Case:** Production deployment with external services

```bash
# Copy the OpenAI configuration
cp .env.openai .env

# Update with your credentials:
# - AWS_S3_ACCESS_KEY / AWS_S3_SECRET_KEY
# - EMBEDDING_EXTERNAL_API_KEY (OpenAI API key)
# - ICEBERG_CATALOG_WAREHOUSE (your S3 bucket)

# Start only core services (no MinIO or embedding service)
docker-compose up -d
```

**What runs:**
- ✅ PostgreSQL (metadata)
- ✅ Control API
- ✅ Worker
- ✅ Search API
- ❌ MinIO (using external S3)
- ❌ Embedding Service (using OpenAI API)

**Embedding Model:** `text-embedding-3-small` (1536 dimensions)

⚠️ **Important:** Update [`Constants.EMBEDDING_DIMENSION`](common/src/main/java/io/vectorsync/common/Constants.java:8) to `1536` when using OpenAI.

---

### 3. `.env.hybrid` - Mixed Configuration
**Use Case:** Local storage with cloud embeddings, or vice versa

```bash
# Copy the hybrid configuration
cp .env.hybrid .env

# Update with your embedding API key
# Start with local storage only
docker-compose --profile local-storage up -d
```

**What runs:**
- ✅ PostgreSQL (metadata)
- ✅ MinIO (local S3)
- ✅ Control API
- ✅ Worker
- ✅ Search API
- ❌ Embedding Service (using external API)

---

## Deployment Scenarios

### Scenario 1: Quick Local Demo

**Goal:** Get VectorSync running locally with zero external dependencies.

```bash
# 1. Copy local configuration
cp .env.local .env

# 2. Start all services
docker-compose --profile local-storage --profile local-embedding up -d

# 3. Wait for services to be healthy
docker-compose ps

# 4. Run demo
./deployment/demo-run.sh
```

**Expected Output:**
- All services start successfully
- Demo seeds data and performs search
- Results returned with similarity scores

---

### Scenario 2: Production with AWS S3 and OpenAI

**Goal:** Deploy to production using AWS infrastructure and OpenAI embeddings.

```bash
# 1. Copy OpenAI configuration
cp .env.openai .env

# 2. Update credentials in .env
# - AWS_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
# - AWS_S3_ACCESS_KEY=your-key
# - AWS_S3_SECRET_KEY=your-secret
# - ICEBERG_CATALOG_WAREHOUSE=s3a://your-bucket/warehouse
# - EMBEDDING_EXTERNAL_API_KEY=sk-...

# 3. Update embedding dimension
# Edit common/src/main/java/io/vectorsync/common/Constants.java
# Change EMBEDDING_DIMENSION from 384 to 1536

# 4. Rebuild services
docker-compose build

# 5. Start services (no profiles needed)
docker-compose up -d
```

**Cost Considerations:**
- OpenAI API: ~$0.00002 per 1K tokens
- AWS S3: Standard storage and request costs
- No local compute for embeddings

---

### Scenario 3: IBM Cloud Object Storage with Local Embeddings

**Goal:** Use IBM COS for storage but keep embeddings local for cost control.

```bash
# 1. Copy hybrid configuration
cp .env.hybrid .env

# 2. Update S3 configuration for IBM COS
# - AWS_S3_ENDPOINT=https://s3.us-south.cloud-object-storage.appdomain.cloud
# - AWS_S3_ACCESS_KEY=your-ibm-access-key
# - AWS_S3_SECRET_KEY=your-ibm-secret-key
# - ICEBERG_CATALOG_WAREHOUSE=s3a://your-cos-bucket/warehouse

# 3. Update embedding configuration for local service
# - EMBEDDING_EXTERNAL_API_URL=http://embedding-service:8000/embed
# - Remove EMBEDDING_EXTERNAL_API_KEY

# 4. Start with local embedding service
docker-compose --profile local-embedding up -d
```

---

## Profile Reference

### Starting Services with Profiles

```bash
# No profiles - Core services only (postgres, control-api, worker, search-api)
docker-compose up -d

# With local storage
docker-compose --profile local-storage up -d

# With local embeddings
docker-compose --profile local-embedding up -d

# With both local storage and embeddings
docker-compose --profile local-storage --profile local-embedding up -d
```

### Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

### Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f worker

# Embedding service (if running)
docker-compose logs -f embedding-service
```

---

## Service Dependencies

The services have the following dependency chain:

```
postgres (always required)
  ↓
control-api
  ↓
worker → embedding-service (optional, if local-embedding profile)
  ↓
search-api → embedding-service (optional, if local-embedding profile)
```

**Key Points:**
- PostgreSQL is always required (metadata storage)
- MinIO only starts with `local-storage` profile
- Embedding service only starts with `local-embedding` profile
- Worker and Search API adapt based on `EMBEDDING_PROVIDER` configuration

---

## Environment Variables Reference

### Required Variables (All Deployments)

```bash
# PostgreSQL
POSTGRES_USER=vectorsync
POSTGRES_PASSWORD=vectorsync123
POSTGRES_DB=vectorsync

# Iceberg
ICEBERG_CATALOG_WAREHOUSE=s3a://bucket/warehouse
ICEBERG_VECTOR_NAMESPACE=vectorsync

# S3 Configuration
AWS_S3_ENDPOINT=http://minio:9000 or https://s3.amazonaws.com
AWS_S3_ACCESS_KEY=your-key
AWS_S3_SECRET_KEY=your-secret
AWS_S3_PATH_STYLE_ACCESS=true (MinIO) or false (AWS)
AWS_REGION=us-east-1
```

### Embedding Configuration

#### For Local Embedding Service:
```bash
EMBEDDING_PROVIDER=external
EMBEDDING_EXTERNAL_TYPE=http
EMBEDDING_EXTERNAL_API_URL=http://embedding-service:8000/embed
EMBEDDING_EXTERNAL_REQUEST_TEMPLATE={"text":"${text}"}
EMBEDDING_EXTERNAL_RESPONSE_JSON_POINTER=/embedding
```

#### For OpenAI:
```bash
EMBEDDING_PROVIDER=external
EMBEDDING_EXTERNAL_TYPE=http
EMBEDDING_EXTERNAL_API_URL=https://api.openai.com/v1/embeddings
EMBEDDING_EXTERNAL_API_KEY=sk-...
EMBEDDING_EXTERNAL_MODEL=text-embedding-3-small
EMBEDDING_EXTERNAL_REQUEST_TEMPLATE={"input":"${text}","model":"text-embedding-3-small"}
EMBEDDING_EXTERNAL_RESPONSE_JSON_POINTER=/data/0/embedding
```

#### For Google Gemini:
```bash
EMBEDDING_PROVIDER=external
EMBEDDING_EXTERNAL_TYPE=http
EMBEDDING_EXTERNAL_API_URL=https://generativelanguage.googleapis.com/v1beta/models/embedding-001:embedContent
EMBEDDING_EXTERNAL_API_KEY=your-gemini-key
EMBEDDING_EXTERNAL_REQUEST_TEMPLATE={"content":{"parts":[{"text":"${text}"}]}}
EMBEDDING_EXTERNAL_RESPONSE_JSON_POINTER=/embedding/values
```

---

## Troubleshooting

### Issue: Services fail to start

**Check health status:**
```bash
docker-compose ps
```

**View logs:**
```bash
docker-compose logs -f
```

**Common causes:**
- Port conflicts (8080, 8081, 8082, 9000, 5432)
- Missing environment variables
- Incorrect S3 credentials

---

### Issue: Embedding service not responding

**If using local embedding service:**
```bash
# Check if service is running
docker-compose ps embedding-service

# Check logs
docker-compose logs -f embedding-service

# Test health endpoint
curl http://localhost:8000/health
```

**If using external API:**
- Verify API key is correct
- Check API endpoint URL
- Verify request template matches API format

---

### Issue: MinIO not accessible

**Check MinIO is running:**
```bash
docker-compose ps minio
```

**Access MinIO console:**
- URL: http://localhost:9001
- Username: minioadmin
- Password: minioadmin123

**Verify bucket exists:**
```bash
# Install mc (MinIO client)
# Create bucket if needed
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc mb local/vectorsync
```

---

### Issue: Wrong embedding dimensions

**Symptom:** Errors about dimension mismatch

**Solution:**
1. Check your embedding model's output dimensions
2. Update [`Constants.EMBEDDING_DIMENSION`](common/src/main/java/io/vectorsync/common/Constants.java:8)
3. Rebuild services: `docker-compose build`
4. Restart: `docker-compose up -d`

**Common dimensions:**
- `all-MiniLM-L6-v2`: 384
- `text-embedding-3-small`: 1536
- `text-embedding-3-large`: 3072
- `text-embedding-ada-002`: 1536

---

## Performance Tuning

### Local Embedding Service

**Adjust worker count in [`embedding-service/app.py`](embedding-service/app.py:112):**
```python
uvicorn.run(app, host="0.0.0.0", port=8000, workers=4)
```

**Adjust batch size:**
```python
MAX_BATCH_SIZE = 32  # Increase for better throughput
```

### Worker Service

**Adjust sync interval in [`worker/src/main/resources/application.yml`](worker/src/main/resources/application.yml):**
```yaml
sync:
  scheduler:
    fixed-delay: 30000  # milliseconds
```

---

## Next Steps

1. **Choose your deployment scenario** from the options above
2. **Copy the appropriate `.env` file**
3. **Update credentials and endpoints**
4. **Start services with correct profiles**
5. **Run the demo** to verify everything works
6. **Monitor logs** for any issues

For more information:
- [Embedding Integration Guide](EMBEDDING_INTEGRATION.md)
- [Architecture Documentation](docs/architecture.md)
- [Performance Roadmap](PERFORMANCE_ROADMAP.md)