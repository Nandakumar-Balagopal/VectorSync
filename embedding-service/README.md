# VectorSync Embedding Service

Production-ready embedding service for VectorSync - a distributed system for semantic vector search on Apache Iceberg tables.

## Features

- ✅ **High-throughput batch processing** for CDC ingestion
- ✅ **Low-latency query embedding** for search
- ✅ **Pluggable providers** (self-hosted & managed APIs)
- ✅ **Async/await** throughout for optimal performance
- ✅ **Rate limiting** via semaphore for external APIs
- ✅ **Idempotent operations** with vector ID preservation
- ✅ **Partial failure support** in batch processing
- ✅ **Comprehensive error handling** and retry logic
- ✅ **Structured logging** with request tracing
- ✅ **Health checks** and monitoring endpoints

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   FastAPI Application                    │
├─────────────────────────────────────────────────────────┤
│  POST /api/v1/embed          │  Batch embedding         │
│  POST /api/v1/embed-query    │  Query embedding         │
│  GET  /api/v1/health         │  Health check            │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              Embedding Service (Orchestrator)            │
│  • Provider selection                                    │
│  • Batching strategy                                     │
│  • Error handling                                        │
│  • Result mapping                                        │
└─────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
┌──────────────────────┐      ┌──────────────────────┐
│  Self-Hosted Provider│      │  Managed Provider    │
│  • sentence-trans... │      │  • OpenAI API        │
│  • Thread pool exec  │      │  • Gemini API        │
│  • CPU/GPU support   │      │  • Rate limiting     │
│  • Local models      │      │  • Retry logic       │
└──────────────────────┘      └──────────────────────┘
```

## Quick Start

### 1. Setup with Virtual Environment

**Automated Setup (Recommended):**
```bash
# Run the setup script
chmod +x setup.sh
./setup.sh

# Activate virtual environment
source venv/bin/activate
```

**Manual Setup:**
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Linux/Mac
# OR
venv\Scripts\activate     # On Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` with your configuration:

```bash
# Provider Configuration
DEFAULT_PROVIDER=self_hosted
DEFAULT_MODEL=all-MiniLM-L6-v2

# For managed providers (OpenAI, etc.)
MANAGED_API_URL=https://api.openai.com/v1
MANAGED_API_KEY=your-api-key-here
```

### 3. Run Locally

**Make sure virtual environment is activated:**
```bash
source venv/bin/activate  # On Linux/Mac
# OR
venv\Scripts\activate     # On Windows
```

**Start the service:**
```bash
# Development mode with auto-reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Production mode
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### 4. Run with Docker

```bash
# Build image
docker build -t vectorsync-embedding-service .

# Run container
docker run -p 8000:8000 \
  -e DEFAULT_PROVIDER=self_hosted \
  -e DEFAULT_MODEL=all-MiniLM-L6-v2 \
  vectorsync-embedding-service
```

## API Documentation

### POST /api/v1/embed (Batch Embedding)

Generate embeddings for multiple records (optimized for high-throughput ingestion).

**Request:**
```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "records": [
    {
      "vector_id": "vec-001",
      "source_table": "products",
      "source_row_id": "prod-123",
      "text": "Lightweight running shoes for trail running",
      "metadata": {
        "snapshot_id": "12345",
        "column": "description"
      },
      "model_name": "all-MiniLM-L6-v2",
      "provider": "self_hosted"
    }
  ]
}
```

**Response:**
```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "results": [
    {
      "vector_id": "vec-001",
      "embedding": [0.123, -0.456, 0.789, ...]
    }
  ],
  "total_records": 1,
  "successful": 1,
  "failed": 0,
  "processing_time_ms": 45.2
}
```

**Features:**
- Supports batches up to `MAX_BATCH_SIZE` (default: 64)
- Automatic sub-batching for larger requests
- Partial failure support (returns successful embeddings even if some fail)
- Preserves `vector_id` mapping for idempotency

### POST /api/v1/embed-query (Query Embedding)

Generate embedding for a single query text (optimized for low-latency search).

**Request:**
```json
{
  "text": "affordable running shoes",
  "model_name": "all-MiniLM-L6-v2",
  "provider": "self_hosted"
}
```

**Response:**
```json
{
  "embedding": [0.123, -0.456, 0.789, ...],
  "model_name": "all-MiniLM-L6-v2",
  "dimension": 384,
  "processing_time_ms": 12.5
}
```

### GET /api/v1/health

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "providers": {
    "self_hosted": true,
    "managed": true
  },
  "models_loaded": ["all-MiniLM-L6-v2"]
}
```

## Configuration Reference

### Service Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE_NAME` | `vectorsync-embedding-service` | Service name |
| `SERVICE_VERSION` | `1.0.0` | Service version |
| `HOST` | `0.0.0.0` | Bind host |
| `PORT` | `8000` | Bind port |
| `LOG_LEVEL` | `INFO` | Logging level |

### Provider Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DEFAULT_PROVIDER` | `self_hosted` | Default provider (`self_hosted` or `managed`) |
| `DEFAULT_MODEL` | `all-MiniLM-L6-v2` | Default model name |

### Self-Hosted Provider

| Variable | Default | Description |
|----------|---------|-------------|
| `SELF_HOSTED_DEVICE` | `cpu` | Device (`cpu`, `cuda`, `mps`) |
| `SELF_HOSTED_MAX_WORKERS` | `4` | Thread pool size |

### Managed Provider

| Variable | Default | Description |
|----------|---------|-------------|
| `MANAGED_API_URL` | - | API base URL |
| `MANAGED_API_KEY` | - | API key |
| `MANAGED_MAX_CONCURRENT` | `10` | Max concurrent requests |
| `MANAGED_TIMEOUT` | `30.0` | Request timeout (seconds) |
| `MANAGED_MAX_RETRIES` | `3` | Max retry attempts |

### Batching Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_BATCH_SIZE` | `64` | Maximum batch size |
| `ENABLE_TOKEN_BATCHING` | `false` | Token-aware batching |

## Testing

**Make sure virtual environment is activated:**
```bash
source venv/bin/activate
```

**Run tests:**
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_embedding_service.py -v

# View coverage report
open htmlcov/index.html  # On Mac
# OR
xdg-open htmlcov/index.html  # On Linux
```

## Performance Considerations

### Self-Hosted Provider

- **CPU Mode**: ~50-100 embeddings/second (depends on model)
- **GPU Mode**: ~500-1000 embeddings/second (with CUDA)
- **Memory**: ~2GB for MiniLM, ~4GB for larger models
- **Thread Pool**: Adjust `SELF_HOSTED_MAX_WORKERS` based on CPU cores

### Managed Provider

- **Throughput**: Limited by API rate limits
- **Latency**: ~100-500ms per request (network dependent)
- **Concurrency**: Controlled by `MANAGED_MAX_CONCURRENT`
- **Cost**: Pay per token/request

## Production Deployment

### Docker Compose

```yaml
services:
  embedding-service:
    image: vectorsync-embedding-service:latest
    ports:
      - "8000:8000"
    environment:
      - DEFAULT_PROVIDER=self_hosted
      - DEFAULT_MODEL=all-MiniLM-L6-v2
      - SELF_HOSTED_DEVICE=cpu
      - LOG_LEVEL=INFO
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 4G
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/api/v1/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: embedding-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: embedding-service
  template:
    metadata:
      labels:
        app: embedding-service
    spec:
      containers:
      - name: embedding-service
        image: vectorsync-embedding-service:latest
        ports:
        - containerPort: 8000
        env:
        - name: DEFAULT_PROVIDER
          value: "self_hosted"
        - name: DEFAULT_MODEL
          value: "all-MiniLM-L6-v2"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /api/v1/health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
```

## Troubleshooting

### Issue: Slow embedding generation

**Solution:**
- Use GPU if available (`SELF_HOSTED_DEVICE=cuda`)
- Increase thread pool size (`SELF_HOSTED_MAX_WORKERS=8`)
- Use smaller model (e.g., `all-MiniLM-L6-v2` instead of `all-mpnet-base-v2`)

### Issue: Out of memory errors

**Solution:**
- Reduce `MAX_BATCH_SIZE`
- Reduce `SELF_HOSTED_MAX_WORKERS`
- Use smaller model
- Increase container memory limits

### Issue: External API rate limits

**Solution:**
- Reduce `MANAGED_MAX_CONCURRENT`
- Increase `MANAGED_TIMEOUT`
- Implement exponential backoff (already included)

## Development

### Project Structure

```
embedding-service/
├── app/
│   ├── api/
│   │   └── routes.py          # FastAPI routes
│   ├── models/
│   │   ├── requests.py        # Request models
│   │   └── responses.py       # Response models
│   ├── providers/
│   │   ├── base.py            # Provider interface
│   │   ├── self_hosted.py     # Self-hosted provider
│   │   └── managed.py         # Managed provider
│   ├── services/
│   │   └── embedding_service.py  # Core service
│   ├── config.py              # Configuration
│   └── main.py                # Application entry point
├── tests/
│   └── test_embedding_service.py
├── requirements.txt
├── pyproject.toml
├── Dockerfile
└── README.md
```

### Code Quality

```bash
# Format code
black app/ tests/

# Lint code
ruff check app/ tests/

# Type checking
mypy app/
```

## License

Apache 2.0

## Support

For issues and questions:
- GitHub Issues: https://github.com/your-org/vectorsync
- Documentation: https://vectorsync.io/docs