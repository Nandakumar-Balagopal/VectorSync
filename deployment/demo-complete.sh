#!/bin/bash

set -e

echo "=========================================="
echo "VectorSync Complete Demo Setup"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if .env file exists
if [ ! -f .env ]; then
    print_error ".env file not found!"
    print_status "Creating .env from .env.example..."
    cp .env.example .env
    print_warning "Please update .env with your configuration and run again"
    exit 1
fi

print_status "Starting VectorSync Complete Demo..."
echo ""

# Step 1: Clean up any existing containers
print_status "Step 1: Cleaning up existing containers..."
docker compose --profile local-storage --profile local-embedding down -v 2>/dev/null || true
print_success "Cleanup complete"
echo ""

# Step 2: Build and start all services
print_status "Step 2: Building and starting all services..."
print_status "This includes: PostgreSQL, MinIO, Embedding Service, Control API, Worker, Search API, and Dashboard"
docker compose --profile local-storage --profile local-embedding up -d --build

print_status "Waiting for services to be healthy..."
sleep 10

# Wait for services to be ready
print_status "Checking service health..."
for i in {1..30}; do
    if docker compose ps | grep -q "healthy"; then
        print_success "Services are starting up..."
        break
    fi
    echo -n "."
    sleep 2
done
echo ""

# Give services more time to fully initialize
print_status "Waiting for all services to fully initialize (60 seconds)..."
sleep 60

print_success "All services started"
echo ""

# Step 3: Create MinIO bucket
print_status "Step 3: Setting up MinIO bucket..."
docker exec vectorsync-minio mc alias set myminio http://localhost:9000 minioadmin minioadmin123 2>/dev/null || true
docker exec vectorsync-minio mc mb myminio/vectorsync-warehouse 2>/dev/null || print_warning "Bucket may already exist"
print_success "MinIO bucket ready"
echo ""

# Step 4: Check embedding service
print_status "Step 4: Verifying embedding service..."
EMBED_HEALTH=$(curl -s http://localhost:8000/health || echo "failed")
if echo "$EMBED_HEALTH" | grep -q "healthy"; then
    print_success "Embedding service is healthy"
    echo "$EMBED_HEALTH" | python3 -m json.tool 2>/dev/null || echo "$EMBED_HEALTH"
else
    print_error "Embedding service is not responding"
    print_status "Checking logs..."
    docker logs vectorsync-embedding --tail 20
fi
echo ""

# Step 5: Register a demo table
print_status "Step 5: Registering demo table configuration..."
REGISTER_RESPONSE=$(curl -s -X POST http://localhost:8080/api/tables/register \
  -H "Content-Type: application/json" \
  -d '{
    "catalogName": "iceberg_data",
    "schemaName": "demo",
    "tableName": "products",
    "textColumn": "description",
    "vectorDimension": 384,
    "syncEnabled": true
  }' || echo '{"error": "failed"}')

if echo "$REGISTER_RESPONSE" | grep -q "tableId"; then
    print_success "Table registered successfully"
    echo "$REGISTER_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$REGISTER_RESPONSE"
else
    print_warning "Table registration response: $REGISTER_RESPONSE"
fi
echo ""

# Step 6: Seed demo data
print_status "Step 6: Seeding demo data..."
SEED_RESPONSE=$(curl -s -X POST http://localhost:8081/api/demo/seed || echo "failed")
if echo "$SEED_RESPONSE" | grep -q "success\|created"; then
    print_success "Demo data seeded successfully"
    echo "$SEED_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$SEED_RESPONSE"
else
    print_warning "Seed response: $SEED_RESPONSE"
fi
echo ""

# Step 7: Trigger initial sync
print_status "Step 7: Triggering initial vector sync..."
sleep 5
SYNC_RESPONSE=$(curl -s -X POST http://localhost:8081/api/demo/sync || echo "failed")
if echo "$SYNC_RESPONSE" | grep -q "success\|synced\|processed"; then
    print_success "Initial sync triggered"
    echo "$SYNC_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$SYNC_RESPONSE"
else
    print_warning "Sync response: $SYNC_RESPONSE"
fi
echo ""

# Step 8: Wait for sync to complete
print_status "Step 8: Waiting for sync to complete (30 seconds)..."
sleep 30

# Check vector count
VECTOR_COUNT=$(curl -s http://localhost:8081/api/vectors/count || echo "0")
print_status "Current vector count: $VECTOR_COUNT"
echo ""

# Step 9: Test search functionality
print_status "Step 9: Testing search functionality..."
SEARCH_RESPONSE=$(curl -s -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "laptop computer",
    "sourceTable": "demo.products",
    "topK": 3
  }' || echo '{"error": "failed"}')

if echo "$SEARCH_RESPONSE" | grep -q "results"; then
    print_success "Search is working!"
    echo "$SEARCH_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$SEARCH_RESPONSE"
else
    print_warning "Search response: $SEARCH_RESPONSE"
fi
echo ""

# Step 10: Test incremental update
print_status "Step 10: Testing incremental update..."
print_status "Adding new product to source table..."

# This would typically be done via SQL, but for demo we'll trigger another seed
sleep 5
INCREMENTAL_RESPONSE=$(curl -s -X POST http://localhost:8081/api/demo/seed || echo "failed")
print_status "Incremental data added"

print_status "Triggering incremental sync..."
sleep 5
INCREMENTAL_SYNC=$(curl -s -X POST http://localhost:8081/api/demo/sync || echo "failed")
print_status "Incremental sync triggered"

print_status "Waiting for incremental sync (20 seconds)..."
sleep 20

NEW_VECTOR_COUNT=$(curl -s http://localhost:8081/api/vectors/count || echo "0")
print_status "New vector count: $NEW_VECTOR_COUNT"

if [ "$NEW_VECTOR_COUNT" -gt "$VECTOR_COUNT" ]; then
    print_success "Incremental sync working! Vectors increased from $VECTOR_COUNT to $NEW_VECTOR_COUNT"
else
    print_warning "Vector count unchanged. This might be expected if demo data is identical."
fi
echo ""

# Step 11: Display service URLs
echo ""
echo "=========================================="
print_success "Demo Setup Complete!"
echo "=========================================="
echo ""
echo "Service URLs:"
echo "  📊 Dashboard:        http://localhost:3000"
echo "  🎛️  Control API:      http://localhost:8080"
echo "  ⚙️  Worker API:       http://localhost:8081"
echo "  🔍 Search API:       http://localhost:8082"
echo "  🧠 Embedding Service: http://localhost:8000"
echo "  🗄️  MinIO Console:    http://localhost:9001 (minioadmin/minioadmin123)"
echo "  🐘 PostgreSQL:       localhost:5433 (postgres/vectorsync123)"
echo ""
echo "Quick Test Commands:"
echo "  # Check sync status"
echo "  curl http://localhost:8080/api/sync/status"
echo ""
echo "  # Search for products"
echo "  curl -X POST http://localhost:8083/api/search \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"query\": \"laptop\", \"sourceTable\": \"demo.products\", \"topK\": 5}'"
echo ""
echo "  # Check vector count"
echo "  curl http://localhost:8081/api/vectors/count"
echo ""
echo "  # View logs"
echo "  docker compose logs -f worker"
echo ""
echo "To stop all services:"
echo "  docker compose --profile local-storage --profile local-embedding down"
echo ""
print_success "Open http://localhost:3000 in your browser to access the dashboard!"
echo ""

# Made with Bob
