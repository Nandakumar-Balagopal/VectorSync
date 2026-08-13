#!/bin/bash

# VectorSync End-to-End Test with Real Embeddings
# This script tests the complete workflow with real embedding generation

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test configuration
CONTROL_PLANE_URL="http://localhost:8080"
WORKER_URL="http://localhost:8081"
SEARCH_URL="http://localhost:8083"
EMBEDDING_URL="http://localhost:8000"
DASHBOARD_URL="http://localhost:3000"

# Helper functions
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

wait_for_service() {
    local url=$1
    local service_name=$2
    local max_attempts=30
    local attempt=1
    
    print_info "Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            print_success "$service_name is ready"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    print_error "$service_name failed to start"
    return 1
}

# Main test flow
main() {
    print_header "VectorSync E2E Test with Real Embeddings"
    
    # Step 1: Environment Setup
    print_header "Step 1: Environment Setup"
    
    if [ ! -f ".env" ]; then
        print_info "Creating .env from .env.local"
        cp .env.local .env
    fi
    
    print_success "Environment file ready"
    
    # Step 2: Clean Previous State
    print_header "Step 2: Cleaning Previous State"
    
    print_info "Stopping existing containers..."
    docker-compose down -v > /dev/null 2>&1 || true
    
    print_success "Previous state cleaned"
    
    # Step 3: Start Services
    print_header "Step 3: Starting Services"
    
    print_info "Starting all services with local storage and embedding service..."
    docker-compose --profile local-storage --profile local-embedding up -d
    
    print_success "Services started"
    
    # Step 4: Health Checks
    print_header "Step 4: Health Checks"
    
    wait_for_service "$CONTROL_PLANE_URL/actuator/health" "Control Plane"
    wait_for_service "$WORKER_URL/api/vectors/health" "Worker"
    wait_for_service "$SEARCH_URL/api/search/health" "Search Service"
    wait_for_service "$EMBEDDING_URL/api/v1/health" "Embedding Service"
    wait_for_service "$DASHBOARD_URL" "Dashboard"
    
    print_success "All services are healthy"
    
    # Step 5: Test Embedding Service
    print_header "Step 5: Testing Embedding Service"
    
    print_info "Generating test embedding..."
    EMBED_RESPONSE=$(curl -s -X POST "$EMBEDDING_URL/api/v1/embed" \
        -H "Content-Type: application/json" \
        -d '{"texts":["test sentence"],"model":"sentence-transformers/all-MiniLM-L6-v2"}')
    
    DIMENSIONS=$(echo "$EMBED_RESPONSE" | jq -r '.dimensions')
    
    if [ "$DIMENSIONS" = "384" ]; then
        print_success "Embedding service working (384 dimensions)"
    else
        print_error "Embedding service returned wrong dimensions: $DIMENSIONS"
        exit 1
    fi
    
    # Step 6: Seed Demo Data
    print_header "Step 6: Seeding Demo Data"
    
    print_info "Creating demo products table..."
    SEED_RESPONSE=$(curl -s -X POST "$WORKER_URL/api/demo/seed")
    
    STATUS=$(echo "$SEED_RESPONSE" | jq -r '.status')
    RECORDS=$(echo "$SEED_RESPONSE" | jq -r '.recordsCreated')
    
    if [ "$STATUS" = "success" ]; then
        print_success "Demo data seeded: $RECORDS products created"
    else
        print_error "Failed to seed demo data"
        echo "$SEED_RESPONSE" | jq
        exit 1
    fi
    
    # Step 7: Register Table
    print_header "Step 7: Registering Table"
    
    print_info "Registering products table..."
    REGISTER_RESPONSE=$(curl -s -X POST "$CONTROL_PLANE_URL/api/tables/register" \
        -H "Content-Type: application/json" \
        -d '{
            "catalogName": "iceberg_data",
            "schemaName": "default",
            "tableName": "products",
            "textColumns": ["name", "description"],
            "pollInterval": 30000,
            "enabled": true
        }')
    
    TABLE_ID=$(echo "$REGISTER_RESPONSE" | jq -r '.id')
    
    if [ "$TABLE_ID" != "null" ] && [ -n "$TABLE_ID" ]; then
        print_success "Table registered with ID: $TABLE_ID"
    else
        print_error "Failed to register table"
        echo "$REGISTER_RESPONSE" | jq
        exit 1
    fi
    
    # Step 8: Run Sync
    print_header "Step 8: Running Sync with Real Embeddings"
    
    print_info "Triggering sync (this may take 10-30 seconds)..."
    SYNC_RESPONSE=$(curl -s -X POST "$WORKER_URL/api/demo/sync")
    
    TABLES_SYNCED=$(echo "$SYNC_RESPONSE" | jq -r '.tablesSynced')
    VECTOR_COUNT=$(echo "$SYNC_RESPONSE" | jq -r '.vectorCount')
    
    if [ "$TABLES_SYNCED" -gt 0 ] && [ "$VECTOR_COUNT" -gt 0 ]; then
        print_success "Sync completed: $TABLES_SYNCED tables, $VECTOR_COUNT vectors"
    else
        print_error "Sync failed or no vectors generated"
        echo "$SYNC_RESPONSE" | jq
        exit 1
    fi
    
    # Step 9: Verify Vectors
    print_header "Step 9: Verifying Vector Storage"
    
    print_info "Checking vector count..."
    COUNT_RESPONSE=$(curl -s "$WORKER_URL/api/vectors/count")
    COUNT=$(echo "$COUNT_RESPONSE" | jq -r '.count')
    
    if [ "$COUNT" -eq "$VECTOR_COUNT" ]; then
        print_success "Vector count verified: $COUNT vectors stored"
    else
        print_error "Vector count mismatch: expected $VECTOR_COUNT, got $COUNT"
        exit 1
    fi
    
    # Step 10: Test Semantic Search
    print_header "Step 10: Testing Semantic Search"
    
    # Test 1: Search for shoes
    print_info "Test 1: Searching for 'comfortable running shoes'..."
    SEARCH1=$(curl -s -X POST "$SEARCH_URL/api/search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": "comfortable running shoes",
            "topK": 3,
            "sourceTable": "products"
        }')
    
    RESULTS1=$(echo "$SEARCH1" | jq -r '.totalResults')
    EXEC_TIME1=$(echo "$SEARCH1" | jq -r '.executionTimeMs')
    TOP_SIMILARITY1=$(echo "$SEARCH1" | jq -r '.results[0].similarity')
    
    if [ "$RESULTS1" -gt 0 ]; then
        print_success "Search 1: Found $RESULTS1 results in ${EXEC_TIME1}ms (top similarity: $TOP_SIMILARITY1)"
    else
        print_error "Search 1: No results found"
        exit 1
    fi
    
    # Test 2: Search for laptop
    print_info "Test 2: Searching for 'portable computer for work'..."
    SEARCH2=$(curl -s -X POST "$SEARCH_URL/api/search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": "portable computer for work",
            "topK": 3,
            "sourceTable": "products"
        }')
    
    RESULTS2=$(echo "$SEARCH2" | jq -r '.totalResults')
    EXEC_TIME2=$(echo "$SEARCH2" | jq -r '.executionTimeMs')
    TOP_SIMILARITY2=$(echo "$SEARCH2" | jq -r '.results[0].similarity')
    
    if [ "$RESULTS2" -gt 0 ]; then
        print_success "Search 2: Found $RESULTS2 results in ${EXEC_TIME2}ms (top similarity: $TOP_SIMILARITY2)"
    else
        print_error "Search 2: No results found"
        exit 1
    fi
    
    # Test 3: Search for affordable
    print_info "Test 3: Searching for 'cheap budget friendly products'..."
    SEARCH3=$(curl -s -X POST "$SEARCH_URL/api/search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": "cheap budget friendly products",
            "topK": 5,
            "sourceTable": "products"
        }')
    
    RESULTS3=$(echo "$SEARCH3" | jq -r '.totalResults')
    EXEC_TIME3=$(echo "$SEARCH3" | jq -r '.executionTimeMs')
    
    if [ "$RESULTS3" -gt 0 ]; then
        print_success "Search 3: Found $RESULTS3 results in ${EXEC_TIME3}ms"
    else
        print_error "Search 3: No results found"
        exit 1
    fi
    
    # Step 11: Verify Search Consistency
    print_header "Step 11: Verifying Search Consistency"
    
    print_info "Running same search 3 times..."
    TOP_ID1=$(curl -s -X POST "$SEARCH_URL/api/search" \
        -H "Content-Type: application/json" \
        -d '{"query":"shoes","topK":3,"sourceTable":"products"}' \
        | jq -r '.results[0].id')
    
    sleep 1
    
    TOP_ID2=$(curl -s -X POST "$SEARCH_URL/api/search" \
        -H "Content-Type: application/json" \
        -d '{"query":"shoes","topK":3,"sourceTable":"products"}' \
        | jq -r '.results[0].id')
    
    sleep 1
    
    TOP_ID3=$(curl -s -X POST "$SEARCH_URL/api/search" \
        -H "Content-Type: application/json" \
        -d '{"query":"shoes","topK":3,"sourceTable":"products"}' \
        | jq -r '.results[0].id')
    
    if [ "$TOP_ID1" = "$TOP_ID2" ] && [ "$TOP_ID2" = "$TOP_ID3" ]; then
        print_success "Search is consistent (same top result: $TOP_ID1)"
    else
        print_error "Search is inconsistent: $TOP_ID1, $TOP_ID2, $TOP_ID3"
        exit 1
    fi
    
    # Step 12: Test Dashboard
    print_header "Step 12: Testing Dashboard"
    
    print_info "Checking dashboard accessibility..."
    if curl -s -f "$DASHBOARD_URL" > /dev/null; then
        print_success "Dashboard is accessible at $DASHBOARD_URL"
    else
        print_error "Dashboard is not accessible"
        exit 1
    fi
    
    # Final Summary
    print_header "Test Summary"
    
    echo -e "${GREEN}✅ All tests passed!${NC}\n"
    echo "Services:"
    echo "  - Control Plane: $CONTROL_PLANE_URL"
    echo "  - Worker: $WORKER_URL"
    echo "  - Search Service: $SEARCH_URL"
    echo "  - Embedding Service: $EMBEDDING_URL"
    echo "  - Dashboard: $DASHBOARD_URL"
    echo ""
    echo "Results:"
    echo "  - Demo products: $RECORDS"
    echo "  - Vectors generated: $VECTOR_COUNT"
    echo "  - Embedding dimensions: 384"
    echo "  - Search tests: 3/3 passed"
    echo "  - Average search time: ~${EXEC_TIME1}ms"
    echo ""
    echo "Next steps:"
    echo "  1. Open dashboard: $DASHBOARD_URL"
    echo "  2. Try semantic search in the UI"
    echo "  3. Register more tables"
    echo "  4. Scale test with more data"
    echo ""
    print_success "VectorSync is working with real embeddings!"
}

# Run main function
main "$@"

# Made with Bob
