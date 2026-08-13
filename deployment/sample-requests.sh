#!/bin/bash

# Sample API requests for Iceberg Vector MVP
# Usage: bash deployment/sample-requests.sh

set -e

CONTROL_API="http://localhost:8080"
SEARCH_API="http://localhost:8082"

echo "=== Iceberg Vector Sample API Requests ==="
echo ""

# 1. Register a table
echo "1. Registering 'products' table..."
REGISTER_RESPONSE=$(curl -s -X POST $CONTROL_API/api/tables/register \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "default",
    "tableName": "default.products",
    "embeddingColumns": ["name", "description"],
    "modelName": "mock-embedding-v1",
    "enabled": true
  }')

echo "$REGISTER_RESPONSE" | jq .
TABLE_ID=$(echo "$REGISTER_RESPONSE" | jq -r '.tableId')
echo "Table ID: $TABLE_ID"
echo ""

# 2. Wait for worker to process
echo "2. Waiting 15 seconds for worker to initialize..."
sleep 15

# 3. List all tables
echo "3. Listing all registered tables..."
curl -s $CONTROL_API/api/tables | jq .
echo ""

# 4. Get table status
echo "4. Getting table status..."
curl -s $CONTROL_API/api/tables/$TABLE_ID/status | jq .
echo ""

# 5. Get vector count
echo "5. Checking vector count in worker..."
curl -s http://localhost:8081/api/vectors/count
echo ""
echo ""

# 6. Register another table
echo "6. Registering 'articles' table..."
REGISTER2=$(curl -s -X POST $CONTROL_API/api/tables/register \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "default",
    "tableName": "default.articles",
    "embeddingColumns": ["title", "content"],
    "modelName": "mock-embedding-v1",
    "enabled": true
  }')

echo "$REGISTER2" | jq .
TABLE_ID2=$(echo "$REGISTER2" | jq -r '.tableId')
echo ""

# 7. Perform search
echo "7. Performing semantic search..."
SEARCH_RESPONSE=$(curl -s -X POST $SEARCH_API/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "affordable shoes",
    "topK": 5,
    "sourceTable": "default.products"
  }')

echo "$SEARCH_RESPONSE" | jq .
echo ""

echo "=== Sample Request Complete ==="
echo ""
echo "API Endpoints:"
echo "- Control API:  $CONTROL_API"
echo "- Search API:   $SEARCH_API"
echo "- Worker API:   http://localhost:8081"
echo ""
echo "Sample curl commands:"
echo ""
echo "Register table:"
echo "curl -X POST http://localhost:8080/api/tables/register \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"catalog\": \"default\", \"tableName\": \"my_table\", ...}'"
echo ""
echo "Search:"
echo "curl -X POST http://localhost:8083/api/search \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"query\": \"search term\", \"topK\": 10}'"
echo ""
echo "Health check:"
echo "curl http://localhost:8080/api/tables"
echo "curl http://localhost:8081/api/vectors/health"
echo "curl http://localhost:8083/api/search/health"
