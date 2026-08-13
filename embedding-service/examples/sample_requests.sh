#!/bin/bash

# Sample requests for the VectorSync Embedding Service
# Make sure the service is running on http://localhost:8000

BASE_URL="http://localhost:8000/api/v1"

echo "=== VectorSync Embedding Service - Sample Requests ==="
echo ""

# 1. Health Check
echo "1. Health Check"
echo "GET $BASE_URL/health"
curl -s -X GET "$BASE_URL/health" | jq
echo ""
echo "---"
echo ""

# 2. Single Query Embedding (Low Latency)
echo "2. Query Embedding (Low Latency)"
echo "POST $BASE_URL/embed-query"
curl -s -X POST "$BASE_URL/embed-query" \
  -H "Content-Type: application/json" \
  -d '{
    "text": "affordable running shoes",
    "model_name": "all-MiniLM-L6-v2",
    "provider": "self_hosted"
  }' | jq '{
    model_name,
    dimension,
    processing_time_ms,
    embedding_sample: .embedding[:5]
  }'
echo ""
echo "---"
echo ""

# 3. Batch Embedding (High Throughput)
echo "3. Batch Embedding (High Throughput)"
echo "POST $BASE_URL/embed"
curl -s -X POST "$BASE_URL/embed" \
  -H "Content-Type: application/json" \
  -d '{
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
      },
      {
        "vector_id": "vec-002",
        "source_table": "products",
        "source_row_id": "prod-456",
        "text": "Comfortable walking shoes for everyday use",
        "metadata": {
          "snapshot_id": "12345",
          "column": "description"
        },
        "model_name": "all-MiniLM-L6-v2",
        "provider": "self_hosted"
      },
      {
        "vector_id": "vec-003",
        "source_table": "products",
        "source_row_id": "prod-789",
        "text": "Professional basketball shoes with ankle support",
        "metadata": {
          "snapshot_id": "12345",
          "column": "description"
        },
        "model_name": "all-MiniLM-L6-v2",
        "provider": "self_hosted"
      }
    ]
  }' | jq '{
    request_id,
    total_records,
    successful,
    failed,
    processing_time_ms,
    results: [.results[] | {
      vector_id,
      embedding_sample: .embedding[:5],
      error
    }]
  }'
echo ""
echo "---"
echo ""

# 4. Large Batch (Testing Batching)
echo "4. Large Batch (Testing Batching)"
echo "POST $BASE_URL/embed (10 records)"

# Generate 10 records
RECORDS=$(cat <<EOF
{
  "request_id": "batch-test-001",
  "records": [
    $(for i in {1..10}; do
      echo "{
        \"vector_id\": \"vec-$(printf '%03d' $i)\",
        \"source_table\": \"products\",
        \"source_row_id\": \"prod-$(printf '%03d' $i)\",
        \"text\": \"Product description number $i with various features\",
        \"model_name\": \"all-MiniLM-L6-v2\",
        \"provider\": \"self_hosted\"
      }"
      if [ $i -lt 10 ]; then echo ","; fi
    done)
  ]
}
EOF
)

curl -s -X POST "$BASE_URL/embed" \
  -H "Content-Type: application/json" \
  -d "$RECORDS" | jq '{
    request_id,
    total_records,
    successful,
    failed,
    processing_time_ms,
    avg_time_per_record: (.processing_time_ms / .total_records)
  }'
echo ""
echo "---"
echo ""

# 5. Error Handling (Empty Text)
echo "5. Error Handling (Empty Text)"
echo "POST $BASE_URL/embed-query (should fail)"
curl -s -X POST "$BASE_URL/embed-query" \
  -H "Content-Type: application/json" \
  -d '{
    "text": "",
    "model_name": "all-MiniLM-L6-v2",
    "provider": "self_hosted"
  }' | jq
echo ""
echo "---"
echo ""

# 6. Idempotency Test (Same Request Twice)
echo "6. Idempotency Test (Same Request Twice)"
echo "POST $BASE_URL/embed (twice with same vector_id)"

REQUEST_DATA='{
  "request_id": "idempotency-test-001",
  "records": [
    {
      "vector_id": "vec-idempotent-001",
      "source_table": "test",
      "source_row_id": "test-001",
      "text": "This is a test for idempotency",
      "model_name": "all-MiniLM-L6-v2",
      "provider": "self_hosted"
    }
  ]
}'

echo "First request:"
RESULT1=$(curl -s -X POST "$BASE_URL/embed" \
  -H "Content-Type: application/json" \
  -d "$REQUEST_DATA")
echo "$RESULT1" | jq '{vector_id: .results[0].vector_id, embedding_sample: .results[0].embedding[:3]}'

echo ""
echo "Second request (should produce same embedding):"
RESULT2=$(curl -s -X POST "$BASE_URL/embed" \
  -H "Content-Type: application/json" \
  -d "$REQUEST_DATA")
echo "$RESULT2" | jq '{vector_id: .results[0].vector_id, embedding_sample: .results[0].embedding[:3]}'

echo ""
echo "Embeddings match: $([ "$(echo $RESULT1 | jq -r '.results[0].embedding[0]')" == "$(echo $RESULT2 | jq -r '.results[0].embedding[0]')" ] && echo 'YES' || echo 'NO')"
echo ""
echo "---"
echo ""

echo "=== All sample requests completed ==="
