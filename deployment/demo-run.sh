#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONTROL_API_CONTAINER="vectorsync-control-api"
WORKER_CONTAINER="vectorsync-worker"
SEARCH_CONTAINER="vectorsync-search-api"

wait_for_url() {
  local container="$1"
  local url="$2"
  local label="$3"

  for _ in {1..30}; do
    if docker exec "$container" curl -s "$url" >/dev/null 2>&1; then
      echo "$label ready"
      return 0
    fi
    sleep 2
  done

  echo "Timed out waiting for $label"
  return 1
}

python_json_value() {
  python3 -c '
import json
import sys

raw = sys.stdin.read().strip()
if not raw:
    print("")
    raise SystemExit(0)

try:
    data = json.loads(raw)
except json.JSONDecodeError:
    print("")
    raise SystemExit(0)

key = sys.argv[1]
match_value = sys.argv[2] if len(sys.argv) > 2 else None

value = ""
if isinstance(data, list):
    for item in data:
        if match_value is None or item.get("tableName") == match_value:
            value = item.get(key, "")
            break
else:
    value = data.get(key, "")

print(value)
' "$@"
}

echo "=== VectorSync Demo Run ==="
echo ""
echo "Starting services with local storage and embedding service..."
echo "This will start: PostgreSQL, MinIO, Embedding Service, Control API, Worker, Search API"
echo ""

docker compose --profile local-storage --profile local-embedding up -d --build

echo ""
echo "Waiting for services to be healthy..."
wait_for_url "$CONTROL_API_CONTAINER" "http://localhost:8080/api/tables" "control-api"
wait_for_url "$WORKER_CONTAINER" "http://localhost:8081/api/vectors/health" "worker"
wait_for_url "$SEARCH_CONTAINER" "http://localhost:8083/api/search/health" "search-service"

existing_tables=$(docker exec "$CONTROL_API_CONTAINER" curl -s http://localhost:8080/api/tables)
existing_table_id=$(printf "%s" "$existing_tables" | python_json_value tableId "default.products")

if [[ -z "$existing_table_id" ]]; then
  echo "Registering demo table in control-api"
  register_response=$(docker exec "$CONTROL_API_CONTAINER" curl -s -w "\n%{http_code}" -X POST http://localhost:8080/api/tables/register \
    -H "Content-Type: application/json" \
    -d '{"catalog":"default","tableName":"default.products","embeddingColumns":["name","description"],"modelName":"mock-embedding-v1","enabled":true}')
  
  http_code=$(echo "$register_response" | tail -n1)
  response_body=$(echo "$register_response" | sed '$d')
  
  if [[ "$http_code" == "201" ]]; then
    echo "Table registered successfully"
    existing_table_id=$(printf "%s" "$response_body" | python_json_value tableId)
  elif [[ "$http_code" == "409" ]]; then
    echo "Table already exists, fetching existing table ID"
    existing_tables=$(docker exec "$CONTROL_API_CONTAINER" curl -s http://localhost:8080/api/tables)
    existing_table_id=$(printf "%s" "$existing_tables" | python_json_value tableId "default.products")
  else
    echo "Registration failed with HTTP $http_code: $response_body"
    existing_tables=$(docker exec "$CONTROL_API_CONTAINER" curl -s http://localhost:8080/api/tables)
    existing_table_id=$(printf "%s" "$existing_tables" | python_json_value tableId "default.products")
  fi
fi

if [[ -z "$existing_table_id" ]]; then
  echo "Failed to resolve demo table ID"
  exit 1
fi

echo "Table ID: $existing_table_id"

echo "Seeding mock Iceberg source table"
docker exec "$WORKER_CONTAINER" curl -s -X POST http://localhost:8081/api/demo/seed

echo "Triggering sync"
docker exec "$WORKER_CONTAINER" curl -s -X POST http://localhost:8081/api/demo/sync

vector_count=$(docker exec "$WORKER_CONTAINER" curl -s http://localhost:8081/api/vectors/count)
echo "Vector count: $vector_count"

echo "Running search"
search_response=$(docker exec "$SEARCH_CONTAINER" curl -s -X POST http://localhost:8083/api/search \
  -H "Content-Type: application/json" \
  -d '{"query":"affordable shoes","topK":5,"sourceTable":"default.products"}')

echo "$search_response"
echo "=== Demo Complete ==="
