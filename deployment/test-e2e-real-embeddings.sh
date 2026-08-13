#!/usr/bin/env bash
set -euo pipefail

CONTROL_URL="${CONTROL_URL:-http://localhost:8080}"
WORKER_URL="${WORKER_URL:-http://localhost:8081}"
SEARCH_URL="${SEARCH_URL:-http://localhost:8083}"
EMBEDDING_URL="${EMBEDDING_URL:-http://localhost:8000}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

json() {
  python3 -m json.tool
}

wait_for() {
  local name="$1"
  local url="$2"
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 5 "$url" >/dev/null 2>&1; then
      echo "✓ ${name} is healthy"
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for ${name}: ${url}" >&2
  exit 1
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local message="$3"
  if [[ "$haystack" != *"$needle"* ]]; then
    echo "Assertion failed: ${message}" >&2
    echo "$haystack" | json || echo "$haystack"
    exit 1
  fi
}

assert_not_contains() {
  local haystack="$1"
  local needle="$2"
  local message="$3"
  if [[ "$haystack" == *"$needle"* ]]; then
    echo "Assertion failed: ${message}" >&2
    echo "$haystack" | json || echo "$haystack"
    exit 1
  fi
}

need curl
need python3

echo "Starting full local stack..."
docker compose --profile local-storage --profile local-embedding up -d --build

wait_for "control-plane" "${CONTROL_URL}/actuator/health"
wait_for "worker" "${WORKER_URL}/actuator/health"
wait_for "search-service" "${SEARCH_URL}/actuator/health"
wait_for "embedding-service" "${EMBEDDING_URL}/api/v1/health"

echo "Seeding clean demo table and vector table..."
seed_response="$(curl -fsS --max-time 30 -X POST "${WORKER_URL}/api/demo/seed")"
assert_contains "$seed_response" '"recordsWritten":4' "seed should create four source rows"

echo "Running initial sync..."
sync_response="$(curl -fsS --max-time 120 -X POST "${WORKER_URL}/api/demo/sync")"
assert_contains "$sync_response" '"vectorCount":4' "initial sync should create four live vectors"

baseline_count="$(curl -fsS --max-time 20 "${WORKER_URL}/api/vectors/count")"
[[ "$baseline_count" == "4" ]] || {
  echo "Expected baseline vector count 4, got ${baseline_count}" >&2
  exit 1
}

baseline_search="$(curl -fsS --max-time 30 -X POST "${SEARCH_URL}/api/search" \
  -H 'Content-Type: application/json' \
  -d '{"query":"lightweight running shoe","topK":8,"sourceTable":"products"}')"
assert_contains "$baseline_search" '"sourceRowId":"p-100"' "baseline search should find p-100"

echo "Testing append..."
curl -fsS --max-time 30 -X POST "${WORKER_URL}/api/demo/products" \
  -H 'Content-Type: application/json' \
  -d '{"id":"p-e2e-001","name":"Court Trainer","description":"Supportive indoor court shoe with pivot grip","category":"shoes","price":66.0}' >/dev/null
sync_response="$(curl -fsS --max-time 120 -X POST "${WORKER_URL}/api/demo/sync")"
assert_contains "$sync_response" '"vectorCount":5' "append sync should create five live vectors"
append_search="$(curl -fsS --max-time 30 -X POST "${SEARCH_URL}/api/search" \
  -H 'Content-Type: application/json' \
  -d '{"query":"indoor court pivot grip","topK":8,"sourceTable":"products"}')"
assert_contains "$append_search" '"sourceRowId":"p-e2e-001"' "append search should find new row"
assert_contains "$append_search" 'Court Trainer' "append search should return appended text"

echo "Testing update..."
curl -fsS --max-time 30 -X POST "${WORKER_URL}/api/demo/products/p-e2e-001" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Kitchen Blender","description":"Countertop blender for smoothies and soup","category":"appliances","price":59.0}' >/dev/null
sync_response="$(curl -fsS --max-time 120 -X POST "${WORKER_URL}/api/demo/sync")"
assert_contains "$sync_response" '"vectorCount":5' "update sync should keep five live vectors"
update_search="$(curl -fsS --max-time 30 -X POST "${SEARCH_URL}/api/search" \
  -H 'Content-Type: application/json' \
  -d '{"query":"smoothies soup blender","topK":8,"sourceTable":"products"}')"
assert_contains "$update_search" '"sourceRowId":"p-e2e-001"' "update search should find updated row"
assert_contains "$update_search" 'Kitchen Blender' "update search should return updated text"

echo "Testing delete..."
curl -fsS --max-time 30 -X DELETE "${WORKER_URL}/api/demo/products/p-e2e-001" >/dev/null
sync_response="$(curl -fsS --max-time 120 -X POST "${WORKER_URL}/api/demo/sync")"
assert_contains "$sync_response" '"vectorCount":4' "delete sync should return to four live vectors"
delete_search="$(curl -fsS --max-time 30 -X POST "${SEARCH_URL}/api/search" \
  -H 'Content-Type: application/json' \
  -d '{"query":"smoothies soup blender","topK":8,"sourceTable":"products"}')"
assert_not_contains "$delete_search" '"sourceRowId":"p-e2e-001"' "delete search should not return deleted row"
assert_not_contains "$delete_search" 'Kitchen Blender' "delete search should not return deleted text"

echo
echo "✓ End-to-end real embedding test passed"
