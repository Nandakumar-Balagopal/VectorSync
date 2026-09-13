#!/usr/bin/env bash
#
# End-to-end proof of the embedding lifecycle thesis against the full stack with real embeddings:
#
#   materialize v1 -> index v1 -> promote -> serve
#                  -> materialize v2 alongside v1
#                  -> index v2 -> evaluate v1 vs v2 -> promote v2 -> serve
#                  -> roll back to v1 -> serve
#                  -> trace a result back to the source row at its snapshot
#
# The point is that both versions coexist, promotion and rollback are single commits, and every
# artifact carries the lineage needed to reproduce it.
#
set -euo pipefail

CONTROL_URL="${CONTROL_URL:-http://localhost:18080}"
WORKER_URL="${WORKER_URL:-http://localhost:18081}"
SEARCH_URL="${SEARCH_URL:-http://localhost:18083}"
EMBEDDING_URL="${EMBEDDING_URL:-http://localhost:18000}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-vectorsync-e2e}"
SKIP_STACK_START="${SKIP_STACK_START:-false}"
SOURCE_TABLE="default.products"

export POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-15433}"
export MINIO_API_HOST_PORT="${MINIO_API_HOST_PORT:-19000}"
export MINIO_CONSOLE_HOST_PORT="${MINIO_CONSOLE_HOST_PORT:-19001}"
export EMBEDDING_HOST_PORT="${EMBEDDING_HOST_PORT:-18000}"
export CONTROL_PLANE_HOST_PORT="${CONTROL_PLANE_HOST_PORT:-18080}"
export WORKER_HOST_PORT="${WORKER_HOST_PORT:-18081}"
export SEARCH_SERVICE_HOST_PORT="${SEARCH_SERVICE_HOST_PORT:-18083}"
export DASHBOARD_HOST_PORT="${DASHBOARD_HOST_PORT:-13000}"

COMPOSE_FILES=(-f docker-compose.yml -f deployment/docker-compose.e2e.yml)
COMPOSE_PROFILES=(--profile local-storage --profile local-embedding)

compose() {
  COMPOSE_PROJECT_NAME="$COMPOSE_PROJECT_NAME" docker compose "${COMPOSE_FILES[@]}" "${COMPOSE_PROFILES[@]}" "$@"
}

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

json() { python3 -m json.tool; }

jget() {
  # jget <json> <python-expression-over-'d'>
  python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print($2)" <<<"$1"
}

wait_for() {
  local name="$1" url="$2"
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

fail() {
  echo "Assertion failed: $1" >&2
  [[ $# -gt 1 ]] && { echo "$2" | json 2>/dev/null || echo "$2"; }
  exit 1
}

assert_eq() {
  [[ "$1" == "$2" ]] || fail "$3 (expected '$2', got '$1')"
}

assert_contains() {
  [[ "$1" == *"$2"* ]] || fail "$3" "$1"
}

assert_not_contains() {
  [[ "$1" != *"$2"* ]] || fail "$3" "$1"
}

need curl
need python3

if [[ "$SKIP_STACK_START" != "true" ]]; then
  echo "Resetting local E2E stack state..."
  compose down -v --remove-orphans
  echo "Starting full local stack..."
  compose up -d --build
fi

wait_for "control-plane" "${CONTROL_URL}/actuator/health"
wait_for "worker"        "${WORKER_URL}/actuator/health"
wait_for "search-service" "${SEARCH_URL}/actuator/health"
wait_for "embedding-service" "${EMBEDDING_URL}/api/v1/health"

echo
echo "=== 1. Materialize embedding version v1 ==="
curl -fsS --max-time 30 -X POST "${WORKER_URL}/api/demo/seed" >/dev/null

registration="$(curl -fsS --max-time 30 -X POST "${CONTROL_URL}/api/tables/register" \
  -H 'Content-Type: application/json' \
  -d "{\"catalog\":\"default\",\"tableName\":\"${SOURCE_TABLE}\",\"embeddingColumns\":[\"name\",\"description\"],\"modelName\":\"all-MiniLM-L6-v2\",\"embeddingVersion\":\"v1\",\"enabled\":true}")"
TABLE_ID="$(jget "$registration" "d['tableId']")"
echo "registered tableId=${TABLE_ID}"

sync="$(curl -fsS --max-time 180 -X POST "${WORKER_URL}/api/demo/sync")"
assert_eq "$(jget "$sync" "d['vectorCount']")" "4" "initial sync should produce four live vectors"

versions="$(curl -fsS --max-time 30 "${SEARCH_URL}/api/lifecycle/model-versions?sourceTable=${SOURCE_TABLE}")"
assert_contains "$versions" "all-MiniLM-L6-v2:v1" "v1 should be materialized"
SNAPSHOT_ID="$(jget "$versions" "d['latestSourceSnapshotId']")"
echo "source snapshot=${SNAPSHOT_ID}"

echo
echo "=== 2. Build and promote an index over v1 ==="
build_v1="$(curl -fsS --max-time 180 -X POST "${SEARCH_URL}/api/lifecycle/index/build" \
  -H 'Content-Type: application/json' \
  -d "{\"sourceTable\":\"${SOURCE_TABLE}\",\"modelVersion\":\"all-MiniLM-L6-v2:v1\"}")"
INDEX_V1="$(jget "$build_v1" "d['indexId']")"
assert_eq "$(jget "$build_v1" "d['status']")" "READY" "index v1 should build successfully"
assert_eq "$(jget "$build_v1" "d['vectorCount']")" "4" "index v1 should cover four vectors"
echo "built index v1=${INDEX_V1}"

curl -fsS --max-time 30 -X POST "${SEARCH_URL}/api/lifecycle/promote" \
  -H 'Content-Type: application/json' \
  -d "{\"sourceTable\":\"${SOURCE_TABLE}\",\"indexId\":\"${INDEX_V1}\",\"promotedBy\":\"e2e\",\"note\":\"initial v1\"}" >/dev/null

promoted="$(curl -fsS --max-time 30 "${SEARCH_URL}/api/lifecycle/promoted?sourceTable=${SOURCE_TABLE}")"
assert_eq "$(jget "$promoted" "d['indexId']")" "${INDEX_V1}" "v1 should be promoted"

search_v1="$(curl -fsS --max-time 60 -X POST "${SEARCH_URL}/api/search" \
  -H 'Content-Type: application/json' \
  -d "{\"query\":\"lightweight running shoe\",\"topK\":4,\"sourceTable\":\"${SOURCE_TABLE}\"}")"
assert_contains "$search_v1" '"sourceRowId":"p-100"' "search on v1 should find the trail runner"

echo
echo "=== 3. Materialize v2 alongside v1 ==="
curl -fsS --max-time 30 -X PUT "${CONTROL_URL}/api/tables/${TABLE_ID}" \
  -H 'Content-Type: application/json' \
  -d '{"embeddingVersion":"v2"}' >/dev/null

sync="$(curl -fsS --max-time 180 -X POST "${WORKER_URL}/api/demo/sync")"
assert_eq "$(jget "$sync" "d['vectorCount']")" "8" "v1 and v2 should coexist as eight live vectors"

versions="$(curl -fsS --max-time 30 "${SEARCH_URL}/api/lifecycle/model-versions?sourceTable=${SOURCE_TABLE}")"
assert_contains "$versions" "all-MiniLM-L6-v2:v1" "v1 must be retained"
assert_contains "$versions" "all-MiniLM-L6-v2:v2" "v2 must be present"

echo
echo "=== 4. Build v2 and evaluate both versions ==="
build_v2="$(curl -fsS --max-time 180 -X POST "${SEARCH_URL}/api/lifecycle/index/build" \
  -H 'Content-Type: application/json' \
  -d "{\"sourceTable\":\"${SOURCE_TABLE}\",\"modelVersion\":\"all-MiniLM-L6-v2:v2\"}")"
INDEX_V2="$(jget "$build_v2" "d['indexId']")"
assert_eq "$(jget "$build_v2" "d['status']")" "READY" "index v2 should build successfully"
[[ "$INDEX_V1" != "$INDEX_V2" ]] || fail "v1 and v2 must produce distinct index ids"
echo "built index v2=${INDEX_V2}"

eval_payload='{"topK":4,"queries":[
  {"query":"lightweight running shoe","relevantSourceRowIds":["p-100"]},
  {"query":"waterproof hiking boot","relevantSourceRowIds":["p-300"]},
  {"query":"affordable canvas shoe","relevantSourceRowIds":["p-400"]}]}'

for idx in "$INDEX_V1" "$INDEX_V2"; do
  report="$(curl -fsS --max-time 120 -X POST "${SEARCH_URL}/api/lifecycle/evaluate" \
    -H 'Content-Type: application/json' \
    -d "$(python3 -c "
import json,sys
p = json.loads(sys.argv[1]); p['indexId'] = sys.argv[2]; print(json.dumps(p))
" "$eval_payload" "$idx")")"
  echo "  ${idx:0:12} indexRecall@4=$(jget "$report" "d['indexRecallAtK']") precision@4=$(jget "$report" "d['precisionAtK']")"
done

# metrics must be durable on the manifest, not just in the response
indexes="$(curl -fsS --max-time 30 "${SEARCH_URL}/api/lifecycle/indexes?sourceTable=${SOURCE_TABLE}")"
assert_contains "$indexes" "index_recall@4" "evaluation metrics must be recorded on the manifest"

echo
echo "=== 5. Promote v2 ==="
curl -fsS --max-time 30 -X POST "${SEARCH_URL}/api/lifecycle/promote" \
  -H 'Content-Type: application/json' \
  -d "{\"sourceTable\":\"${SOURCE_TABLE}\",\"indexId\":\"${INDEX_V2}\",\"promotedBy\":\"e2e\",\"note\":\"promote v2\"}" >/dev/null

promoted="$(curl -fsS --max-time 30 "${SEARCH_URL}/api/lifecycle/promoted?sourceTable=${SOURCE_TABLE}")"
assert_eq "$(jget "$promoted" "d['indexId']")" "${INDEX_V2}" "v2 should now be promoted"
assert_eq "$(jget "$promoted" "d['embeddingVersion']")" "v2" "promoted index should be the v2 artifact"

search_v2="$(curl -fsS --max-time 60 -X POST "${SEARCH_URL}/api/search" \
  -H 'Content-Type: application/json' \
  -d "{\"query\":\"lightweight running shoe\",\"topK\":4,\"sourceTable\":\"${SOURCE_TABLE}\"}")"
assert_contains "$search_v2" '"sourceRowId":"p-100"' "search on v2 should still find the trail runner"
VECTOR_ID="$(jget "$search_v2" "d['results'][0]['vectorId']")"

echo
echo "=== 6. Roll back to v1 ==="
curl -fsS --max-time 30 -X POST "${SEARCH_URL}/api/lifecycle/rollback?sourceTable=${SOURCE_TABLE}&rolledBackBy=e2e" >/dev/null

promoted="$(curl -fsS --max-time 30 "${SEARCH_URL}/api/lifecycle/promoted?sourceTable=${SOURCE_TABLE}")"
assert_eq "$(jget "$promoted" "d['indexId']")" "${INDEX_V1}" "rollback should restore v1"

history="$(curl -fsS --max-time 30 "${SEARCH_URL}/api/lifecycle/history?sourceTable=${SOURCE_TABLE}")"
assert_eq "$(jget "$history" "len(d)")" "3" "promote, promote, rollback must all be retained"
assert_contains "$history" "rollback" "the rollback must be recorded with its note"

echo
echo "=== 7. Trace provenance back to the source row ==="
# re-promote v2 so the traced vector's serving index matches the version that produced it
curl -fsS --max-time 30 -X POST "${SEARCH_URL}/api/lifecycle/promote" \
  -H 'Content-Type: application/json' \
  -d "{\"sourceTable\":\"${SOURCE_TABLE}\",\"indexId\":\"${INDEX_V2}\",\"promotedBy\":\"e2e\",\"note\":\"re-promote for provenance\"}" >/dev/null

chain="$(curl -fsS --max-time 60 "${SEARCH_URL}/api/provenance/vector/${VECTOR_ID}")"
assert_eq "$(jget "$chain" "d['embedding']['version']")" "v2" "provenance should name the embedding version"
assert_eq "$(jget "$chain" "d['source']['table']")" "${SOURCE_TABLE}" "provenance should name the source table"
assert_eq "$(jget "$chain" "d['index']['indexId']")" "${INDEX_V2}" "provenance should name the serving index"
assert_contains "$chain" '"sourceRowAtSnapshot"' "provenance should include the source row"
assert_not_contains "$(jget "$chain" "d['sourceRowAtSnapshot']")" "not present at snapshot" \
  "the source row must be readable at the snapshot it was embedded from"

row_history="$(curl -fsS --max-time 30 "${SEARCH_URL}/api/provenance/row?sourceTable=${SOURCE_TABLE}&sourceRowId=p-100")"
assert_eq "$(jget "$row_history" "len(d)")" "2" "p-100 should have one stored embedding per version"

echo
echo "=== 8. Deletes remain excluded under EVERY version ==="
curl -fsS --max-time 30 -X DELETE "${WORKER_URL}/api/demo/products/p-100" >/dev/null
sync="$(curl -fsS --max-time 180 -X POST "${WORKER_URL}/api/demo/sync")"
# 4 rows x 2 versions = 8, minus both versions of the deleted row = 6. Tombstoning only the
# currently configured version would leave 7 and keep the row discoverable under the other.
assert_eq "$(jget "$sync" "d['vectorCount']")" "6" \
  "a delete must tombstone the row under every materialized version"

for version in v1 v2; do
  after_delete="$(curl -fsS --max-time 60 -X POST "${SEARCH_URL}/api/search/exact" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"lightweight running shoe\",\"topK\":8,\"sourceTable\":\"${SOURCE_TABLE}\",\"modelVersion\":\"all-MiniLM-L6-v2:${version}\"}")"
  assert_not_contains "$after_delete" '"sourceRowId":"p-100"' \
    "a deleted row must not appear under ${version}"
  # scoping to one version means each row appears once, not once per version
  assert_eq "$(jget "$after_delete" "d['totalResults']")" "3" \
    "exact search scoped to ${version} should return the three surviving rows once each"
done

echo
echo "✓ End-to-end lifecycle test passed"
echo "  index v1: ${INDEX_V1}"
echo "  index v2: ${INDEX_V2}"
