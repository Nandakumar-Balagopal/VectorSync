#!/usr/bin/env bash
set -euo pipefail

CONTROL_URL="${CONTROL_URL:-http://localhost:18080}"
WORKER_URL="${WORKER_URL:-http://localhost:18081}"
EMBEDDING_URL="${EMBEDDING_URL:-http://localhost:18000}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-vectorsync-e2e}"
SKIP_STACK_START="${SKIP_STACK_START:-false}"
export POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-15433}"
export MINIO_API_HOST_PORT="${MINIO_API_HOST_PORT:-19000}"
export MINIO_CONSOLE_HOST_PORT="${MINIO_CONSOLE_HOST_PORT:-19001}"
export EMBEDDING_HOST_PORT="${EMBEDDING_HOST_PORT:-18000}"
export CONTROL_PLANE_HOST_PORT="${CONTROL_PLANE_HOST_PORT:-18080}"
export WORKER_HOST_PORT="${WORKER_HOST_PORT:-18081}"
export DASHBOARD_HOST_PORT="${DASHBOARD_HOST_PORT:-13000}"
COMPOSE_FILES=(-f docker-compose.yml -f deployment/docker-compose.e2e.yml)
COMPOSE_PROFILES=(--profile local-storage --profile local-embedding)

compose() {
  COMPOSE_PROJECT_NAME="$COMPOSE_PROJECT_NAME" docker compose "${COMPOSE_FILES[@]}" "${COMPOSE_PROFILES[@]}" "$@"
}

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

fail() { echo "ASSERTION FAILED: $1" >&2; exit 1; }
assert_eq() { [ "$1" = "$2" ] || fail "$3 (expected '$2', got '$1')"; }
assert_contains() { case "$1" in *"$2"*) ;; *) fail "$3" ;; esac; }
jget() { python3 -c "import sys,json; d=json.load(sys.stdin); print($2)" <<<"$1"; }

# ---------------------------------------------------------------------------
# End-to-end over HTTP against the current three-tier pipeline.
#
# The script this replaces drove /api/search on a service that no longer exists.
# Losing it left the project with unit and integration coverage but nothing
# asserting across running processes -- which is exactly where several defects
# hid: a watermark that never advanced, request parameters that could not bind,
# metrics that were never recorded, Tier 3 serving superseded content. Every one
# of them passed its own tests.
# ---------------------------------------------------------------------------

TABLE="default.e2e_products"
MODEL="all-MiniLM-L6-v2"
SPEC='{"sourceTable":"'"$TABLE"'","keyColumns":["id"],"embeddingColumns":["name","description"],"joinSeparator":" ","chunker":"whole","modelName":"'"$MODEL"'","modelRevision":"e2e","embeddingVersion":"v1"}'
SPEC_PUBLISH='{"sourceTable":"'"$TABLE"'","keyColumns":["id"],"embeddingColumns":["name","description"],"joinSeparator":" ","chunker":"whole","modelName":"'"$MODEL"'","modelRevision":"e2e","embeddingVersion":"v1","publishProjection":true}'
QUERY="Widget 7 in cat-1, a distinctive product description"

need curl
need python3

if [ "$SKIP_STACK_START" != "true" ]; then
  echo "== bringing up the stack =="
  compose up -d --build
fi

wait_for "control-plane" "${CONTROL_URL}/actuator/health"
wait_for "worker" "${WORKER_URL}/actuator/health"

echo "== 1. seed a partitioned source =="
# Partitioned deliberately: a reconcile can only be scoped to the partitions a
# change touched, and an unpartitioned source can only be reconciled whole-table.
seed_rows=$(python3 -c '
import json
rows=[{"id":"e2e-%03d"%i,"name":"Widget %d"%i,
       "description":"Widget %d in cat-%d, a distinctive product description"%(i,i%3),
       "category":"cat-%d"%(i%3),"price":1.0+i} for i in range(30)]
print(json.dumps({"tableName":"default.e2e_products","rows":rows,"partitionColumn":"category"}))')
seeded=$(curl -fsS --max-time 300 -X POST "${WORKER_URL}/api/demo/tables" -H 'Content-Type: application/json' -d "$seed_rows")
assert_eq "$(jget "$seeded" "d['recordsWritten']")" "30" "the seed should write thirty rows"

echo "== 2. admit a materialization =="
admitted=$(curl -sS --max-time 300 -X POST "${CONTROL_URL}/api/materializations" -H 'Content-Type: application/json' -d "$SPEC")
assert_eq "$(jget "$admitted" "str(d['admitted'])")" "True" "admission refused: $admitted"
MID=$(jget "$admitted" "d['materialization']['id']")
CFG=$(jget "$admitted" "d['configId']")
echo "   materialization $MID, config $CFG"

echo "== 3. the scheduler brings it LIVE =="
for _ in $(seq 1 60); do
  state=$(curl -fsS --max-time 10 "${CONTROL_URL}/api/materializations/${MID}")
  s=$(jget "$state" "d['state']"); w=$(jget "$state" "d['incrementalWatermark']")
  [ "$s" = "LIVE" ] && [ "$w" != "0" ] && break
  sleep 5
done
assert_eq "$s" "LIVE" "never reached LIVE"

echo "== 4. retrieval returns the right row =="
search_body='{"sourceTable":"'"$TABLE"'","configId":"'"$CFG"'","query":"'"$QUERY"'","modelName":"'"$MODEL"'","k":3}'
hits=$(curl -fsS --max-time 120 -X POST "${WORKER_URL}/api/derive/search" -H 'Content-Type: application/json' -d "$search_body")
assert_eq "$(jget "$hits" "d['hits'][0]['sourceRowId']")" "e2e-007" "the wrong row ranked first"

echo "== 5. provenance explains it =="
chain=$(curl -fsS --max-time 60 "${WORKER_URL}/api/derive/provenance?sourceTable=${TABLE}&configId=${CFG}&sourceRowId=e2e-007&chunkOrdinal=0")
assert_eq "$(jget "$chain" "str(d['served'])")" "True" "provenance says it is not served"
[ "$(jget "$chain" "len(d['history'])")" -ge 1 ] || fail "provenance returned no history"

echo "== 6. the promotion gate accepts it =="
promoted=$(curl -sS --max-time 60 -X POST "${CONTROL_URL}/api/materializations/${MID}/promote")
assert_eq "$(jget "$promoted" "d['promoted']")" "$MID" "promotion refused: $promoted"

echo "== 7. metrics are scrapeable =="
meters=$(curl -fsS --max-time 30 "${WORKER_URL}/actuator/metrics")
assert_contains "$meters" "vectorsync.inference.calls" "inference meters are not exposed"

echo "== 8. a re-derive of unchanged content costs no inference =="
again=$(curl -fsS --max-time 900 -X POST "${WORKER_URL}/api/derive/run" -H 'Content-Type: application/json' -d "$SPEC_PUBLISH")
assert_eq "$(jget "$again" "d['metrics']['inferenceCalls']")" "0" "unchanged content was re-embedded"

echo "== 9. deleting a partition is reconciled out of serving =="
curl -fsS --max-time 120 -X POST "${WORKER_URL}/api/demo/tables/delete-partition" -H 'Content-Type: application/json' \
  -d '{"tableName":"'"$TABLE"'","column":"category","value":"cat-1"}' >/dev/null
# cat-1 holds e2e-007, so a query for its own text must stop returning it.
top="e2e-007"
for _ in $(seq 1 60); do
  hits=$(curl -fsS --max-time 120 -X POST "${WORKER_URL}/api/derive/search" -H 'Content-Type: application/json' -d "$search_body" || echo '{"hits":[]}')
  top=$(jget "$hits" "d['hits'][0]['sourceRowId'] if d['hits'] else 'none'")
  [ "$top" != "e2e-007" ] && break
  sleep 5
done
[ "$top" != "e2e-007" ] || fail "the deleted row is still served after the reconcile"

echo
echo "ALL E2E ASSERTIONS PASSED"
