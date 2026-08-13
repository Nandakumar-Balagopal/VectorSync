#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env.host"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

need curl
need docker
need mvn

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${ROOT_DIR}/.env.host.example" "${ENV_FILE}"
  echo "Created ${ENV_FILE} from .env.host.example"
fi

set -a
source "${ENV_FILE}"
set +a

cd "${ROOT_DIR}"

echo "Starting local infrastructure in Docker..."
docker compose --env-file .env.host --profile local-storage --profile local-embedding up -d postgres minio embedding-service

echo "Waiting for infrastructure health..."
docker compose --env-file .env.host ps

wait_for_http() {
  local name="$1"
  local url="$2"
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 5 "${url}" >/dev/null 2>&1; then
      echo "✓ ${name} is healthy"
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for ${name}: ${url}" >&2
  exit 1
}

cleanup() {
  if [[ -n "${CONTROL_PID:-}" ]]; then kill "${CONTROL_PID}" 2>/dev/null || true; fi
  if [[ -n "${WORKER_PID:-}" ]]; then kill "${WORKER_PID}" 2>/dev/null || true; fi
  if [[ -n "${SEARCH_PID:-}" ]]; then kill "${SEARCH_PID}" 2>/dev/null || true; fi
}
trap cleanup EXIT INT TERM

echo "Building Java modules once with the host Maven cache..."
mvn -q -DskipTests install

echo "Starting control-plane on localhost:8080..."
mvn -q -pl control-plane spring-boot:run &
CONTROL_PID=$!
wait_for_http "control-plane" "http://localhost:8080/actuator/health"

echo "Starting worker on localhost:8081..."
mvn -q -pl worker spring-boot:run &
WORKER_PID=$!
wait_for_http "worker" "http://localhost:8081/api/vectors/health"

echo "Starting search-service on localhost:8083..."
mvn -q -pl search-service spring-boot:run &
SEARCH_PID=$!
wait_for_http "search-service" "http://localhost:8083/api/search/health"

echo
echo "VectorSync local dev is ready."
echo "Health checks:"
echo "  control-plane:  curl http://localhost:8080/actuator/health"
echo "  worker:         curl http://localhost:8081/actuator/health"
echo "  search-service: curl http://localhost:8083/actuator/health"
echo "  embeddings:     curl http://localhost:8000/api/v1/health"
echo
echo "Press Ctrl-C to stop Java services. Docker infra remains running."

wait
