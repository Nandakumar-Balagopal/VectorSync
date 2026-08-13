#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env.host"
START_INFRA=true

usage() {
  cat <<'USAGE'
Usage: scripts/start-local.sh [--no-infra]

Starts the Java services on the host with Maven so they can reuse the local ~/.m2 cache.

Options:
  --no-infra   Do not start Docker infrastructure. Use this when Postgres,
               MinIO, and the embedding service are already running locally.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-infra)
      START_INFRA=false
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

need curl
need mvn
if [[ "${START_INFRA}" == "true" ]]; then
  need docker
fi

ensure_port_free() {
  local port="$1"
  if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port ${port} is already in use. Stop the existing process or change the service port before starting VectorSync locally." >&2
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN >&2 || true
    exit 1
  fi
}

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${ROOT_DIR}/.env.host.example" "${ENV_FILE}"
  echo "Created ${ENV_FILE} from .env.host.example"
fi

set -a
source "${ENV_FILE}"
set +a

cd "${ROOT_DIR}"

ensure_port_free 8080
ensure_port_free 8081
ensure_port_free 8083

if [[ "${START_INFRA}" == "true" ]]; then
  echo "Starting local infrastructure in Docker..."
  docker compose --env-file .env.host --profile local-storage --profile local-embedding up -d postgres minio embedding-service

  echo "Waiting for infrastructure health..."
  docker compose --env-file .env.host ps
else
  echo "Skipping Docker infrastructure startup (--no-infra)."
  echo "Expected local dependencies:"
  echo "  Postgres:       ${SPRING_DATASOURCE_URL}"
  echo "  MinIO/S3:       ${AWS_S3_ENDPOINT}"
  echo "  embeddings:     ${EMBEDDING_EXTERNAL_API_URL}"
fi

wait_for_http() {
  local name="$1"
  local url="$2"
  local pid="${3:-}"
  for _ in $(seq 1 60); do
    if [[ -n "${pid}" ]] && ! kill -0 "${pid}" 2>/dev/null; then
      echo "${name} exited before becoming healthy" >&2
      return 1
    fi
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
wait_for_http "control-plane" "http://localhost:8080/actuator/health" "${CONTROL_PID}"

echo "Starting worker on localhost:8081..."
mvn -q -pl worker spring-boot:run &
WORKER_PID=$!
wait_for_http "worker" "http://localhost:8081/api/vectors/health" "${WORKER_PID}"

echo "Starting search-service on localhost:8083..."
mvn -q -pl search-service spring-boot:run &
SEARCH_PID=$!
wait_for_http "search-service" "http://localhost:8083/api/search/health" "${SEARCH_PID}"

echo
echo "VectorSync local dev is ready."
echo "Health checks:"
echo "  control-plane:  curl http://localhost:8080/actuator/health"
echo "  worker:         curl http://localhost:8081/actuator/health"
echo "  search-service: curl http://localhost:8083/actuator/health"
echo "  embeddings:     curl http://localhost:8000/api/v1/health"
echo
if [[ "${START_INFRA}" == "true" ]]; then
  echo "Press Ctrl-C to stop Java services. Docker infra remains running."
else
  echo "Press Ctrl-C to stop Java services."
fi

wait
