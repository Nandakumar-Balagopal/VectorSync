#!/bin/bash

# Test script to debug JSON parsing issue in demo-run.sh

# Python JSON parser function (copied from demo-run.sh)
python_json_value() {
  python3 - "$@" <<'PY'
import json
import sys

raw = sys.stdin.read().strip()
if not raw:
    print("DEBUG: Empty input to python_json_value", file=sys.stderr)
    print("")
    raise SystemExit(0)

try:
    data = json.loads(raw)
except json.JSONDecodeError as e:
    print(f"DEBUG: JSON decode error: {e}", file=sys.stderr)
    print("")
    raise SystemExit(0)

key = sys.argv[1] if len(sys.argv) > 1 else None
if key:
    value = data.get(key, "")
    print(value)
else:
    print(json.dumps(data, indent=2))
PY
}

echo "=== Test 1: Simulating curl response with HTTP code ==="
# Simulate what curl returns with -w "\n%{http_code}"
mock_curl_response='{"tableId":"23169fcb-2a1f-45e5-8cb7-952cb8b359b1","catalog":"default","tableName":"products","embeddingColumns":["name","description"],"modelName":"mock-embedding-v1","enabled":true,"createdAt":"2026-04-23T04:00:00Z"}
201'

echo "Mock curl response:"
echo "$mock_curl_response"
echo ""

echo "=== Test 2: Extract HTTP code (last line) ==="
http_code=$(echo "$mock_curl_response" | tail -n1)
echo "HTTP code: $http_code"
echo ""

echo "=== Test 3: Extract response body (all but last line) ==="
response_body=$(echo "$mock_curl_response" | sed '$d')
echo "Response body: $response_body"
echo "Response body length: ${#response_body}"
echo ""

echo "=== Test 4: Try to extract tableId using printf ==="
table_id=$(printf "%s" "$response_body" | python_json_value tableId)
echo "Extracted table ID: $table_id"
echo ""

echo "=== Test 5: Try to extract tableId using echo ==="
table_id2=$(echo "$response_body" | python_json_value tableId)
echo "Extracted table ID (echo): $table_id2"
echo ""

echo "=== Test 6: Direct pipe test ==="
echo '{"tableId":"test-123","name":"test"}' | python_json_value tableId
echo ""

echo "=== Test 7: Check if response_body has trailing newline ==="
printf "Response body hex dump (first 200 bytes):\n"
printf "%s" "$response_body" | head -c 200 | od -A x -t x1z -v
echo ""

echo "=== Test 8: Alternative extraction method ==="
# Try using head instead of sed
response_body_alt=$(echo "$mock_curl_response" | head -n -1)
echo "Response body (using head): $response_body_alt"
table_id_alt=$(printf "%s" "$response_body_alt" | python_json_value tableId)
echo "Extracted table ID (head method): $table_id_alt"
echo ""

echo "=== Test 9: Check what printf actually outputs ==="
printf "Output of printf (piped to cat -A to show special chars):\n"
printf "%s" "$response_body" | cat -A
echo ""
echo ""

echo "=== Test 10: Variable assignment in subshell ==="
result=$(
  response='{"tableId":"subshell-test","name":"test"}
201'
  body=$(echo "$response" | sed '$d')
  printf "%s" "$body" | python_json_value tableId
)
echo "Result from subshell: $result"

# Made with Bob
