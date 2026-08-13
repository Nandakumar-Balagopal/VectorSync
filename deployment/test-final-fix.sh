#!/bin/bash

# Test the fixed python_json_value function from demo-run.sh

python_json_value() {
  python3 -c '
import json
import sys

raw = sys.stdin.read().strip()
if not raw:
    print("", file=sys.stderr)
    print("DEBUG: Empty input to python_json_value", file=sys.stderr)
    print("")
    raise SystemExit(0)

print(f"DEBUG: Python received: {raw[:200]}", file=sys.stderr)

try:
    data = json.loads(raw)
except json.JSONDecodeError as e:
    print(f"DEBUG: JSON parse error: {e}", file=sys.stderr)
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

print(f"DEBUG: Extracted value for key '"'"'{key}'"'"': {value}", file=sys.stderr)
print(value)
' "$@"
}

echo "=== Test 1: Single object response (registration) ==="
mock_register_response='{"tableId":"23169fcb-2a1f-45e5-8cb7-952cb8b359b1","catalog":"default","tableName":"products","embeddingColumns":["name","description"],"modelName":"mock-embedding-v1","enabled":true,"createdAt":"2026-04-23T04:00:00Z"}
201'

http_code=$(echo "$mock_register_response" | tail -n1)
response_body=$(echo "$mock_register_response" | sed '$d')

echo "HTTP Code: $http_code"
echo "Response Body: $response_body"
echo ""

table_id=$(printf "%s" "$response_body" | python_json_value tableId)
echo "Extracted Table ID: $table_id"
echo ""

echo "=== Test 2: Array response (list tables) ==="
mock_list_response='[{"tableId":"abc-123","catalog":"default","tableName":"products","enabled":true},{"tableId":"def-456","catalog":"default","tableName":"orders","enabled":false}]'

echo "Response: $mock_list_response"
echo ""

# Extract tableId for "products" table
table_id2=$(printf "%s" "$mock_list_response" | python_json_value tableId products)
echo "Extracted Table ID for 'products': $table_id2"
echo ""

# Extract tableId for "orders" table
table_id3=$(printf "%s" "$mock_list_response" | python_json_value tableId orders)
echo "Extracted Table ID for 'orders': $table_id3"
echo ""

echo "=== Test 3: Empty response ==="
empty_response=""
table_id4=$(printf "%s" "$empty_response" | python_json_value tableId)
echo "Extracted Table ID from empty: '$table_id4'"
echo ""

echo "=== Test 4: Simulate full registration flow ==="
register_response='{"tableId":"final-test-id","catalog":"default","tableName":"products","embeddingColumns":["name","description"],"modelName":"mock-embedding-v1","enabled":true}
201'

http_code=$(echo "$register_response" | tail -n1)
response_body=$(echo "$register_response" | sed '$d')

if [[ "$http_code" == "201" ]]; then
  echo "Registration successful (HTTP $http_code)"
  existing_table_id=$(printf "%s" "$response_body" | python_json_value tableId)
  echo "Table ID: $existing_table_id"
  
  if [[ -z "$existing_table_id" ]]; then
    echo "ERROR: Failed to extract table ID!"
    exit 1
  else
    echo "SUCCESS: Table ID extracted correctly!"
  fi
fi

# Made with Bob
