#!/bin/bash

echo "=== Test 1: Check if jq is available ==="
if command -v jq &> /dev/null; then
    echo "✓ jq is installed"
    jq --version
else
    echo "✗ jq is NOT installed"
    echo "Install with: brew install jq (macOS) or apt-get install jq (Linux)"
    exit 1
fi
echo ""

echo "=== Test 2: Extract tableId from single object ==="
response='{"tableId":"23169fcb-2a1f-45e5-8cb7-952cb8b359b1","catalog":"default","tableName":"products"}'
table_id=$(echo "$response" | jq -r '.tableId // empty')
echo "Response: $response"
echo "Extracted tableId: $table_id"
echo ""

echo "=== Test 3: Extract tableId from array (find by tableName) ==="
array_response='[{"tableId":"abc-123","tableName":"products"},{"tableId":"def-456","tableName":"orders"}]'
table_id=$(echo "$array_response" | jq -r '.[] | select(.tableName == "products") | .tableId // empty')
echo "Response: $array_response"
echo "Extracted tableId for 'products': $table_id"
echo ""

echo "=== Test 4: Handle empty response ==="
empty_response=""
table_id=$(echo "$empty_response" | jq -r '.tableId // empty' 2>/dev/null || echo "")
echo "Response: (empty)"
echo "Extracted tableId: '$table_id'"
echo ""

echo "=== Test 5: Simulate full registration flow ==="
register_response='{"tableId":"final-test-id","catalog":"default","tableName":"products"}
201'

http_code=$(echo "$register_response" | tail -n1)
response_body=$(echo "$register_response" | sed '$d')

echo "HTTP Code: $http_code"
echo "Response Body: $response_body"

if [[ "$http_code" == "201" ]]; then
  table_id=$(echo "$response_body" | jq -r '.tableId // empty')
  echo "Extracted tableId: $table_id"
  
  if [[ -z "$table_id" ]]; then
    echo "ERROR: Failed to extract table ID!"
    exit 1
  else
    echo "SUCCESS: Table ID extracted correctly!"
  fi
fi
echo ""

echo "=== Test 6: Generic function for both cases ==="
get_table_id() {
  local json="$1"
  local table_name="${2:-}"
  
  if [[ -z "$json" ]]; then
    echo ""
    return
  fi
  
  # Check if it's an array or object
  if echo "$json" | jq -e 'type == "array"' >/dev/null 2>&1; then
    # Array: find by tableName if provided
    if [[ -n "$table_name" ]]; then
      echo "$json" | jq -r ".[] | select(.tableName == \"$table_name\") | .tableId // empty"
    else
      echo "$json" | jq -r '.[0].tableId // empty'
    fi
  else
    # Object: just get tableId
    echo "$json" | jq -r '.tableId // empty'
  fi
}

single='{"tableId":"single-123","tableName":"test"}'
array='[{"tableId":"array-456","tableName":"products"}]'

echo "Single object:"
get_table_id "$single"

echo "Array with tableName filter:"
get_table_id "$array" "products"

echo "Array without filter (first item):"
get_table_id "$array"

# Made with Bob
