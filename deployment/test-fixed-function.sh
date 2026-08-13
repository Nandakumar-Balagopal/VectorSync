#!/bin/bash

echo "=== Original broken function ==="
python_json_value_broken() {
  python3 - "$@" <<'PY'
import json
import sys
raw = sys.stdin.read().strip()
if not raw:
    print("Empty input", file=sys.stderr)
    print("")
    raise SystemExit(0)
data = json.loads(raw)
key = sys.argv[1] if len(sys.argv) > 1 else None
if key:
    print(data.get(key, ""))
else:
    print(json.dumps(data, indent=2))
PY
}

echo '{"tableId":"test-123"}' | python_json_value_broken tableId
echo ""

echo "=== Fixed function - Method 1: Use /dev/stdin explicitly ==="
python_json_value_fixed1() {
  python3 <<PY "$@"
import json
import sys

with open('/dev/stdin', 'r') as f:
    raw = f.read().strip()

if not raw:
    print("Empty input", file=sys.stderr)
    print("")
    raise SystemExit(0)

data = json.loads(raw)
key = sys.argv[1] if len(sys.argv) > 1 else None
if key:
    print(data.get(key, ""))
else:
    print(json.dumps(data, indent=2))
PY
}

echo '{"tableId":"test-456"}' | python_json_value_fixed1 tableId
echo ""

echo "=== Fixed function - Method 2: Pass JSON as argument ==="
python_json_value_fixed2() {
  local json_input="$1"
  local key="$2"
  python3 <<PY
import json
import sys

raw = """$json_input""".strip()
if not raw:
    print("Empty input", file=sys.stderr)
    print("")
    raise SystemExit(0)

data = json.loads(raw)
key = "$key" if "$key" else None
if key:
    print(data.get(key, ""))
else:
    print(json.dumps(data, indent=2))
PY
}

json_data='{"tableId":"test-789"}'
python_json_value_fixed2 "$json_data" "tableId"
echo ""

echo "=== Fixed function - Method 3: Use python3 -c instead of heredoc ==="
python_json_value_fixed3() {
  python3 -c '
import json
import sys

raw = sys.stdin.read().strip()
if not raw:
    print("Empty input", file=sys.stderr)
    print("")
    raise SystemExit(0)

data = json.loads(raw)
key = sys.argv[1] if len(sys.argv) > 1 else None
if key:
    print(data.get(key, ""))
else:
    print(json.dumps(data, indent=2))
' "$@"
}

echo '{"tableId":"test-999"}' | python_json_value_fixed3 tableId
echo ""

echo "=== Test all three methods ==="
test_json='{"tableId":"final-test","name":"VectorSync","enabled":true}'

echo "Method 1 (dev/stdin):"
echo "$test_json" | python_json_value_fixed1 tableId

echo "Method 2 (argument):"
python_json_value_fixed2 "$test_json" "tableId"

echo "Method 3 (python -c):"
echo "$test_json" | python_json_value_fixed3 tableId

# Made with Bob
