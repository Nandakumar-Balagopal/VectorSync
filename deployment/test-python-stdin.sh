#!/bin/bash

echo "=== Test 1: Simple Python stdin test ==="
echo '{"test":"value"}' | python3 -c "import sys; print(sys.stdin.read())"
echo ""

echo "=== Test 2: Test the python_json_value function directly ==="
python_json_value() {
  python3 - "$@" <<'PY'
import json
import sys

print(f"DEBUG: sys.argv = {sys.argv}", file=sys.stderr)
raw = sys.stdin.read().strip()
print(f"DEBUG: raw input = '{raw}'", file=sys.stderr)
print(f"DEBUG: raw length = {len(raw)}", file=sys.stderr)

if not raw:
    print("DEBUG: Empty input to python_json_value", file=sys.stderr)
    print("")
    raise SystemExit(0)

try:
    data = json.loads(raw)
    print(f"DEBUG: Parsed JSON successfully", file=sys.stderr)
except json.JSONDecodeError as e:
    print(f"DEBUG: JSON decode error: {e}", file=sys.stderr)
    print("")
    raise SystemExit(0)

key = sys.argv[1] if len(sys.argv) > 1 else None
print(f"DEBUG: Looking for key = '{key}'", file=sys.stderr)
if key:
    value = data.get(key, "")
    print(f"DEBUG: Found value = '{value}'", file=sys.stderr)
    print(value)
else:
    print(json.dumps(data, indent=2))
PY
}

echo '{"tableId":"test-123"}' | python_json_value tableId
echo ""

echo "=== Test 3: Check if the issue is with the heredoc ==="
echo '{"tableId":"test-456"}' | python3 <<'EOF'
import json
import sys
raw = sys.stdin.read().strip()
print(f"Raw: {raw}", file=sys.stderr)
data = json.loads(raw)
print(data.get("tableId", ""))
EOF
echo ""

echo "=== Test 4: Test with arguments passed to python3 - ==="
echo '{"tableId":"test-789"}' | python3 - tableId <<'EOF'
import json
import sys
print(f"Args: {sys.argv}", file=sys.stderr)
raw = sys.stdin.read().strip()
print(f"Raw: {raw}", file=sys.stderr)
data = json.loads(raw)
key = sys.argv[1] if len(sys.argv) > 1 else None
print(data.get(key, ""))
EOF

# Made with Bob
