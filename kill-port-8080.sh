#!/bin/bash

# Script to kill process running on port 8080

echo "Finding process on port 8080..."

# Find the PID of the process using port 8080
PID=$(lsof -ti:8080)

if [ -z "$PID" ]; then
    echo "No process found running on port 8080"
    exit 0
fi

echo "Found process with PID: $PID"
echo "Killing process..."

# Kill the process
kill -9 $PID

# Verify it's killed
sleep 1
if lsof -ti:8080 > /dev/null 2>&1; then
    echo "❌ Failed to kill process on port 8080"
    exit 1
else
    echo "✅ Successfully killed process on port 8080"
fi

# Made with Bob
