#!/bin/bash

echo "=== Restarting Control API with CORS Fix ==="
echo ""

# Kill existing control-api process on port 8080
echo "1. Stopping existing control-api..."
lsof -ti:8080 | xargs kill -9 2>/dev/null || echo "No process on port 8080"

# Rebuild control-api
echo ""
echo "2. Rebuilding control-api..."
cd control-api
mvn clean install -DskipTests

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo ""
echo "3. Starting control-api..."
echo "   (Press Ctrl+C to stop)"
echo ""

# Start control-api
mvn spring-boot:run

# Made with Bob
