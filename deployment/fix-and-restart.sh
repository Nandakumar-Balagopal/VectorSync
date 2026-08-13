#!/bin/bash

echo "🔧 VectorSync Quick Fix & Restart"
echo "=================================="
echo ""

# Kill any process on port 8000
echo "1. Freeing port 8000..."
lsof -ti:8000 | xargs kill -9 2>/dev/null || echo "   Port 8000 is free"

# Remove the stuck container
echo "2. Removing stuck embedding container..."
docker rm -f vectorsync-embedding 2>/dev/null || echo "   No stuck container"

# Stop all services using docker compose (with space)
echo "3. Stopping all services..."
docker compose --profile local-storage --profile local-embedding down 2>/dev/null || echo "   Services already stopped"

# Wait a moment
sleep 2

# Start everything fresh
echo "4. Starting all services..."
docker compose --profile local-storage --profile local-embedding up -d --build

echo ""
echo "✅ Services starting up!"
echo ""
echo "Wait 60 seconds, then run the setup commands from DEMO_COMMANDS.md"
echo ""
echo "Quick check status:"
echo "  docker compose ps"

# Made with Bob
