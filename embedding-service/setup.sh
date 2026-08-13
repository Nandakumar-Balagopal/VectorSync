#!/bin/bash

# Setup script for VectorSync Embedding Service
# Creates virtual environment and installs dependencies

set -e

echo "=== VectorSync Embedding Service Setup ==="
echo ""

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Found Python $PYTHON_VERSION"

# Check if Python 3.11+ is available
if ! python3 -c 'import sys; exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
    echo "✗ Error: Python 3.11 or higher is required"
    echo "  Current version: $PYTHON_VERSION"
    exit 1
fi

# Create virtual environment
if [ ! -d "venv" ]; then
    echo ""
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate
echo "✓ Virtual environment activated"

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1
echo "✓ pip upgraded"

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install -r requirements.txt
echo "✓ Dependencies installed"

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo ""
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "✓ .env file created"
    echo ""
    echo "⚠️  Please edit .env file with your configuration"
else
    echo ""
    echo "✓ .env file already exists"
fi

# Make sample requests script executable
if [ -f "examples/sample_requests.sh" ]; then
    chmod +x examples/sample_requests.sh
    echo "✓ Sample requests script is executable"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To activate the virtual environment, run:"
echo "  source venv/bin/activate"
echo ""
echo "To start the service, run:"
echo "  uvicorn app.main:app --reload"
echo ""
echo "To run tests, run:"
echo "  pytest"
echo ""
echo "To test the API, run:"
echo "  ./examples/sample_requests.sh"
echo ""

