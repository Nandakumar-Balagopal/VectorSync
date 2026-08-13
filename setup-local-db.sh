#!/bin/bash

# Local Database Setup Script for VectorSync Table Sync Feature
# This script creates the PostgreSQL database and required tables

set -e  # Exit on error

echo "=========================================="
echo "VectorSync Local Database Setup"
echo "=========================================="
echo ""

# Configuration
DB_NAME="vectorsync"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_HOST="localhost"
DB_PORT="5432"

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "❌ PostgreSQL is not installed or not in PATH"
    echo ""
    echo "Please install PostgreSQL:"
    echo "  - macOS: brew install postgresql@15"
    echo "  - Ubuntu: sudo apt-get install postgresql-15"
    echo ""
    echo "Or use Docker:"
    echo "  docker run -d --name vectorsync-postgres \\"
    echo "    -e POSTGRES_PASSWORD=postgres \\"
    echo "    -p 5432:5432 postgres:15"
    exit 1
fi

echo "✓ PostgreSQL is installed"
echo ""

# Check if PostgreSQL is running
if ! pg_isready -h $DB_HOST -p $DB_PORT &> /dev/null; then
    echo "❌ PostgreSQL is not running on $DB_HOST:$DB_PORT"
    echo ""
    echo "Start PostgreSQL:"
    echo "  - macOS: brew services start postgresql@15"
    echo "  - Ubuntu: sudo systemctl start postgresql"
    echo "  - Docker: docker start vectorsync-postgres"
    exit 1
fi

echo "✓ PostgreSQL is running"
echo ""

# Create database if it doesn't exist
echo "Creating database '$DB_NAME'..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1 || \
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -c "CREATE DATABASE $DB_NAME"

echo "✓ Database '$DB_NAME' is ready"
echo ""

# Create tables
echo "Creating tables for Table Sync feature..."

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << 'EOF'

-- Create discovered_tables table
CREATE TABLE IF NOT EXISTS discovered_tables (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    location VARCHAR(1000) NOT NULL,
    warehouse_url VARCHAR(1000),
    metadata_location VARCHAR(1000) NOT NULL,
    uuid VARCHAR(255) UNIQUE,
    schema_json TEXT,
    partition_spec_json TEXT,
    properties_json TEXT,
    total_records BIGINT,
    total_files BIGINT,
    total_size BIGINT,
    discovered_at TIMESTAMP NOT NULL,
    registered BOOLEAN NOT NULL DEFAULT FALSE,
    registered_at TIMESTAMP,
    sync_job_id VARCHAR(500),
    catalog_name VARCHAR(500)
);

-- Create sync_jobs table
CREATE TABLE IF NOT EXISTS sync_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    catalog_name VARCHAR(255) NOT NULL,
    s3_path VARCHAR(1000) NOT NULL,
    status VARCHAR(50) NOT NULL,
    sync_existing_tables BOOLEAN,
    register_new_tables BOOLEAN,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    tables_discovered INTEGER,
    tables_registered INTEGER,
    tables_updated INTEGER,
    tables_failed INTEGER,
    error_message TEXT,
    error_details TEXT,
    created_by VARCHAR(500)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_discovered_tables_uuid ON discovered_tables(uuid);
CREATE INDEX IF NOT EXISTS idx_discovered_tables_sync_job_id ON discovered_tables(sync_job_id);
CREATE INDEX IF NOT EXISTS idx_discovered_tables_registered ON discovered_tables(registered);
CREATE INDEX IF NOT EXISTS idx_discovered_tables_catalog ON discovered_tables(catalog_name);
CREATE INDEX IF NOT EXISTS idx_discovered_tables_schema_table ON discovered_tables(schema_name, table_name);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs(status);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_catalog ON sync_jobs(catalog_name);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_started_at ON sync_jobs(started_at DESC);

EOF

echo "✓ Tables created successfully"
echo ""

# Verify tables
echo "Verifying tables..."
TABLE_COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('discovered_tables', 'sync_jobs')")

if [ "$TABLE_COUNT" -eq 2 ]; then
    echo "✓ All tables verified"
else
    echo "⚠️  Warning: Expected 2 tables, found $TABLE_COUNT"
fi

echo ""
echo "=========================================="
echo "✅ Database setup complete!"
echo "=========================================="
echo ""
echo "Database Details:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"
echo ""
echo "Environment Variables (add to .env):"
echo "  SPRING_DATASOURCE_URL=jdbc:postgresql://$DB_HOST:$DB_PORT/$DB_NAME"
echo "  SPRING_DATASOURCE_USERNAME=$DB_USER"
echo "  SPRING_DATASOURCE_PASSWORD=$DB_PASSWORD"
echo ""
echo "Next Steps:"
echo "  1. Set environment variables (or add to .env file)"
echo "  2. cd control-api && mvn spring-boot:run"
echo "  3. cd dashboard && npm run dev"
echo "  4. Open http://localhost:5173 and test the sync feature"
echo ""

# Made with Bob
