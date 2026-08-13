# Database Setup for Table Sync Feature

The application failed to start because it couldn't connect to the database or create tables. Here are the solutions:

## Option 1: Run SQL Script Manually (Quickest)

If your PostgreSQL database is running, connect to it and run this SQL:

```sql
-- Connect to your vectorsync database
\c vectorsync

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

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_discovered_tables_uuid ON discovered_tables(uuid);
CREATE INDEX IF NOT EXISTS idx_discovered_tables_sync_job_id ON discovered_tables(sync_job_id);
CREATE INDEX IF NOT EXISTS idx_discovered_tables_registered ON discovered_tables(registered);
CREATE INDEX IF NOT EXISTS idx_discovered_tables_catalog ON discovered_tables(catalog_name);
CREATE INDEX IF NOT EXISTS idx_discovered_tables_schema_table ON discovered_tables(schema_name, table_name);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs(status);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_catalog ON sync_jobs(catalog_name);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_started_at ON sync_jobs(started_at DESC);
```

### Using psql command line:

```bash
psql -U postgres -d vectorsync -f control-api/src/main/resources/db/migration/V3__add_table_sync_tables.sql
```

## Option 2: Check Database Connection

Make sure your database is running and environment variables are set:

```bash
# Check if PostgreSQL is running
pg_isready

# Or check Docker container
docker ps | grep postgres

# Set environment variables (add to .env or export)
export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/vectorsync
export SPRING_DATASOURCE_USERNAME=postgres
export SPRING_DATASOURCE_PASSWORD=your_password
```

## Option 3: Start Database with Docker

If you don't have PostgreSQL running:

```bash
# Start PostgreSQL with Docker
docker run -d \
  --name vectorsync-postgres \
  -e POSTGRES_DB=vectorsync \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15

# Wait a few seconds for it to start, then run the SQL script
docker exec -i vectorsync-postgres psql -U postgres -d vectorsync < control-api/src/main/resources/db/migration/V3__add_table_sync_tables.sql
```

## Option 4: Use Docker Compose

If you have docker-compose.yml with database service:

```bash
# Start just the database
docker-compose up -d postgres

# Run migrations
docker-compose exec postgres psql -U postgres -d vectorsync -f /path/to/V3__add_table_sync_tables.sql
```

## After Database is Ready

Try starting the control-api again:

```bash
cd control-api
mvn spring-boot:run
```

## Verify Tables Were Created

```sql
-- Check if tables exist
\dt

-- Or
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('discovered_tables', 'sync_jobs');

-- Check table structure
\d discovered_tables
\d sync_jobs
```

## Troubleshooting

### Error: "database does not exist"

Create the database first:
```sql
CREATE DATABASE vectorsync;
```

### Error: "role does not exist"

Create the user:
```sql
CREATE USER postgres WITH PASSWORD 'postgres';
GRANT ALL PRIVILEGES ON DATABASE vectorsync TO postgres;
```

### Error: "connection refused"

- Check if PostgreSQL is running: `systemctl status postgresql` or `brew services list`
- Check if port 5432 is in use: `lsof -i :5432`
- Verify connection string in .env file

## Quick Test

After tables are created, test the API:

```bash
# Start control-api
cd control-api
mvn spring-boot:run

# In another terminal, test the endpoint
curl http://localhost:8080/api/tables/sync/jobs
```

Should return an empty array `[]` if successful.