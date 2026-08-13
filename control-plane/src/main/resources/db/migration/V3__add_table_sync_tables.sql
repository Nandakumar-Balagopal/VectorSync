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

-- Made with Bob
