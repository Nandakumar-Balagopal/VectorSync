-- Fix sync_jobs table column name issue
-- The table was created by Hibernate with 's3path' but should be 's3_path'

-- Drop the incorrectly created table
DROP TABLE IF EXISTS sync_jobs CASCADE;

-- Recreate with correct schema
CREATE TABLE sync_jobs (
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

-- Recreate indexes
CREATE INDEX idx_sync_jobs_status ON sync_jobs(status);
CREATE INDEX idx_sync_jobs_catalog ON sync_jobs(catalog_name);
CREATE INDEX idx_sync_jobs_started_at ON sync_jobs(started_at DESC);

-- Made with Bob
