-- Initialize database schema for Iceberg Vector

-- Table configurations
CREATE TABLE IF NOT EXISTS table_configs (
    table_id VARCHAR(255) PRIMARY KEY,
    catalog VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    embedding_columns TEXT NOT NULL,
    model_name VARCHAR(255) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    UNIQUE(catalog, table_name)
);

-- Sync state tracking
CREATE TABLE IF NOT EXISTS sync_state (
    table_id VARCHAR(255) PRIMARY KEY REFERENCES table_configs(table_id),
    last_snapshot_id BIGINT,
    last_sync_at TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_table_configs_enabled ON table_configs(enabled);
CREATE INDEX idx_sync_state_last_sync ON sync_state(last_sync_at);
