// Type definitions for VectorSync Dashboard

export type HealthStatus = 'healthy' | 'lagging' | 'broken' | 'unknown';
export type SyncStatus = 'synced' | 'syncing' | 'error' | 'pending';

export interface TableConfig {
  tableId: string;
  catalog: string;
  tableName: string;
  embeddingColumns: string[];
  vectorColumn: string; // Primary vector column
  textColumn: string; // Source text column
  primaryKey?: string; // Primary key column for unique identification
  modelName: string;
  enabled: boolean;
  syncEnabled: boolean;
  createdAt?: string;
}

export interface TableHealth {
  tableId: string;
  health: HealthStatus;
  lag: number; // seconds
  pendingRows: number;
  indexStatus: 'fresh' | 'stale' | 'broken' | 'unknown';
  lastSyncTime: string | null;
  errorMessage?: string;
}

export interface SyncState {
  tableId: string;
  lastSnapshotId: number | null;
  lastSyncTime: string | null;
  status: SyncStatus;
}

export interface SystemMetrics {
  syncStatus: 'active' | 'inactive';
  totalVectors: number;
  avgSyncLatency: number;
  embeddingProvider: string;
  tablesHealthy?: number;
  tablesTotal?: number;
  tablesLagging?: number;
  totalPendingRows?: number;
  avgIndexFreshness?: number;
}

export interface SearchResult {
  vectorId: string;
  sourceTable: string;
  sourceRowId: string;
  similarity: number;
  text: string;
  metadata: Record<string, unknown>;
}

export interface SearchResponse {
  query: string;
  executionTimeMs: number;
  totalResults: number;
  results: SearchResult[];
}

export interface Job {
  jobId: string;
  tableId: string;
  tableName: string;
  type: 'embedding' | 'cdc' | 'index';
  status: 'queued' | 'running' | 'completed' | 'failed';
  retries: number;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  errorMessage?: string;
}

export interface TableDetails {
  config: TableConfig;
  health: TableHealth;
  syncState: SyncState;
  cdcTimeline: CDCSnapshot[];
  embeddingStats: EmbeddingStats;
  indexStats: IndexStats;
}

export interface CDCSnapshot {
  snapshotId: number;
  timestamp: string;
  rowsChanged: number;
  operation: 'insert' | 'update' | 'delete';
  type: 'insert' | 'update' | 'delete'; // Alias for operation
  count: number; // Alias for rowsChanged
  processed: boolean;
}

export interface EmbeddingStats {
  totalRows: number;
  embeddedRows: number;
  pendingRows: number;
  failedRows: number;
  coveragePercent: number;
  throughput: number; // rows per second
  failures: number;
  retries: number;
  lastEmbeddingTime?: string;
}

export interface IndexStats {
  lastUpdatedSnapshot: number;
  vectorCount: number;
  freshnessScore: number; // 0-100
  indexType: 'hnsw' | 'brute-force';
  status: 'fresh' | 'stale' | 'broken' | 'unknown';
  dimension: number;
  m: number; // HNSW M parameter
  efConstruction: number; // HNSW ef_construction parameter
  buildTime?: number;
  lastBuildTime?: string;
}

export interface Alert {
  id: string;
  severity: 'critical' | 'warning' | 'info';
  message: string;
  tableId?: string;
  timestamp: string;
  acknowledged: boolean;
}

export interface GlobalPipelineStats {
  totalJobsQueued: number;
  activeWorkers: number;
  cpuUsage: number;
  memoryUsage: number;
  backlogSize: number;
  jobsInProgress?: number;
  jobsCompleted?: number;
  throughput?: number; // jobs per minute
  successRate?: number; // percentage
  avgProcessingTime?: number; // seconds
}

export interface WorkerInfo {
  workerId: string;
  status: 'active' | 'idle' | 'offline';
  cpuUsage: number;
  memoryUsage: number;
  currentJobs: number;
  maxCapacity: number;
  uptime: number; // seconds
  lastHeartbeat: string;
}

export interface PerformanceMetrics {
  cdcLatency: LatencyMetrics;
  embeddingLatency: LatencyMetrics;
  indexLatency: LatencyMetrics;
  errorRates: ErrorRates;
}

export interface LatencyMetrics {
  p50: number; // milliseconds
  p95: number;
  p99: number;
  avg: number;
}

export interface ErrorRates {
  cdcErrors: number;
  embeddingErrors: number;
  indexErrors: number;
  totalErrors: number;
  errorRate: number; // percentage
}

export interface RowDebugInfo {
  rowId: string;
  tableId: string;
  tableName: string;
  sourceData: Record<string, unknown>;
  embedding: number[];
  embeddingDimension: number;
  modelUsed: string;
  createdAt: string;
  lastModified: string;
  cdcHistory: CDCEvent[];
  nearestNeighbors?: NearestNeighbor[];
}

export interface CDCEvent {
  snapshotId: number;
  timestamp: string;
  operation: 'insert' | 'update' | 'delete';
  beforeValue?: Record<string, unknown>;
  afterValue?: Record<string, unknown>;
}

export interface NearestNeighbor {
  rowId: string;
  similarity: number;
  text: string;
  distance: number;
}

export interface EmbeddingComparison {
  rowId: string;
  model1: string;
  model2: string;
  embedding1: number[];
  embedding2: number[];
  cosineSimilarity: number;
  euclideanDistance: number;
}

export interface SystemConfig {
  embeddingService: {
    endpoint: string;
    timeout: number;
    retryAttempts: number;
  };
  defaultModel: string;
  workerPool: {
    size: number;
    maxConcurrent: number;
  };
  syncFrequency: number; // seconds
  retryPolicy: {
    maxRetries: number;
    backoffMultiplier: number;
    initialDelay: number;
  };
}

export interface TableConfigUpdate {
  tableId: string;
  modelName?: string;
  syncEnabled?: boolean;
  syncFrequency?: number;
  priority?: 'high' | 'medium' | 'low';
  batchSize?: number;
}

export interface ModelInfo {
  id: string;
  name: string;
  dimension: number;
  maxTokens: number;
  provider: 'openai' | 'cohere' | 'local' | 'custom';
  endpoint?: string;
  performance: {
    avgLatency: number; // ms
    throughput: number; // tokens/sec
  };
}

export interface StorageConfig {
  iceberg: {
    catalogType: 'rest' | 'hive' | 'glue';
    uri: string;
    warehouse: string;
  };
  s3: {
    endpoint: string;
    accessKey: string;
    secretKey: string;
    bucket: string;
    region: string;
  };
  database: {
    host: string;
    port: number;
    database: string;
    username: string;
  };
}

export interface IndexConfig {
  hnsw: {
    m: number;
    efConstruction: number;
    efSearch: number;
  };
  cacheSize: number; // MB
  rebuildThreshold: number; // staleness percentage
}

// Made with Bob
