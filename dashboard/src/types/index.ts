// Type definitions mirroring the backend API. Every type here corresponds to a real response.

export interface TableConfig {
  tableId: string;
  catalog: string;
  tableName: string;
  embeddingColumns: string[];
  modelName: string;
  embeddingVersion?: string;
  enabled: boolean;
  createdAt?: string;
}

export interface SyncStatus {
  tableId: string;
  tableName: string;
  enabled: boolean;
  lastSnapshotId: number | null;
  lastSyncAt: string | null;
}

export interface ModelVersions {
  sourceTable: string;
  modelVersions: string[];
  latestSourceSnapshotId: number;
}

export interface IndexManifestEntry {
  indexId: string;
  sourceTable: string;
  sourceSnapshotId: number;
  embeddingModel: string;
  embeddingVersion: string;
  partitionValue?: string | null;
  indexAlgorithm: string;
  indexParams: Record<string, string>;
  similarityMetric: string;
  dimension: number;
  indexUri: string;
  indexFiles: string[];
  vectorCount: number;
  status: 'BUILDING' | 'READY' | 'FAILED' | 'ARCHIVED';
  evalMetrics: Record<string, string>;
  builtAt: string;
  updatedAt?: string;
  errorMessage?: string | null;
}

export interface IndexAliasEntry {
  aliasName: string;
  sourceTable: string;
  indexId: string;
  updatedAt: string;
  updatedBy?: string;
  note?: string;
}

export interface SearchResult {
  vectorId: string;
  sourceTable: string;
  sourceRowId: string;
  similarity: number;
  text: string;
}

export interface SearchResponse {
  query: string;
  executionTimeMs?: number;
  totalResults: number;
  results: SearchResult[];
}

export interface DiscoveredTable {
  uuid: string;
  tableName: string;
  schemaName: string;
  location: string;
  metadataLocation: string;
  totalRecords: number | null;
  totalFiles: number | null;
  totalSize: number | null;
  discoveredAt: string;
  registered: boolean;
  catalogName: string;
}
