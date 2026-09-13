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

export type IndexStatus = 'BUILDING' | 'READY' | 'FAILED' | 'ARCHIVED';

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
  status: IndexStatus;
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

export interface EvaluationReport {
  indexId: string;
  queryCount: number;
  k: number;
  /** Overlap with an exhaustive scan. Measures the index, not the model. */
  indexRecallAtK: number;
  /** Against judged relevance. Measures the model. Null when no labels were supplied. */
  precisionAtK: number | null;
  perQueryIndexRecall: Record<string, number>;
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

export interface ProvenanceChain {
  vector: Record<string, unknown>;
  servedByAlias: Record<string, unknown> | null;
  index: Record<string, unknown> | null;
  embedding: Record<string, unknown>;
  source: Record<string, unknown>;
  sourceRowAtSnapshot: Record<string, unknown> | null;
}

export interface RowEmbeddingHistory {
  vectorId: string;
  modelVersion: string;
  sourceSnapshotId: number;
  deleted: boolean;
  text: string | null;
  createdAt: string;
  metadata: Record<string, string>;
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
