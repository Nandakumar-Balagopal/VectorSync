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
  /** Identity only — Iceberg snapshot ids are random longs and cannot be ordered. */
  sourceSnapshotId: number;
  /** Iceberg's monotonic sequence number; this is what orders indexes by data version. */
  sourceSequenceNumber: number;
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

/** One index annotated with whether it still covers the newest source version. */
export interface IndexStatusRow {
  indexId: string;
  embeddingModel: string;
  embeddingVersion: string;
  dimension: number;
  vectorCount: number;
  status: 'BUILDING' | 'READY' | 'FAILED' | 'ARCHIVED';
  sourceSnapshotId: number;
  sourceSequenceNumber: number;
  /** False when the index was built over a snapshot that has since been superseded. */
  current: boolean;
  serving: boolean;
  evalMetrics: Record<string, string>;
  builtAt: string;
}

export interface EvaluationReport {
  indexId: string;
  k: number;
  /** Probes sampled from the index's own partition. Zero when recall came from supplied queries. */
  probeCount: number;
  /** Caller-supplied queries. Zero for a label-free run. */
  queryCount: number;
  /** Overlap with an exhaustive scan. Measures the index, not the model. No labels needed. */
  indexRecallAtK: number;
  /** Against judged relevance. Measures the model. Null when no labels were supplied. */
  precisionAtK: number | null;
  /** Identifier of the judged query set behind precisionAtK. */
  fixtureRef: string | null;
  /** False when precision was computed but not recorded, because no fixtureRef identified it. */
  precisionPersisted: boolean;
  perProbeRecall: Record<string, number>;
}
