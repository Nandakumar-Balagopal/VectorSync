import axios from 'axios';
import type {
  TableConfig,
  SearchResponse,
  SyncStatus,
  ModelVersions,
  IndexManifestEntry,
  IndexAliasEntry,
  DiscoveredTable,
} from '../types';

/**
 * Every call here hits a real endpoint.
 *
 * This module previously wrapped most calls in catch blocks that returned fabricated data --
 * invented latency percentiles, fake workers, Math.random() health -- against endpoints that
 * did not exist. Failures now surface to the caller so the UI can say "unavailable" instead of
 * showing numbers that were never measured.
 */
/**
 * No baseURL: the three prefixes below are siblings at the root, not children of one base.
 * Setting baseURL to '/api' silently produced '/api/worker-api/...' and '/api/search-api/...',
 * which nginx does not serve, so every worker and search call 404'd.
 */
const api = axios.create({
  headers: { 'Content-Type': 'application/json' },
});

// Must match the location blocks in dashboard/nginx.conf.
const CONTROL = '/api';          // -> control-plane:8080/api/
const WORKER = '/worker-api';    // -> worker:8081/api/
const SEARCH = '/search-api';    // -> search-service:8083/api/

export const vectorSyncApi = {
  // ---------- tables (control-plane) ----------

  getTables: async (): Promise<TableConfig[]> => {
    const response = await api.get(`${CONTROL}/tables`);
    return response.data;
  },

  getTable: async (tableId: string): Promise<TableConfig> => {
    const response = await api.get(`${CONTROL}/tables/${tableId}`);
    return response.data;
  },

  getSyncStatus: async (tableId: string): Promise<SyncStatus> => {
    const response = await api.get(`${CONTROL}/tables/${tableId}/status`);
    return response.data;
  },

  registerTable: async (config: Omit<TableConfig, 'tableId'>): Promise<TableConfig> => {
    const response = await api.post(`${CONTROL}/tables/register`, config);
    return response.data;
  },

  /** Bumping embeddingVersion starts a migration; the existing version is retained. */
  setEmbeddingVersion: async (tableId: string, embeddingVersion: string): Promise<TableConfig> => {
    const response = await api.put(`${CONTROL}/tables/${tableId}`, { embeddingVersion });
    return response.data;
  },

  deleteTable: async (tableId: string, deleteEmbeddings = false): Promise<void> => {
    await api.delete(`${CONTROL}/tables/${tableId}?deleteEmbeddings=${deleteEmbeddings}`);
  },

  // ---------- discovered tables (control-plane S3 crawl) ----------

  getDiscoveredTables: async (registered?: boolean): Promise<DiscoveredTable[]> => {
    const url = registered !== undefined
      ? `${CONTROL}/tables/discovered?registered=${registered}`
      : `${CONTROL}/tables/discovered`;
    const response = await api.get(url);
    return response.data;
  },

  startTableDiscovery: async (request: Record<string, unknown>): Promise<{ jobId: string }> => {
    const response = await api.post(`${CONTROL}/tables/sync`, request);
    return response.data;
  },

  // ---------- materialization (worker) ----------

  triggerSync: async (): Promise<{ tablesSynced: number; vectorCount: number }> => {
    const response = await api.post(`${WORKER}/demo/sync`);
    return response.data;
  },

  getVectorCount: async (): Promise<number> => {
    const response = await api.get(`${WORKER}/vectors/count`);
    return response.data;
  },

  // ---------- lifecycle (search-service) ----------

  getModelVersions: async (sourceTable: string): Promise<ModelVersions> => {
    const response = await api.get(
      `${SEARCH}/lifecycle/model-versions?sourceTable=${encodeURIComponent(sourceTable)}`);
    return response.data;
  },

  getIndexes: async (sourceTable?: string): Promise<IndexManifestEntry[]> => {
    const url = sourceTable
      ? `${SEARCH}/lifecycle/indexes?sourceTable=${encodeURIComponent(sourceTable)}`
      : `${SEARCH}/lifecycle/indexes`;
    const response = await api.get(url);
    return response.data;
  },

  getPromotedIndex: async (sourceTable: string): Promise<IndexManifestEntry | { promoted: false }> => {
    const response = await api.get(
      `${SEARCH}/lifecycle/promoted?sourceTable=${encodeURIComponent(sourceTable)}`);
    return response.data;
  },

  getPromotionHistory: async (sourceTable: string): Promise<IndexAliasEntry[]> => {
    const response = await api.get(
      `${SEARCH}/lifecycle/history?sourceTable=${encodeURIComponent(sourceTable)}`);
    return response.data;
  },

  buildIndex: async (sourceTable: string, modelVersion: string): Promise<IndexManifestEntry> => {
    const response = await api.post(`${SEARCH}/lifecycle/index/build`, { sourceTable, modelVersion });
    return response.data;
  },

  promoteIndex: async (
    sourceTable: string, indexId: string, note?: string,
  ): Promise<IndexAliasEntry> => {
    const response = await api.post(`${SEARCH}/lifecycle/promote`, {
      sourceTable, indexId, promotedBy: 'dashboard', note,
    });
    return response.data;
  },

  rollbackIndex: async (sourceTable: string): Promise<IndexAliasEntry> => {
    const response = await api.post(
      `${SEARCH}/lifecycle/rollback?sourceTable=${encodeURIComponent(sourceTable)}&rolledBackBy=dashboard`);
    return response.data;
  },

  // ---------- search ----------

  search: async (query: string, sourceTable: string, topK = 5): Promise<SearchResponse> => {
    const response = await api.post(`${SEARCH}/search`, { query, sourceTable, topK });
    return response.data;
  },

  searchExact: async (query: string, sourceTable: string, topK = 5): Promise<SearchResponse> => {
    const response = await api.post(`${SEARCH}/search/exact`, { query, sourceTable, topK });
    return response.data;
  },

  // ---------- provenance ----------

};
