import axios from 'axios';
import type {
  TableConfig,
  SearchResponse,
  SystemMetrics,
  TableHealth,
  TableDetails,
  Job,
  Alert,
  GlobalPipelineStats,
} from '../types';

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
});

export const vectorSyncApi = {
  // Table Management
  getTables: async (): Promise<TableConfig[]> => {
    try {
      const response = await api.get('/tables');
      return response.data;
    } catch {
      // Mock data for development
      return [
        {
          tableId: 'table-1',
          catalog: 'iceberg_data',
          tableName: 'products',
          embeddingColumns: ['description'],
          vectorColumn: 'description_vector',
          textColumn: 'description',
          modelName: 'sentence-transformers/all-MiniLM-L6-v2',
          enabled: true,
          syncEnabled: true,
        },
        {
          tableId: 'table-2',
          catalog: 'iceberg_data',
          tableName: 'reviews',
          embeddingColumns: ['review_text'],
          vectorColumn: 'review_vector',
          textColumn: 'review_text',
          modelName: 'sentence-transformers/all-MiniLM-L6-v2',
          enabled: true,
          syncEnabled: true,
        },
      ];
    }
  },

  getTable: async (tableId: string): Promise<TableConfig> => {
    try {
      const response = await api.get(`/tables/${tableId}`);
      return response.data;
    } catch {
      // Mock data - find from getTables
      const tables = await vectorSyncApi.getTables();
      const table = tables.find(t => t.tableId === tableId);
      if (!table) {
        throw new Error(`Table ${tableId} not found`);
      }
      return table;
    }
  },

  getTableHealth: async (tableId: string): Promise<TableHealth> => {
    try {
      const response = await api.get(`/tables/${tableId}/health`);
      return response.data;
    } catch {
      // Mock data for now
      return {
        tableId,
        health: 'healthy',
        lag: Math.floor(Math.random() * 300),
        pendingRows: Math.floor(Math.random() * 1000),
        indexStatus: 'fresh',
        lastSyncTime: new Date().toISOString(),
      };
    }
  },

  getAllTableHealth: async (): Promise<TableHealth[]> => {
    try {
      const response = await api.get('/tables/health');
      return response.data;
    } catch {
      // Mock data for now
      const tables = await vectorSyncApi.getTables();
      return tables.map(t => ({
        tableId: t.tableId,
        health: ['healthy', 'lagging', 'broken'][Math.floor(Math.random() * 3)] as any,
        lag: Math.floor(Math.random() * 600),
        pendingRows: Math.floor(Math.random() * 5000),
        indexStatus: ['fresh', 'stale'][Math.floor(Math.random() * 2)] as any,
        lastSyncTime: new Date(Date.now() - Math.random() * 3600000).toISOString(),
      }));
    }
  },

  getTableDetails: async (tableId: string): Promise<TableDetails> => {
    try {
      const response = await api.get(`/tables/${tableId}/details`);
      return response.data;
    } catch {
      // Mock data for now
      const config = (await vectorSyncApi.getTables()).find(t => t.tableId === tableId)!;
      const health = await vectorSyncApi.getTableHealth(tableId);
      return {
        config,
        health,
        syncState: {
          tableId,
          lastSnapshotId: 12345,
          lastSyncTime: new Date().toISOString(),
          status: 'synced',
        },
        cdcTimeline: Array.from({ length: 10 }, (_, i) => ({
          snapshotId: 12340 + i,
          timestamp: new Date(Date.now() - i * 3600000).toISOString(),
          rowsChanged: Math.floor(Math.random() * 1000),
          operation: 'insert' as const,
          type: 'insert' as const,
          count: Math.floor(Math.random() * 1000),
          processed: i < 8,
        })),
        embeddingStats: {
          totalRows: 10000,
          embeddedRows: 9500,
          pendingRows: 450,
          failedRows: 50,
          coveragePercent: 95,
          throughput: 150,
          failures: 5,
          retries: 2,
          lastEmbeddingTime: new Date(Date.now() - 300000).toISOString(),
        },
        indexStats: {
          lastUpdatedSnapshot: 12345,
          vectorCount: 9500,
          freshnessScore: 98,
          indexType: 'hnsw',
          status: 'fresh' as const,
          dimension: 384,
          m: 16,
          efConstruction: 200,
          buildTime: 45,
          lastBuildTime: new Date(Date.now() - 600000).toISOString(),
        },
      };
    }
  },

  registerTable: async (config: Omit<TableConfig, 'tableId'>): Promise<TableConfig> => {
    const response = await api.post('/tables/register', config);
    return response.data;
  },

  deleteTable: async (tableId: string): Promise<void> => {
    await api.delete(`/tables/${tableId}`);
  },

  // Sync Operations
  getSyncStatus: async (): Promise<Record<string, unknown>> => {
    const response = await api.get('/sync/status');
    return response.data;
  },

  triggerSync: async (tableId?: string): Promise<Record<string, unknown>> => {
    const url = tableId ? `/sync/${tableId}` : '/demo/sync';
    const response = await api.post(url);
    return response.data;
  },

  // Vector Operations
  getVectorCount: async (): Promise<number> => {
    const response = await api.get('/worker-api/vectors/count');
    return response.data;
  },

  // Search
  search: async (
    query: string,
    sourceTable: string,
    topK: number = 5,
    useIndex: boolean = true
  ): Promise<SearchResponse> => {
    const response = await api.post('/search-api/search', {
      query,
      sourceTable,
      topK,
      useIndex,
    });
    return response.data;
  },

  // Jobs & Queue
  getJobs: async (status?: string): Promise<Job[]> => {
    try {
      const url = status ? `/jobs?status=${status}` : '/jobs';
      const response = await api.get(url);
      return response.data;
    } catch {
      // Mock data
      return Array.from({ length: 5 }, (_, i) => ({
        jobId: `job-${i + 1}`,
        tableId: `table-${i % 3}`,
        tableName: ['products', 'orders', 'logs'][i % 3],
        type: ['embedding', 'cdc', 'index'][i % 3] as any,
        status: ['running', 'queued', 'failed'][i % 3] as any,
        retries: i % 4,
        createdAt: new Date(Date.now() - i * 600000).toISOString(),
      }));
    }
  },

  retryJob: async (jobId: string): Promise<void> => {
    await api.post(`/jobs/${jobId}/retry`);
  },

  cancelJob: async (jobId: string): Promise<void> => {
    await api.post(`/jobs/${jobId}/cancel`);
  },

  // Alerts
  getAlerts: async (): Promise<Alert[]> => {
    try {
      const response = await api.get('/alerts');
      return response.data;
    } catch {
      // Mock data
      return [
        {
          id: 'alert-1',
          severity: 'warning',
          message: '12 tables lagging > 10 min',
          timestamp: new Date().toISOString(),
          acknowledged: false,
        },
        {
          id: 'alert-2',
          severity: 'critical',
          message: '3 tables index out of sync',
          timestamp: new Date().toISOString(),
          acknowledged: false,
        },
        {
          id: 'alert-3',
          severity: 'warning',
          message: 'High embedding failure rate (orders)',
          tableId: 'orders',
          timestamp: new Date().toISOString(),
          acknowledged: false,
        },
      ];
    }
  },

  acknowledgeAlert: async (alertId: string): Promise<void> => {
    await api.post(`/alerts/${alertId}/acknowledge`);
  },

  // Global Pipeline Stats
  getPipelineStats: async (): Promise<GlobalPipelineStats> => {
    try {
      const response = await api.get('/pipeline/stats');
      return response.data;
    } catch {
      // Mock data with all fields
      return {
        totalJobsQueued: 15,
        activeWorkers: 3,
        cpuUsage: 45,
        memoryUsage: 62,
        backlogSize: 1200,
        jobsInProgress: 8,
        jobsCompleted: 1247,
        throughput: 12.5,
        successRate: 97.8,
        avgProcessingTime: 4.2,
      };
    }
  },

  // Debug Operations
  getRowDebugInfo: async (tableId: string, rowId: string): Promise<any> => {
    try {
      const response = await api.get(`/debug/row/${tableId}/${rowId}`);
      return response.data;
    } catch {
      // Mock data for development
      const tables = await vectorSyncApi.getTables();
      const table = tables.find(t => t.tableId === tableId);
      
      return {
        rowId,
        tableId,
        tableName: table?.tableName || 'unknown',
        sourceData: {
          id: rowId,
          title: `Sample Product ${rowId}`,
          description: 'High-quality wireless headphones with active noise cancellation and premium sound quality',
          category: 'Electronics',
          price: 299.99,
          brand: 'TechBrand',
          rating: 4.5,
          stock: 150,
          created_at: '2024-01-15T10:30:00Z',
          updated_at: '2024-01-20T14:45:00Z',
        },
        embedding: Array.from({ length: 384 }, () => Math.random() * 2 - 1),
        embeddingDimension: 384,
        modelUsed: table?.modelName || 'sentence-transformers/all-MiniLM-L6-v2',
        createdAt: '2024-01-15T10:30:00Z',
        lastModified: '2024-01-20T14:45:00Z',
        cdcHistory: [
          {
            snapshotId: 12345,
            timestamp: '2024-01-15T10:30:00Z',
            operation: 'insert',
            afterValue: {
              id: rowId,
              title: `Sample Product ${rowId}`,
              description: 'High-quality wireless headphones',
              price: 299.99,
            },
          },
          {
            snapshotId: 12350,
            timestamp: '2024-01-20T14:45:00Z',
            operation: 'update',
            beforeValue: {
              description: 'High-quality wireless headphones',
              stock: 100,
            },
            afterValue: {
              description: 'High-quality wireless headphones with active noise cancellation and premium sound quality',
              stock: 150,
            },
          },
          {
            snapshotId: 12355,
            timestamp: '2024-01-22T09:15:00Z',
            operation: 'update',
            beforeValue: {
              rating: 4.3,
            },
            afterValue: {
              rating: 4.5,
            },
          },
        ],
        nearestNeighbors: [
          {
            rowId: 'prod-456',
            similarity: 0.95,
            text: 'Premium wireless earbuds with noise cancellation',
            distance: 0.05,
          },
          {
            rowId: 'prod-789',
            similarity: 0.88,
            text: 'Over-ear headphones with superior audio quality',
            distance: 0.12,
          },
          {
            rowId: 'prod-321',
            similarity: 0.82,
            text: 'Bluetooth headset with microphone',
            distance: 0.18,
          },
          {
            rowId: 'prod-654',
            similarity: 0.76,
            text: 'Studio monitor headphones for professionals',
            distance: 0.24,
          },
          {
            rowId: 'prod-987',
            similarity: 0.71,
            text: 'Gaming headset with RGB lighting',
            distance: 0.29,
          },
        ],
      };
    }
  },

  reembedRow: async (tableId: string, rowId: string): Promise<void> => {
    try {
      await api.post(`/debug/reembed/${tableId}/${rowId}`);
    } catch {
      // Mock - just simulate delay
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  },

  // Workers
  getWorkers: async (): Promise<any[]> => {
    try {
      const response = await api.get('/workers');
      return response.data;
    } catch {
      // Mock data
      return [
        {
          workerId: 'worker-1',
          status: 'active',
          cpuUsage: 45,
          memoryUsage: 62,
          currentJobs: 3,
          maxCapacity: 10,
          uptime: 86400, // 1 day in seconds
          lastHeartbeat: new Date().toISOString(),
        },
        {
          workerId: 'worker-2',
          status: 'active',
          cpuUsage: 38,
          memoryUsage: 55,
          currentJobs: 2,
          maxCapacity: 10,
          uptime: 43200, // 12 hours
          lastHeartbeat: new Date().toISOString(),
        },
        {
          workerId: 'worker-3',
          status: 'idle',
          cpuUsage: 12,
          memoryUsage: 28,
          currentJobs: 0,
          maxCapacity: 10,
          uptime: 7200, // 2 hours
          lastHeartbeat: new Date().toISOString(),
        },
      ];
    }
  },

  // Performance Metrics
  getPerformanceMetrics: async (): Promise<any> => {
    try {
      const response = await api.get('/metrics/performance');
      return response.data;
    } catch {
      // Mock data
      return {
        cdcLatency: {
          p50: 45,
          p95: 120,
          p99: 250,
          avg: 68,
        },
        embeddingLatency: {
          p50: 180,
          p95: 450,
          p99: 800,
          avg: 245,
        },
        indexLatency: {
          p50: 95,
          p95: 280,
          p99: 520,
          avg: 145,
        },
        errorRates: {
          cdcErrors: 3,
          embeddingErrors: 12,
          indexErrors: 5,
          totalErrors: 20,
          errorRate: 2.5,
        },
      };
    }
  },

  // System Metrics (Enhanced)
  getSystemMetrics: async (): Promise<SystemMetrics> => {
    try {
      const [syncStatus, vectorCount, healthData] = await Promise.all([
        api.get('/sync/status').catch(() => ({ data: { status: 'inactive' } })),
        api.get('/worker-api/vectors/count').catch(() => ({ data: 0 })),
        vectorSyncApi.getAllTableHealth().catch(() => []),
      ]);

      const tablesHealthy = healthData.filter(h => h.health === 'healthy').length;
      const tablesLagging = healthData.filter(h => h.health === 'lagging').length;
      const totalPendingRows = healthData.length > 0
        ? healthData.map(h => h.pendingRows).reduce((a, b) => a + b, 0)
        : 0;

      return {
        syncStatus: syncStatus.data.status || 'inactive',
        totalVectors: typeof vectorCount.data === 'number' ? vectorCount.data : 0,
        avgSyncLatency: 1.4,
        embeddingProvider: 'Mock Embedding Service',
        tablesHealthy,
        tablesTotal: healthData.length,
        tablesLagging,
        totalPendingRows,
        avgIndexFreshness: 95,
      };
    } catch (error) {
      console.error('Error fetching system metrics:', error);
      return {
        syncStatus: 'inactive',
        totalVectors: 0,
        avgSyncLatency: 0,
        embeddingProvider: 'Unknown',
        tablesHealthy: 0,
        tablesTotal: 0,
        tablesLagging: 0,
        totalPendingRows: 0,
        avgIndexFreshness: 0,
      };
    }
  },

  // Discovered Tables (from S3 sync)
  getDiscoveredTables: async (registered?: boolean): Promise<any[]> => {
    try {
      const url = registered !== undefined
        ? `/tables/discovered?registered=${registered}`
        : '/tables/discovered';
      const response = await api.get(url);
      return response.data;
    } catch (error) {
      console.error('Error fetching discovered tables:', error);
      return [];
    }
  },

  getDiscoveredTable: async (uuid: string): Promise<any> => {
    try {
      const response = await api.get(`/tables/discovered/${uuid}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching discovered table:', error);
      throw error;
    }
  },
};

// Made with Bob
