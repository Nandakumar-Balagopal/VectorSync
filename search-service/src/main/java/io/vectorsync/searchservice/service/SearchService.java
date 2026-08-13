package io.vectorsync.searchservice.service;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.SearchResult;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.common.util.CosineSimilarityUtil;
import io.vectorsync.searchservice.service.iceberg.VectorSyncReader;
import io.vectorsync.searchservice.service.index.HnswIndexService;
import io.vectorsync.searchservice.service.index.VectorSearchResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SearchService {

    private final QueryEmbeddingService embeddingClientService;
    private final VectorSyncReader vectorSyncReader;
    private final HnswIndexService hnswIndexService;
    
    @Value("${search.use-hnsw-index:true}")
    private boolean useHnswIndex;
    
    @Value("${search.index-rebuild-threshold:100}")
    private int indexRebuildThreshold;

    public SearchService(QueryEmbeddingService embeddingClientService,
                         VectorSyncReader vectorSyncReader,
                         HnswIndexService hnswIndexService) {
        this.embeddingClientService = embeddingClientService;
        this.vectorSyncReader = vectorSyncReader;
        this.hnswIndexService = hnswIndexService;
    }

    public List<SearchResult> search(String query, Integer topK, String sourceTable) throws Exception {
        int k = topK != null ? topK : Constants.DEFAULT_TOP_K;

        log.info("Searching for query: {}, topK: {}, sourceTable: {}, useHnsw: {}",
                 query, k, sourceTable, useHnswIndex);

        List<Double> queryEmbedding = embeddingClientService.generateEmbedding(query);
        
        // Decide whether to use HNSW index or brute-force search
        if (useHnswIndex) {
            return searchWithHnsw(queryEmbedding, k, sourceTable);
        } else {
            return searchBruteForce(queryEmbedding, k, sourceTable);
        }
    }
    
    /**
     * Search using HNSW index for efficient ANN search.
     * Automatically rebuilds index if not built or if vector count has changed significantly.
     */
    private List<SearchResult> searchWithHnsw(List<Double> queryEmbedding, int k, String sourceTable)
            throws Exception {
        long startTime = System.currentTimeMillis();
        
        // Check if index needs to be built or rebuilt
        List<VectorRecord> allVectors = vectorSyncReader.readAllVectors();
        int currentVectorCount = allVectors.size();
        
        if (!hnswIndexService.isIndexBuilt() ||
            currentVectorCount != hnswIndexService.getIndexedVectorCount() ||
            hnswIndexService.isStale(allVectors)) {
            
            log.info("Rebuilding HNSW index: current={}, indexed={}",
                     currentVectorCount, hnswIndexService.getIndexedVectorCount());
            hnswIndexService.buildIndex(allVectors);
        }
        
        // Search using HNSW index
        List<VectorSearchResult> indexResults = hnswIndexService.search(queryEmbedding, k, sourceTable);
        
        // Convert to SearchResult
        List<SearchResult> results = indexResults.stream()
                .map(r -> SearchResult.builder()
                        .vectorId(r.getVectorId())
                        .sourceTable(r.getSourceTable())
                        .sourceRowId(r.getSourceRowId())
                        .text(r.getText())
                        .similarity(r.getSimilarity())
                        .build())
                .collect(Collectors.toList());
        
        long duration = System.currentTimeMillis() - startTime;
        log.info("HNSW search returned {} results in {}ms from {} vectors",
                 results.size(), duration, currentVectorCount);
        
        return results;
    }
    
    /**
     * Brute-force search using cosine similarity.
     * Used as fallback or when HNSW is disabled.
     */
    private List<SearchResult> searchBruteForce(List<Double> queryEmbedding, int k, String sourceTable)
            throws Exception {
        long startTime = System.currentTimeMillis();
        
        List<VectorRecord> allVectors = vectorSyncReader.readAllVectors();
        log.debug("Retrieved {} vectors from Iceberg", allVectors.size());

        List<VectorRecord> relevantVectors = allVectors.stream()
            .filter(v -> sourceTable == null
                || sourceTable.isEmpty()
                || v.getSourceTable() == null
                || sourceTable.equals(v.getSourceTable()))
                .collect(Collectors.toList());

        List<SearchResult> results = relevantVectors.stream()
                .map(vectorRecord -> {
                    double similarity = CosineSimilarityUtil.cosineSimilarity(
                            queryEmbedding, vectorRecord.getEmbedding());
                    return SearchResult.builder()
                            .vectorId(vectorRecord.getVectorId())
                            .sourceTable(vectorRecord.getSourceTable())
                            .sourceRowId(vectorRecord.getSourceRowId())
                            .text(vectorRecord.getText())
                            .similarity(similarity)
                            .build();
                })
                .sorted((a, b) -> Double.compare(b.getSimilarity(), a.getSimilarity()))
                .limit(k)
                .collect(Collectors.toList());

        long duration = System.currentTimeMillis() - startTime;
        log.info("Brute-force search returned {} results in {}ms from {} candidates",
                 results.size(), duration, relevantVectors.size());

        return results;
    }
    
    /**
     * Manually trigger index rebuild.
     * Useful for forcing index refresh after bulk vector updates.
     */
    public void rebuildIndex() throws Exception {
        log.info("Manual index rebuild triggered");
        List<VectorRecord> allVectors = vectorSyncReader.readAllVectors();
        hnswIndexService.buildIndex(allVectors);
    }
    
    /**
     * Get index statistics.
     */
    public Map<String, Object> getIndexStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("indexBuilt", hnswIndexService.isIndexBuilt());
        stats.put("indexedVectorCount", hnswIndexService.getIndexedVectorCount());
        stats.put("useHnswIndex", useHnswIndex);
        stats.put("indexRebuildThreshold", indexRebuildThreshold);
        return stats;
    }
}
