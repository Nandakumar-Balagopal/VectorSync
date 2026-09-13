package io.vectorsync.searchservice.service;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.SearchResult;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.common.util.CosineSimilarityUtil;
import io.vectorsync.format.index.IndexManifestEntry;
import io.vectorsync.searchservice.service.iceberg.VectorSyncReader;
import io.vectorsync.searchservice.service.index.HnswIndexArtifact;
import io.vectorsync.searchservice.service.index.IndexFields;
import io.vectorsync.searchservice.service.index.HnswIndexCache;
import io.vectorsync.searchservice.service.index.IndexRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Serves queries through whichever index the {@code production} alias resolves to.
 *
 * <p>Previously this read the entire vector table on every query and rebuilt an in-memory index
 * whenever a content signature changed, so the index saved similarity arithmetic but no I/O.
 * Serving now resolves an alias, opens a durable artifact once, and keeps it.
 */
@Service
@Slf4j
public class SearchService {

    private final QueryEmbeddingService embeddingClientService;
    private final VectorSyncReader vectorSyncReader;
    private final IndexRegistry registry;
    private final HnswIndexCache indexCache;

    public SearchService(QueryEmbeddingService embeddingClientService,
                         VectorSyncReader vectorSyncReader,
                         IndexRegistry registry,
                         HnswIndexCache indexCache) {
        this.embeddingClientService = embeddingClientService;
        this.vectorSyncReader = vectorSyncReader;
        this.registry = registry;
        this.indexCache = indexCache;
    }

    public List<SearchResult> search(String query, Integer topK, String sourceTable) throws Exception {
        int k = topK != null ? topK : Constants.DEFAULT_TOP_K;
        List<Double> queryEmbedding = embeddingClientService.generateEmbedding(query);

        Optional<IndexManifestEntry> promoted = sourceTable == null || sourceTable.isBlank()
                ? Optional.empty()
                : registry.promotedIndex(sourceTable);

        if (promoted.isPresent()) {
            return searchIndex(promoted.get(), queryEmbedding, k);
        }

        log.info("No promoted index for {}; falling back to exact search", sourceTable);
        return searchExact(queryEmbedding, k, sourceTable);
    }

    /** Searches a specific index version, which is how a candidate is evaluated before promotion. */
    public List<SearchResult> searchIndexId(String indexId, String query, int k) throws Exception {
        IndexManifestEntry entry = registry.manifest().findById(indexId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown index: " + indexId));

        return searchIndex(entry, embeddingClientService.generateEmbedding(query), k);
    }

    private List<SearchResult> searchIndex(IndexManifestEntry entry, List<Double> queryEmbedding, int k)
            throws Exception {
        long startTime = System.currentTimeMillis();
        HnswIndexArtifact artifact = indexCache.get(entry);

        float[] target = new float[queryEmbedding.size()];
        for (int i = 0; i < queryEmbedding.size(); i++) {
            target[i] = queryEmbedding.get(i).floatValue();
        }

        // No source-table filter is applied or needed: an index covers exactly one
        // (source_table, model_version), so the artifact is itself the filter. The previous
        // implementation post-filtered after top-k and could silently return fewer than k.
        TopDocs topDocs = artifact.searcher().search(new KnnFloatVectorQuery(
                IndexFields.VECTOR, target, k), k);

        List<SearchResult> results = new java.util.ArrayList<>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Document document = artifact.searcher().storedFields().document(scoreDoc.doc);
            results.add(SearchResult.builder()
                    .vectorId(document.get(IndexFields.VECTOR_ID))
                    .sourceTable(document.get(IndexFields.SOURCE_TABLE))
                    .sourceRowId(document.get(IndexFields.SOURCE_ROW_ID))
                    .text(document.get(IndexFields.TEXT))
                    .similarity(scoreDoc.score)
                    .build());
        }

        log.info("Index {} returned {} results in {}ms",
                entry.getIndexId(), results.size(), System.currentTimeMillis() - startTime);
        return results;
    }

    /**
     * Exhaustive cosine scan. Slower, but exact, so it doubles as the ground truth that index
     * recall is measured against.
     */
    public List<SearchResult> searchExact(List<Double> queryEmbedding, int k, String sourceTable) {
        long startTime = System.currentTimeMillis();

        List<VectorRecord> candidates = vectorSyncReader.readAllVectors().stream()
                .filter(vector -> sourceTable == null
                        || sourceTable.isBlank()
                        || sourceTable.equals(vector.getSourceTable()))
                .filter(vector -> vector.getEmbedding() != null && !vector.getEmbedding().isEmpty())
                .toList();

        List<SearchResult> results = candidates.stream()
                .map(vector -> SearchResult.builder()
                        .vectorId(vector.getVectorId())
                        .sourceTable(vector.getSourceTable())
                        .sourceRowId(vector.getSourceRowId())
                        .text(vector.getText())
                        .similarity(CosineSimilarityUtil.cosineSimilarity(queryEmbedding, vector.getEmbedding()))
                        .build())
                .sorted((a, b) -> Double.compare(b.getSimilarity(), a.getSimilarity()))
                .limit(k)
                .toList();

        log.info("Exact search returned {} of {} candidates in {}ms",
                results.size(), candidates.size(), System.currentTimeMillis() - startTime);
        return results;
    }

    public List<SearchResult> searchExact(String query, int k, String sourceTable) throws Exception {
        return searchExact(embeddingClientService.generateEmbedding(query), k, sourceTable);
    }

    public Map<String, Object> getIndexStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("formatVersion", registry.formatVersion());
        stats.put("openArtifacts", indexCache.openCount());
        stats.put("indexBaseUri", registry.baseUri());
        return stats;
    }
}
