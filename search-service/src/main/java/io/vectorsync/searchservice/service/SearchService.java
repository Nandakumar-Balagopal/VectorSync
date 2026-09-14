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

        Optional<IndexManifestEntry> promoted = sourceTable == null || sourceTable.isBlank()
                ? Optional.empty()
                : registry.promotedIndex(sourceTable);

        if (promoted.isPresent()) {
            // The query is embedded by the promoted index's own model, so the two vector spaces
            // match. Embedding first and resolving the index afterwards would silently compare
            // across models.
            IndexManifestEntry entry = promoted.get();
            return searchIndex(entry, embedQuery(query, entry.getEmbeddingModel()), k);
        }

        // Nothing promoted: scope the exhaustive scan to the table's newest model version, so one
        // query embedding is comparable with every candidate.
        String modelVersion = vectorSyncReader.modelVersionsFor(sourceTable).stream()
                .reduce((first, second) -> second)
                .orElse(null);

        log.info("No promoted index for {}; exact search scoped to {}", sourceTable, modelVersion);
        return searchExact(query, k, sourceTable, modelVersion);
    }

    /** Splits a "model:version" pair down to its model, for embedding a query. */
    private static String modelOf(String modelVersion) {
        if (modelVersion == null) {
            return null;
        }
        int separator = modelVersion.lastIndexOf(':');
        return separator <= 0 ? modelVersion : modelVersion.substring(0, separator);
    }

    private List<Double> embedQuery(String query, String model) throws Exception {
        return embeddingClientService.generateEmbedding(query, model);
    }

    /** Searches a specific index version, which is how a candidate is evaluated before promotion. */
    public List<SearchResult> searchIndexId(String indexId, String query, int k) throws Exception {
        IndexManifestEntry entry = requireIndex(indexId);
        return searchIndex(entry, embedQuery(query, entry.getEmbeddingModel()), k);
    }

    /**
     * Searches a specific index with a vector that is already in that index's embedding space.
     *
     * <p>Used by label-free index-recall measurement, which probes with vectors sampled from the
     * index's own partition. Taking the vector directly avoids an embedding round trip and, more
     * importantly, removes any chance of probing with the wrong model.
     */
    public List<SearchResult> searchIndexWithEmbedding(String indexId, List<Double> queryEmbedding, int k)
            throws Exception {
        return searchIndex(requireIndex(indexId), queryEmbedding, k);
    }

    private IndexManifestEntry requireIndex(String indexId) {
        return registry.manifest().findById(indexId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown index: " + indexId));
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
     *
     * @param modelVersion when set, restricts the scan to one embedding version. Leaving it null
     *        scans every version materialized for the table, which mixes embedding spaces: a query
     *        embedded with one model scored against vectors from another is meaningless, and the
     *        same row appears once per version. Always scope it when comparing against an index.
     */
    public List<SearchResult> searchExact(List<Double> queryEmbedding,
                                          int k,
                                          String sourceTable,
                                          String modelVersion) {
        long startTime = System.currentTimeMillis();

        // Scoped reads push both predicates into Iceberg and prune partitions. The unscoped branch
        // remains only for the degenerate case of a caller with no table at all; it is a full scan
        // and mixes embedding spaces, which is why every caller now resolves a scope first.
        boolean scoped = sourceTable != null && !sourceTable.isBlank()
                && modelVersion != null && !modelVersion.isBlank();

        List<VectorRecord> candidates = scoped
                ? vectorSyncReader.readForIndex(sourceTable, modelVersion)
                : vectorSyncReader.readAllVectors().stream()
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

        log.info("Exact search over {} returned {} of {} candidates in {}ms",
                modelVersion == null ? "all versions" : modelVersion,
                results.size(), candidates.size(), System.currentTimeMillis() - startTime);
        return results;
    }

    public List<SearchResult> searchExact(String query, int k, String sourceTable, String modelVersion)
            throws Exception {
        // Embedded with the model of the version being scanned, for the same reason.
        String scoped = resolveScope(sourceTable, modelVersion);
        return searchExact(embedQuery(query, modelOf(scoped)), k, sourceTable, scoped);
    }

    /**
     * Picks the single embedding version an exhaustive scan will run against.
     *
     * <p>An unscoped scan is not merely imprecise, it is meaningless: the query gets embedded by
     * whatever the default model is and then scored against vectors from every materialized
     * version, so most similarities compare two different embedding spaces and the same row appears
     * once per version. Callers that leave the version out are asking for "the table's current
     * answer", which is what the promoted index defines, so resolve it the same way
     * {@link #search} does rather than scanning everything.
     */
    public String resolveScope(String sourceTable, String requestedModelVersion) {
        if (requestedModelVersion != null && !requestedModelVersion.isBlank()) {
            return requestedModelVersion;
        }
        if (sourceTable == null || sourceTable.isBlank()) {
            return null;
        }

        Optional<IndexManifestEntry> promoted = registry.promotedIndex(sourceTable);
        if (promoted.isPresent()) {
            return promoted.get().modelVersion();
        }

        String newest = vectorSyncReader.modelVersionsFor(sourceTable).stream()
                .reduce((first, second) -> second)
                .orElse(null);
        log.info("Exact search on {} was unscoped; resolved to {}", sourceTable, newest);
        return newest;
    }

    public Map<String, Object> getIndexStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("formatVersion", registry.formatVersion());
        stats.put("openArtifacts", indexCache.openCount());
        stats.put("indexBaseUri", registry.baseUri());
        return stats;
    }
}
