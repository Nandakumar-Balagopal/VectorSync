package io.vectorsync.searchservice.service;

import io.vectorsync.common.dto.SearchResult;
import io.vectorsync.format.index.IndexManifestEntry;
import io.vectorsync.searchservice.service.index.IndexRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Measures index quality and records the result on the manifest entry.
 *
 * <p>Two different things get conflated as "recall", and they are kept separate here:
 *
 * <ul>
 *   <li><b>Index recall</b> — how much of the exact nearest-neighbour set the approximate index
 *       returns. Measured against an exhaustive cosine scan, needs no human labels, and evaluates
 *       the <em>index</em>.
 *   <li><b>Retrieval quality</b> — whether the returned documents are the right ones. Requires
 *       judged relevance, and evaluates the <em>embedding model</em>.
 * </ul>
 *
 * Reporting one as the other produces misleading promotion decisions: a new model can have perfect
 * index recall while retrieving worse documents.
 */
@Service
@Slf4j
public class EvaluationService {

    private final SearchService searchService;
    private final IndexRegistry registry;

    public EvaluationService(SearchService searchService, IndexRegistry registry) {
        this.searchService = searchService;
        this.registry = registry;
    }

    public record QueryJudgement(String query, List<String> relevantSourceRowIds) {
    }

    public record EvaluationReport(String indexId,
                                   int queryCount,
                                   int k,
                                   double indexRecallAtK,
                                   Double precisionAtK,
                                   Map<String, Double> perQueryIndexRecall) {
    }

    /**
     * Evaluates an index and writes the metrics back onto its manifest entry, so the numbers a
     * promotion decision was based on stay attached to the artifact.
     *
     * @param judgements relevance labels; pass empty {@code relevantSourceRowIds} to measure index
     *                   recall only
     */
    public EvaluationReport evaluate(String indexId, List<QueryJudgement> judgements, int k) throws Exception {
        IndexManifestEntry entry = registry.manifest().findById(indexId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown index: " + indexId));

        Map<String, Double> perQueryRecall = new LinkedHashMap<>();
        double recallSum = 0.0;
        double precisionSum = 0.0;
        int judged = 0;

        for (QueryJudgement judgement : judgements) {
            Set<String> exact = rowIds(searchService.searchExact(judgement.query(), k, entry.getSourceTable()));
            Set<String> approximate = rowIds(searchService.searchIndexId(indexId, judgement.query(), k));

            double recall = exact.isEmpty()
                    ? 1.0
                    : (double) intersectionSize(exact, approximate) / exact.size();
            perQueryRecall.put(judgement.query(), recall);
            recallSum += recall;

            if (judgement.relevantSourceRowIds() != null && !judgement.relevantSourceRowIds().isEmpty()) {
                Set<String> relevant = Set.copyOf(judgement.relevantSourceRowIds());
                precisionSum += (double) intersectionSize(relevant, approximate) / Math.min(k, approximate.size() == 0 ? 1 : approximate.size());
                judged++;
            }
        }

        int queryCount = judgements.size();
        double indexRecall = queryCount == 0 ? 0.0 : recallSum / queryCount;
        Double precision = judged == 0 ? null : precisionSum / judged;

        EvaluationReport report = new EvaluationReport(
                indexId, queryCount, k, indexRecall, precision, perQueryRecall);

        recordOnManifest(entry, report);
        log.info("Evaluated index {}: indexRecall@{}={} precision@{}={} over {} queries",
                indexId, k, indexRecall, k, precision, queryCount);

        return report;
    }

    private void recordOnManifest(IndexManifestEntry entry, EvaluationReport report) {
        Map<String, String> metrics = entry.getEvalMetrics() == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(entry.getEvalMetrics());

        metrics.put("index_recall@" + report.k(), String.format("%.4f", report.indexRecallAtK()));
        metrics.put("eval_query_count", String.valueOf(report.queryCount()));
        if (report.precisionAtK() != null) {
            metrics.put("precision@" + report.k(), String.format("%.4f", report.precisionAtK()));
        }

        entry.setEvalMetrics(metrics);
        registry.manifest().put(entry);
    }

    private static Set<String> rowIds(List<SearchResult> results) {
        return results.stream()
                .map(SearchResult::getSourceRowId)
                .filter(id -> id != null && !id.isBlank())
                .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
    }

    private static int intersectionSize(Set<String> a, Set<String> b) {
        return (int) a.stream().filter(b::contains).count();
    }
}
