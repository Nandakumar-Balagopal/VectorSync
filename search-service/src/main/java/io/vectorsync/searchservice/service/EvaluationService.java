package io.vectorsync.searchservice.service;

import io.vectorsync.common.dto.SearchResult;
import io.vectorsync.format.index.IndexManifestEntry;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.searchservice.service.iceberg.VectorSyncReader;
import io.vectorsync.searchservice.service.index.IndexRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Measures index quality and records the result on the manifest entry.
 *
 * <p>Two different things get conflated as "recall", and they are kept separate here because they
 * have different authority:
 *
 * <ul>
 *   <li><b>Index recall</b> — how much of the exact nearest-neighbour set the approximate index
 *       returns. Ground truth is an exhaustive cosine scan over the same partition, so this needs
 *       no external input at all and can be run on any index at any time. It evaluates the
 *       <em>index</em>: whether the HNSW graph and its build parameters lost anything.
 *   <li><b>Precision</b> — whether the returned rows are the right ones. Requires judged relevance
 *       supplied by whoever owns the table. It evaluates the <em>embedding model</em>, which is a
 *       choice made upstream of this system.
 * </ul>
 *
 * <p>The distinction matters most during a model migration. Two indexes built from two different
 * models will both score near-perfect index recall, because each approximates its own embedding
 * space faithfully; only precision against a fixture can tell you that one of them retrieves worse
 * documents. Conversely, precision cannot detect a badly built graph, because a graph that returns
 * plausible-but-not-nearest neighbours still returns topically related rows.
 *
 * <p>Consumers here are query engines, not people, so there is no click-through signal to fall back
 * on: whatever is checked before promotion is the only check that ever happens.
 */
@Service
@Slf4j
public class EvaluationService {

    /** Probes used when a caller does not specify. Enough to be stable, small enough to be quick. */
    private static final int DEFAULT_PROBE_COUNT = 20;

    private final SearchService searchService;
    private final VectorSyncReader vectorSyncReader;
    private final IndexRegistry registry;

    public EvaluationService(SearchService searchService,
                             VectorSyncReader vectorSyncReader,
                             IndexRegistry registry) {
        this.searchService = searchService;
        this.vectorSyncReader = vectorSyncReader;
        this.registry = registry;
    }

    public record QueryJudgement(String query, List<String> relevantSourceRowIds) {
    }

    /**
     * @param probeCount         probes used for label-free recall; 0 when recall came from
     *                           caller-supplied queries instead
     * @param queryCount         caller-supplied queries; 0 for a label-free run
     * @param precisionAtK       null unless at least one query carried relevance labels
     * @param fixtureRef         caller's identifier for the ground truth behind {@code precisionAtK}
     * @param precisionPersisted whether the precision score was written to the manifest; false when
     *                           no {@code fixtureRef} was supplied, because an unattributable score
     *                           is not evidence
     * @param perProbeRecall     recall per probe, keyed by probe vector id or by query text
     */
    public record EvaluationReport(String indexId,
                                   int k,
                                   int probeCount,
                                   int queryCount,
                                   double indexRecallAtK,
                                   Double precisionAtK,
                                   String fixtureRef,
                                   boolean precisionPersisted,
                                   Map<String, Double> perProbeRecall) {
    }

    /**
     * Label-free index recall, with probes sampled from the index's own partition.
     *
     * <p>Self-sourcing the probes is what makes this runnable with no input: "does this graph return
     * what an exhaustive scan returns" has a ground truth that does not involve a human. Sampling is
     * deterministic — evenly spaced over vector-id order — so re-running on an unchanged index
     * reproduces the same number, which is a precondition for treating it as a gate.
     */
    public EvaluationReport evaluateIndexRecall(String indexId, int k, Integer probeCount) throws Exception {
        IndexManifestEntry entry = requireEntry(indexId);
        // Sampling happens inside the reader so only the sampled rows have their embeddings read;
        // pulling the whole partition to choose twenty probes defeats the point of a cheap check.
        List<VectorRecord> probes = vectorSyncReader.sampleLiveVectors(
                entry.getSourceTable(),
                entry.modelVersion(),
                probeCount == null || probeCount <= 0 ? DEFAULT_PROBE_COUNT : probeCount);
        if (probes.isEmpty()) {
            throw new IllegalStateException("No vectors materialized for "
                    + entry.getSourceTable() + " at " + entry.modelVersion()
                    + "; nothing to measure recall against");
        }

        Map<String, Double> perProbe = new LinkedHashMap<>();
        double recallSum = 0.0;
        for (VectorRecord probe : probes) {
            // Identity by vector id, not row id: recall asks whether the graph found the same
            // vectors, and one row can contribute several chunks.
            Set<String> exact = ids(searchService.searchExact(
                    probe.getEmbedding(), k, entry.getSourceTable(), entry.modelVersion()),
                    SearchResult::getVectorId);
            Set<String> approximate = ids(searchService.searchIndexWithEmbedding(
                    indexId, probe.getEmbedding(), k), SearchResult::getVectorId);

            double recall = exact.isEmpty() ? 1.0 : (double) intersectionSize(exact, approximate) / exact.size();
            perProbe.put(probe.getVectorId(), recall);
            recallSum += recall;
        }

        EvaluationReport report = new EvaluationReport(
                indexId, k, probes.size(), 0, recallSum / probes.size(),
                null, null, false, perProbe);

        recordOnManifest(entry, report);
        log.info("Index {} scored index_recall@{}={} over {} probes",
                indexId, k, report.indexRecallAtK(), probes.size());
        return report;
    }

    /**
     * Evaluates an index against caller-supplied queries, computing index recall from those queries
     * and precision from whatever relevance labels they carry.
     *
     * <p>Precision is written to the manifest only when {@code fixtureRef} names the ground truth it
     * came from. A bare score on an artifact looks like audit evidence while being impossible to
     * reproduce or challenge — nobody can later tell whether the queries behind it were
     * representative — which is the same defect this system exists to fix for embeddings.
     *
     * @param fixtureRef caller's stable identifier for the judged query set, ideally addressing a
     *                   snapshot (for example {@code default.product_judgements@8137...})
     */
    public EvaluationReport evaluate(String indexId,
                                     List<QueryJudgement> judgements,
                                     int k,
                                     String fixtureRef) throws Exception {
        IndexManifestEntry entry = requireEntry(indexId);

        Map<String, Double> perQueryRecall = new LinkedHashMap<>();
        double recallSum = 0.0;
        double precisionSum = 0.0;
        int judged = 0;

        for (QueryJudgement judgement : judgements) {
            // Scoped to the index's own version; otherwise recall is measured against a
            // mixed-version population and is understated.
            Set<String> exact = ids(searchService.searchExact(
                    judgement.query(), k, entry.getSourceTable(), entry.modelVersion()),
                    SearchResult::getVectorId);
            List<SearchResult> approximateResults = searchService.searchIndexId(indexId, judgement.query(), k);
            Set<String> approximate = ids(approximateResults, SearchResult::getVectorId);

            double recall = exact.isEmpty()
                    ? 1.0
                    : (double) intersectionSize(exact, approximate) / exact.size();
            perQueryRecall.put(judgement.query(), recall);
            recallSum += recall;

            if (judgement.relevantSourceRowIds() != null && !judgement.relevantSourceRowIds().isEmpty()) {
                // Labels are row ids, so precision is scored on row identity rather than vector id.
                Set<String> retrievedRows = ids(approximateResults, SearchResult::getSourceRowId);
                Set<String> relevant = Set.copyOf(judgement.relevantSourceRowIds());
                int denominator = Math.min(k, Math.max(retrievedRows.size(), 1));
                precisionSum += (double) intersectionSize(relevant, retrievedRows) / denominator;
                judged++;
            }
        }

        int queryCount = judgements.size();
        Double precision = judged == 0 ? null : precisionSum / judged;
        boolean attributable = precision != null && fixtureRef != null && !fixtureRef.isBlank();

        EvaluationReport report = new EvaluationReport(
                indexId, k, 0, queryCount,
                queryCount == 0 ? 0.0 : recallSum / queryCount,
                precision,
                fixtureRef == null || fixtureRef.isBlank() ? null : fixtureRef,
                attributable,
                perQueryRecall);

        recordOnManifest(entry, report);
        if (precision != null && !attributable) {
            log.warn("Index {} scored precision@{}={} but no fixtureRef was supplied; "
                            + "returning it without recording it on the manifest",
                    indexId, k, precision);
        }
        log.info("Evaluated index {}: indexRecall@{}={} precision@{}={} over {} queries",
                indexId, k, report.indexRecallAtK(), k, precision, queryCount);

        return report;
    }

    private IndexManifestEntry requireEntry(String indexId) {
        return registry.manifest().findById(indexId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown index: " + indexId));
    }

    private void recordOnManifest(IndexManifestEntry entry, EvaluationReport report) {
        Map<String, String> metrics = entry.getEvalMetrics() == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(entry.getEvalMetrics());

        // Both paths write index_recall@k, so the entry also records which probes produced it.
        // Without that, an index evaluated both ways carries a recall score sitting between a
        // probe count and a query count with nothing to say which one it came from.
        metrics.put("index_recall@" + report.k(), String.format("%.4f", report.indexRecallAtK()));
        if (report.probeCount() > 0) {
            metrics.put("recall_source", "sampled_probes");
            metrics.put("recall_probe_count", String.valueOf(report.probeCount()));
        } else {
            metrics.put("recall_source", "supplied_queries");
            metrics.put("eval_query_count", String.valueOf(report.queryCount()));
        }

        if (report.precisionPersisted()) {
            metrics.put("precision@" + report.k(), String.format("%.4f", report.precisionAtK()));
            metrics.put("eval_fixture", report.fixtureRef());
        }

        entry.setEvalMetrics(metrics);
        registry.manifest().put(entry);
    }

    private static Set<String> ids(List<SearchResult> results, Function<SearchResult, String> key) {
        return results.stream()
                .map(key)
                .filter(id -> id != null && !id.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static int intersectionSize(Set<String> a, Set<String> b) {
        return (int) a.stream().filter(b::contains).count();
    }
}
