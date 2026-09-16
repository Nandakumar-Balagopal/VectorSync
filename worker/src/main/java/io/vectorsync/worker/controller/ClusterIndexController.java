package io.vectorsync.worker.controller;

import io.vectorsync.format.derive.ClusteredIndex;
import io.vectorsync.worker.service.derive.ClusterIndexService;
import io.vectorsync.worker.service.embedding.EmbeddingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds and probes the cluster-partitioned index.
 *
 * <p>An experiment with a hypothesis: that partitioning by nearest centroid lets a plain query
 * engine perform the candidate reduction an ANN index would, and that the resulting trade between
 * recall and fraction-of-table-read is good enough to be worth having while no engine can read an
 * Iceberg-native ANN index. The probe returns the rows it scanned precisely so that claim can be
 * checked rather than believed.
 */
@RestController
@RequestMapping("/api/cluster")
@Slf4j
public class ClusterIndexController {

    private final ClusterIndexService clusterIndex;
    private final EmbeddingService embeddingService;

    public ClusterIndexController(ClusterIndexService clusterIndex, EmbeddingService embeddingService) {
        this.clusterIndex = clusterIndex;
        this.embeddingService = embeddingService;
    }

    public record BuildRequest(String sourceTable, String modelVersion, String configId, Integer clusters) {
    }

    public record ProbeRequest(String sourceTable,
                               String modelVersion,
                               String configId,
                               String query,
                               Integer topK,
                               Integer probes,
                               Boolean includeExact) {
    }

    @PostMapping("/build")
    public ResponseEntity<?> build(@RequestBody BuildRequest request) {
        try {
            ClusterIndexService.BuildReport report = clusterIndex.build(
                    request.sourceTable(), request.modelVersion(), request.configId(),
                    request.clusters() == null ? 32 : request.clusters());
            return ResponseEntity.ok(report);
        } catch (IllegalStateException | IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Cluster build failed for {}: {}", request.sourceTable(), e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "build failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /**
     * Probes the index and, on request, the exhaustive scan alongside it.
     *
     * <p>Returning both in one response is deliberate: recall is only meaningful against the exact
     * answer for the same query and the same vector set, and computing it here removes any chance of
     * the two being taken from different states.
     */
    @PostMapping("/probe")
    public ResponseEntity<?> probe(@RequestBody ProbeRequest request) {
        try {
            // The query must be embedded by the model the index was built with; the model version is
            // part of the scope precisely so a caller cannot ask one space a question in another.
            String model = request.modelVersion() == null
                    ? null : request.modelVersion().split(":", 2)[0];
            float[] query = toFloats(embeddingService.generateEmbedding(request.query(), model));

            int topK = request.topK() == null ? 10 : request.topK();
            ClusteredIndex.ProbeResult result = clusterIndex.probe(
                    request.sourceTable(), request.modelVersion(), request.configId(),
                    query, topK, request.probes() == null ? 1 : request.probes());

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("probedClusters", result.probedClusters());
            body.put("rowsScanned", result.rowsScanned());
            body.put("totalRows", result.totalRows());
            body.put("fractionScanned", result.fractionScanned());
            body.put("candidates", summarise(result.candidates()));

            if (Boolean.TRUE.equals(request.includeExact())) {
                List<ClusteredIndex.Candidate> exact = clusterIndex.exact(
                        request.sourceTable(), request.modelVersion(), request.configId(), query, topK);
                body.put("exact", summarise(exact));

                List<String> probed = result.candidates().stream()
                        .map(ClusteredIndex.Candidate::contentHash).toList();
                long overlap = exact.stream()
                        .map(ClusteredIndex.Candidate::contentHash)
                        .filter(probed::contains)
                        .count();
                body.put("recallAtK", exact.isEmpty() ? 0.0 : (double) overlap / exact.size());
            }

            return ResponseEntity.ok(body);
        } catch (IllegalStateException | IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Cluster probe failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "probe failed", "message", String.valueOf(e.getMessage())));
        }
    }

    private static List<Map<String, Object>> summarise(List<ClusteredIndex.Candidate> candidates) {
        List<Map<String, Object>> out = new ArrayList<>(candidates.size());
        for (ClusteredIndex.Candidate candidate : candidates) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("contentHash", candidate.contentHash());
            row.put("clusterId", candidate.clusterId());
            row.put("similarity", candidate.similarity());
            if (candidate.text() != null) {
                row.put("text", candidate.text());
            }
            out.add(row);
        }
        return out;
    }

    private static float[] toFloats(List<Double> values) {
        float[] out = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            out[i] = values.get(i).floatValue();
        }
        return out;
    }
}
