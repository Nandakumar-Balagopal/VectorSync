package io.vectorsync.worker.controller;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.worker.service.derive.DeriveMetricsRegistry;
import io.vectorsync.worker.service.derive.DeriveOrchestrationService;
import io.vectorsync.worker.service.derive.ProjectionReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Runs a derivation pass for one materialization.
 *
 * <p>Takes the spec in the request rather than looking it up, which keeps the data plane free of
 * control-plane state and makes a pass reproducible from its inputs alone: the same spec over the
 * same snapshot must produce the same vectors, and that is easiest to guarantee when the spec is an
 * argument rather than something mutable fetched mid-run.
 */
@RestController
@RequestMapping("/api/derive")
@Slf4j
public class DeriveController {

    private final DeriveOrchestrationService orchestration;
    private final DeriveMetricsRegistry metrics;
    private final ProjectionReader projections;

    public DeriveController(DeriveOrchestrationService orchestration,
                            DeriveMetricsRegistry metrics,
                            ProjectionReader projections) {
        this.orchestration = orchestration;
        this.metrics = metrics;
        this.projections = projections;
    }

    /**
     * Derivation counters for one scope.
     *
     * <p>Exposed because the project's central claim -- inference tracks distinct content, not rows
     * -- was only ever observable by grepping logs, so it could be asserted but not verified or
     * alerted on.
     */
    @GetMapping("/metrics")
    public ResponseEntity<?> metrics(@RequestParam("sourceTable") String sourceTable,
                                     @RequestParam("configId") String configId) {
        return ResponseEntity.ok(metrics.snapshot(sourceTable, configId));
    }

    @GetMapping("/metrics/all")
    public ResponseEntity<?> allMetrics() {
        return ResponseEntity.ok(metrics.all());
    }

    /**
     * Samples the serving projection: the rows a query engine actually reads.
     *
     * <p>Deliberately excludes the embedding array. The point of looking at this table is the
     * lineage carried alongside each vector -- content hash, model version, source snapshot and
     * sequence number -- and returning several hundred floats per row over HTTP would bury it.
     */
    @GetMapping("/projection/sample")
    public ResponseEntity<?> sampleProjection(@RequestParam("sourceTable") String sourceTable,
                                              @RequestParam("configId") String configId,
                                              @RequestParam(value = "limit", defaultValue = "5") int limit) {
        try {
            List<Map<String, Object>> rows =
                    projections.sample(sourceTable, configId, Math.max(1, Math.min(limit, 100)));
            return ResponseEntity.ok(Map.of(
                    "sourceTable", sourceTable, "configId", configId,
                    "rows", rows, "sampled", rows.size()));
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Projection sample failed for {}: {}", sourceTable, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "sample failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /**
     * @param fromSnapshotExclusive when present, runs an incremental pass over files added since
     *        that snapshot; otherwise a full backfill over the table's current snapshot
     */
    public record DeriveRequest(String sourceTable,
                                List<String> keyColumns,
                                List<String> embeddingColumns,
                                String joinSeparator,
                                String chunker,
                                Integer chunkSize,
                                Integer chunkOverlap,
                                String modelName,
                                String modelRevision,
                                String embeddingVersion,
                                Boolean normalize,
                                Long fromSnapshotExclusive) {
    }

    @PostMapping("/run")
    public ResponseEntity<?> run(@RequestBody DeriveRequest request) {
        MaterializationSpec spec;
        try {
            spec = MaterializationSpec.builder()
                    .sourceTable(request.sourceTable())
                    .keyColumns(request.keyColumns())
                    .embeddingColumns(request.embeddingColumns())
                    .joinSeparator(request.joinSeparator())
                    .chunker(request.chunker())
                    .chunkSize(request.chunkSize() == null ? 0 : request.chunkSize())
                    .chunkOverlap(request.chunkOverlap() == null ? 0 : request.chunkOverlap())
                    .modelName(request.modelName())
                    .modelRevision(request.modelRevision())
                    .embeddingVersion(request.embeddingVersion())
                    .normalize(Boolean.TRUE.equals(request.normalize()))
                    .build();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        }

        TableConfig config = TableConfig.builder()
                .tableName(request.sourceTable())
                .embeddingColumns(request.embeddingColumns())
                .modelName(request.modelName())
                .embeddingVersion(spec.getEmbeddingVersion())
                .enabled(true)
                .build();

        try {
            long startedAt = System.currentTimeMillis();
            DeriveOrchestrationService.PassResult result = request.fromSnapshotExclusive() == null
                    ? orchestration.backfill(spec, config, startedAt)
                    : orchestration.incremental(spec, config, request.fromSnapshotExclusive(), startedAt);

            return ResponseEntity.ok(Map.of(
                    "sourceTable", result.sourceTable(),
                    "configId", result.configId(),
                    "modelVersion", result.modelVersion(),
                    "snapshotId", result.snapshotId(),
                    "sequenceNumber", result.sequenceNumber(),
                    "filesProcessed", result.filesProcessed(),
                    "filesFailed", result.filesFailed(),
                    "rowsProcessed", result.rowsProcessed(),
                    "chunksProcessed", result.chunksProcessed(),
                    "metrics", Map.of(
                            "distinctHashes", result.distinctHashes(),
                            "cacheHits", result.cacheHits(),
                            "inferenceCalls", result.inferenceCalls(),
                            "inferenceAvoidedRate", result.inferenceAvoidedRate(),
                            "elapsedMillis", result.elapsedMillis(),
                            "complete", result.complete())));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Derive pass failed for {}: {}", request.sourceTable(), e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", "Derive failed", "message", String.valueOf(e.getMessage())));
        }
    }
}
