package io.vectorsync.worker.controller;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.format.derive.ProjectionBuilder;
import io.vectorsync.format.derive.SqlViewGenerator;
import io.vectorsync.worker.service.derive.DeriveMetricsRegistry;
import io.vectorsync.worker.service.derive.DeriveOrchestrationService;
import io.vectorsync.worker.service.derive.ProjectionReader;
import io.vectorsync.worker.service.derive.ProvenanceService;
import io.vectorsync.worker.service.embedding.EmbeddingService;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
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
    private final EmbeddingService embeddings;
    private final IcebergCatalogService catalogService;
    private final ProvenanceService provenance;

    @org.springframework.beans.factory.annotation.Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public DeriveController(DeriveOrchestrationService orchestration,
                            DeriveMetricsRegistry metrics,
                            ProjectionReader projections,
                            EmbeddingService embeddings,
                            IcebergCatalogService catalogService,
                            ProvenanceService provenance) {
        this.orchestration = orchestration;
        this.metrics = metrics;
        this.projections = projections;
        this.embeddings = embeddings;
        this.catalogService = catalogService;
        this.provenance = provenance;
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
                                Long fromSnapshotExclusive,
                                /**
                                 * Publish Tier 2 after the pass, so the result is queryable when
                                 * this call returns. Absent means false, preserving the previous
                                 * behaviour: a pass that derives Tier 1 and leaves publishing to
                                 * the scheduler. Set it when the caller needs the round trip to be
                                 * "changed, then visible" -- which is what a freshness measurement
                                 * has to observe and what an operator driving a one-off pass
                                 * almost always wants.
                                 */
                                Boolean publishProjection) {
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
                    // Nested rather than another top-level key: Map.of caps at ten pairs and the
                    // outer map is already at it.
                    "metrics", Map.of(
                            "distinctHashes", result.distinctHashes(),
                            "cacheHits", result.cacheHits(),
                            "inferenceCalls", result.inferenceCalls(),
                            "inferenceAvoidedRate", result.inferenceAvoidedRate(),
                            "elapsedMillis", result.elapsedMillis(),
                            "projectionRows", publish(request, spec, result),
                            "complete", result.complete())));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Derive pass failed for {}: {}", request.sourceTable(), e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", "Derive failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /**
     * The engine-side SQL that makes a projection queryable, for Trino or Spark.
     *
     * <p>This closes the gap between what this project claims and what it hands you. The thesis is
     * that vectors are an open table queried by the engine you already run, with no VectorSync
     * process in the query path -- and until now {@code SqlViewGenerator} had zero callers, so the
     * generated DDL existed in the codebase and was reachable only by copying it out of a test.
     * Every benchmark and every document that shows a Trino query was pasting SQL by hand.
     *
     * <p>The dimension is read from the projection rather than taken as a parameter, because it is
     * the one value a caller cannot guess and must not get wrong: the emitted view carries a
     * {@code cardinality(...)} guard, and Trino's {@code cosine_similarity} returns NULL rather than
     * raising on a length mismatch, so a wrong width yields an all-NULL ranking that looks like a
     * working query returning bad results.
     *
     * @param qualifier how far to qualify the table name for the target session, e.g.
     *                  {@code iceberg.vectorsync}. Empty leaves it unqualified.
     */
    @GetMapping(value = "/view", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> view(@RequestParam String sourceTable,
                                       @RequestParam String configId,
                                       @RequestParam(defaultValue = "trino") String engine,
                                       @RequestParam(defaultValue = "") String qualifier) {
        int width = projections.dimension(sourceTable, configId);
        if (width <= 0) {
            return ResponseEntity.status(409).body(
                    "No published projection for " + sourceTable + " / " + configId
                            + ". Run a derivation pass before asking for its view.\n");
        }

        String table = ProjectionBuilder.tableName(sourceTable, configId);
        String qualified = qualifier == null || qualifier.isBlank()
                ? table
                : qualifier.trim() + "." + table;

        String normalised = engine == null ? "trino" : engine.trim().toLowerCase();
        String ddl = switch (normalised) {
            case "trino" -> SqlViewGenerator.trino(qualified, width);
            case "spark" -> SqlViewGenerator.spark(qualified, width);
            default -> null;
        };
        if (ddl == null) {
            return ResponseEntity.badRequest().body(
                    "Unknown engine '" + engine + "'; supported: trino, spark\n");
        }
        return ResponseEntity.ok(ddl);
    }

    /**
     * Publishes Tier 2 when asked, returning the rows now serving, or -1 when not asked.
     *
     * <p>Only on a complete pass. Publishing after a partial one would advertise coverage that was
     * never derived, which is the same reason the scheduler's publish requires a queue that drained
     * with zero failures.
     */
    private long publish(DeriveRequest request, MaterializationSpec spec,
                         DeriveOrchestrationService.PassResult result) {
        if (!Boolean.TRUE.equals(request.publishProjection()) || !result.complete()) {
            return -1;
        }
        return ProjectionBuilder.build(catalogService.getCatalog(), vectorNamespace, spec)
                .rowsWritten();
    }

    public record SearchRequest(String sourceTable, String configId, String query,
                                String modelName, Integer k) {
    }

    /**
     * Exact top-k over the serving projection.
     *
     * <p><b>This is a reference implementation and a measurement tool, not the serving story.</b>
     * The architecture's claim is that vectors are an open table queried by an engine you already
     * run, with no VectorSync process in the query path -- {@code GET /api/derive/view} emits that
     * SQL and is the intended route. This endpoint computes the same cosine in-process for two
     * narrower purposes: it isolates data cost from engine cost when benchmarking, and it gives the
     * mutation benchmark a latency number that does not require a Trino deployment.
     *
     * <p>Reading it as the product would be a mistake in the other direction: it scans the whole
     * projection for the scope, with no pruning, because its job is to be obviously correct rather
     * than fast. A query that matters should go through the generated view or the clustered index.
     */
    @PostMapping("/search")
    public ResponseEntity<?> search(@RequestBody SearchRequest request) {
        int limit = request.k() == null ? 10 : Math.max(1, request.k());
        String model = request.modelName() == null ? "all-MiniLM-L6-v2" : request.modelName();

        List<Double> query;
        try {
            // The spec's model, never the provider default. Embedding a query under a different
            // model than the rows puts it in a different vector space and the ranking becomes
            // noise -- a failure this project has hit twice, once in production lineage and once
            // in a test.
            query = embeddings.generateEmbedding(request.query(), model);
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "could not embed the query: " + e.getMessage()));
        }

        try {
            return ResponseEntity.ok(Map.of(
                    "sourceTable", request.sourceTable(),
                    "configId", request.configId(),
                    "k", limit,
                    "hits", projections.topK(
                            request.sourceTable(), request.configId(), query, limit)));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    /**
     * The derivation chain for one chunk: what it resolves to now, and every assertion that got it
     * there.
     *
     * <p>Reconstructed from the content map and the embedding store rather than from an audit log,
     * so it cannot disagree with what a query returns. A tombstoned chunk is a complete answer --
     * "retired at source sequence N" -- not a 404, because that is precisely the fact someone
     * investigating a disappeared result needs.
     */
    @GetMapping("/provenance")
    public ResponseEntity<?> provenance(@RequestParam String sourceTable,
                                        @RequestParam String configId,
                                        @RequestParam String sourceRowId,
                                        @RequestParam(defaultValue = "0") int chunkOrdinal) {
        try {
            return ResponseEntity.ok(
                    provenance.chainFor(sourceTable, configId, sourceRowId, chunkOrdinal));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }
}
