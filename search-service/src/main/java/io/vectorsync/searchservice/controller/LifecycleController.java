package io.vectorsync.searchservice.controller;

import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.index.IndexAliasEntry;
import io.vectorsync.format.index.IndexAliasStore;
import io.vectorsync.format.index.IndexManifestEntry;
import io.vectorsync.format.index.IndexVector;
import io.vectorsync.searchservice.service.EvaluationService;
import io.vectorsync.searchservice.service.iceberg.VectorSyncReader;
import io.vectorsync.searchservice.service.index.HnswIndexBuilder;
import io.vectorsync.searchservice.service.index.HnswIndexCache;
import io.vectorsync.searchservice.service.index.IndexRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The embedding/index lifecycle: build, evaluate, promote, roll back, inspect.
 *
 * <p>Deliberately explicit rather than implicit. Nothing here happens as a side effect of a query,
 * which is what makes a promotion an auditable decision instead of an emergent behaviour.
 */
@RestController
@RequestMapping("/api/lifecycle")
@Slf4j
public class LifecycleController {

    private final IndexRegistry registry;
    private final HnswIndexBuilder builder;
    private final HnswIndexCache indexCache;
    private final VectorSyncReader vectorSyncReader;
    private final EvaluationService evaluationService;

    /**
     * Minimum label-free index recall for an index to be promotable without an override. Not 1.0:
     * HNSW is approximate by design and a healthy graph routinely lands slightly under.
     */
    @Value("${lifecycle.promotion.min-index-recall:0.95}")
    private double minIndexRecall;

    public LifecycleController(IndexRegistry registry,
                               HnswIndexBuilder builder,
                               HnswIndexCache indexCache,
                               VectorSyncReader vectorSyncReader,
                               EvaluationService evaluationService) {
        this.registry = registry;
        this.builder = builder;
        this.indexCache = indexCache;
        this.vectorSyncReader = vectorSyncReader;
        this.evaluationService = evaluationService;
    }

    /**
     * @param asOfSequenceNumber optional Iceberg snapshot sequence number to build as of. A
     *        sequence number rather than a snapshot id because only sequence numbers are ordered.
     */
    public record BuildRequest(String sourceTable, String modelVersion, Long asOfSequenceNumber) {
    }

    /**
     * @param force skips the promotion checks. Requires a note; the override and the blockers it
     *              bypassed are written into the alias log.
     */
    public record PromoteRequest(String sourceTable,
                                 String indexId,
                                 String promotedBy,
                                 String note,
                                 Boolean force) {
    }

    /**
     * @param queries    optional. Omitted or empty means label-free index recall, with probes
     *                   sampled from the index's own partition.
     * @param probeCount optional probe count for the label-free path.
     * @param fixtureRef identifier for the judged query set behind any relevance labels. Required
     *                   for precision to be recorded on the manifest; see {@link EvaluationService}.
     */
    public record EvaluateRequest(String indexId,
                                  int topK,
                                  Integer probeCount,
                                  String fixtureRef,
                                  List<QueryJudgementRequest> queries) {
    }

    public record QueryJudgementRequest(String query, List<String> relevantSourceRowIds) {
    }

    /** Materializes an index over one (source table, model version) pair. */
    @PostMapping("/index/build")
    public ResponseEntity<?> build(@RequestBody BuildRequest request) {
        if (isBlank(request.sourceTable()) || isBlank(request.modelVersion())) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "sourceTable and modelVersion are required"));
        }

        String[] parts = request.modelVersion().split(":", 2);
        if (parts.length != 2) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "modelVersion must be in 'model:version' form"));
        }

        List<IndexVector> vectors = request.asOfSequenceNumber() != null
                ? vectorSyncReader.readForIndexBuildAsOf(
                        request.sourceTable(), request.modelVersion(), request.asOfSequenceNumber())
                : vectorSyncReader.readForIndexBuild(request.sourceTable(), request.modelVersion());

        // Recorded on the manifest for provenance; the snapshot id addresses the source version
        // even though the sequence number is what ordered it.
        // Ordering comes from the sequence number; the snapshot id is carried alongside it for
        // identity. Picking the snapshot id by max() would be wrong -- Iceberg snapshot ids are
        // random longs -- so select the row with the highest sequence number and take its id.
        IndexVector newest = vectors.stream()
                .max(java.util.Comparator.comparingLong(IndexVector::sourceSequenceNumber))
                .orElse(null);
        long snapshotId = newest != null
                ? newest.sourceSnapshotId()
                : vectorSyncReader.latestSourceSnapshot(request.sourceTable());
        long sequenceNumber = newest != null
                ? newest.sourceSequenceNumber()
                : vectorSyncReader.latestSourceSequenceNumber(request.sourceTable());

        if (vectors.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "No live vectors for " + request.sourceTable() + " " + request.modelVersion(),
                    "availableModelVersions", vectorSyncReader.modelVersionsFor(request.sourceTable())));
        }

        try {
            IndexManifestEntry entry = builder.build(
                    request.sourceTable(), snapshotId, sequenceNumber, parts[0], parts[1], vectors);
            return ResponseEntity.status(HttpStatus.CREATED).body(entry);
        } catch (Exception e) {
            log.error("Index build failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Build failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /**
     * Points the production alias at an index. One Iceberg commit.
     *
     * <p>Gated, because promotion is the moment an artifact starts answering every query for the
     * table and there is no feedback channel behind it: the consumers are query engines, so a bad
     * promotion produces no complaints, only quietly worse output. Checks refuse an index that was
     * never evaluated, one whose measured recall is below the floor, and one that does not cover
     * the newest data. All three are overridable with {@code force}, which requires a note and is
     * recorded in the alias log -- an unoverridable gate just gets bypassed by editing the alias
     * table directly, which would lose the audit trail entirely.
     */
    @PostMapping("/promote")
    public ResponseEntity<?> promote(@RequestBody PromoteRequest request) {
        if (isBlank(request.sourceTable()) || isBlank(request.indexId())) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "sourceTable and indexId are required"));
        }

        IndexManifestEntry entry = registry.manifest().findById(request.indexId()).orElse(null);
        if (entry == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Unknown index: " + request.indexId()));
        }
        if (!entry.isServable()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Index is not servable", "status", String.valueOf(entry.getStatus())));
        }

        boolean force = Boolean.TRUE.equals(request.force());
        if (force && isBlank(request.note())) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "force requires a note explaining why the checks are being overridden"));
        }

        List<String> blockers = promotionBlockers(entry);
        if (!blockers.isEmpty() && !force) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Index did not pass promotion checks",
                    "blockers", blockers,
                    "hint", "evaluate the index first, or re-send with force=true and a note"));
        }

        String note = request.note();
        if (!blockers.isEmpty()) {
            log.warn("Forced promotion of {} for {} over blockers {}",
                    entry.getIndexId(), request.sourceTable(), blockers);
            note = "FORCED (" + String.join("; ", blockers) + "): " + note;
        }

        IndexAliasEntry promoted = registry.aliases().promote(
                IndexAliasStore.PRODUCTION,
                request.sourceTable(),
                request.indexId(),
                request.promotedBy() == null ? "api" : request.promotedBy(),
                note);

        return ResponseEntity.ok(promoted);
    }

    /**
     * Reasons this index should not serve production. Empty means it passes.
     *
     * <p>Only label-free recall can gate: precision needs a judged fixture the table owner may
     * never have written, so requiring it would block every honest promotion.
     */
    private List<String> promotionBlockers(IndexManifestEntry entry) {
        List<String> blockers = new ArrayList<>();

        Map<String, String> metrics = entry.getEvalMetrics();
        String recallKey = metrics == null ? null : metrics.keySet().stream()
                .filter(key -> key.startsWith("index_recall@"))
                .findFirst()
                .orElse(null);

        if (recallKey == null) {
            blockers.add("never evaluated: no index_recall@k recorded on the manifest entry");
        } else {
            try {
                double recall = Double.parseDouble(metrics.get(recallKey));
                if (recall < minIndexRecall) {
                    blockers.add(String.format(
                            "%s=%.4f is below the floor of %.4f", recallKey, recall, minIndexRecall));
                }
            } catch (NumberFormatException e) {
                blockers.add(recallKey + " is not a number: " + metrics.get(recallKey));
            }
        }

        long newest = vectorSyncReader.latestSourceSequenceNumber(entry.getSourceTable());
        if (entry.getSourceSequenceNumber() < newest) {
            blockers.add(String.format(
                    "covers source sequence %d but the table is at %d",
                    entry.getSourceSequenceNumber(), newest));
        }

        return blockers;
    }

    /** Re-points production at whatever it served previously. */
    @PostMapping("/rollback")
    public ResponseEntity<?> rollback(@RequestParam("sourceTable") String sourceTable,
                                      @RequestParam(value = "rolledBackBy", required = false) String rolledBackBy) {
        IndexAliasEntry previous = registry.aliases()
                .previous(IndexAliasStore.PRODUCTION, sourceTable)
                .orElse(null);

        if (previous == null) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "No previous index to roll back to for " + sourceTable));
        }

        IndexAliasEntry restored = registry.aliases().promote(
                IndexAliasStore.PRODUCTION,
                sourceTable,
                previous.getIndexId(),
                rolledBackBy == null ? "api" : rolledBackBy,
                "rollback to " + previous.getIndexId());

        return ResponseEntity.ok(restored);
    }

    /**
     * Measures index recall against an exhaustive scan, plus precision when labels are supplied.
     *
     * <p>With no {@code queries} this runs label-free against self-sampled probes, which is the form
     * an operator or a pre-promotion check can always run. With {@code queries} it also scores
     * precision, which a pipeline owning a judged set would use to compare two models.
     */
    @PostMapping("/evaluate")
    public ResponseEntity<?> evaluate(@RequestBody EvaluateRequest request) {
        if (isBlank(request.indexId())) {
            return ResponseEntity.badRequest().body(Map.of("error", "indexId is required"));
        }

        int topK = request.topK() > 0 ? request.topK() : 10;

        try {
            // No queries means the label-free path, which is the one that needs no external input
            // and so is the one an operator can always run.
            if (request.queries() == null || request.queries().isEmpty()) {
                return ResponseEntity.ok(evaluationService.evaluateIndexRecall(
                        request.indexId(), topK, request.probeCount()));
            }

            List<EvaluationService.QueryJudgement> judgements = request.queries().stream()
                    .map(entry -> new EvaluationService.QueryJudgement(
                            entry.query(),
                            entry.relevantSourceRowIds() == null ? List.of() : entry.relevantSourceRowIds()))
                    .toList();

            return ResponseEntity.ok(evaluationService.evaluate(
                    request.indexId(), judgements, topK, request.fixtureRef()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Evaluation failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Evaluation failed", "message", String.valueOf(e.getMessage())));
        }
    }

    @GetMapping("/indexes")
    public ResponseEntity<List<IndexManifestEntry>> indexes(
            @RequestParam(value = "sourceTable", required = false) String sourceTable) {
        return ResponseEntity.ok(sourceTable == null
                ? registry.manifest().list()
                : registry.indexesFor(sourceTable));
    }

    /**
     * Indexes for a table, annotated with whether each still covers the newest source version.
     * An index built over a superseded snapshot stays in the manifest for audit and rollback, but
     * serving it would return results from stale data.
     */
    @GetMapping("/indexes/status")
    public ResponseEntity<List<Map<String, Object>>> indexStatus(
            @RequestParam("sourceTable") String sourceTable) {
        // The vector table's newest version, not the manifest's. Asking the manifest for
        // latestCoveredSequenceNumber compares indexes against each other, so the newest index
        // always satisfies it and "current" could never go false no matter how far the data moved
        // ahead -- which defeats the only purpose of the flag.
        long newest = vectorSyncReader.latestSourceSequenceNumber(sourceTable);
        String promoted = registry.promotedAlias(sourceTable)
                .map(alias -> alias.getIndexId())
                .orElse(null);

        List<Map<String, Object>> rows = registry.indexesFor(sourceTable).stream()
                .map(entry -> {
                    Map<String, Object> row = new java.util.LinkedHashMap<>();
                    row.put("indexId", entry.getIndexId());
                    row.put("embeddingModel", entry.getEmbeddingModel());
                    row.put("embeddingVersion", entry.getEmbeddingVersion());
                    row.put("dimension", entry.getDimension());
                    row.put("vectorCount", entry.getVectorCount());
                    row.put("status", String.valueOf(entry.getStatus()));
                    row.put("sourceSnapshotId", entry.getSourceSnapshotId());
                    row.put("sourceSequenceNumber", entry.getSourceSequenceNumber());
                    row.put("current", entry.getSourceSequenceNumber() >= newest);
                    row.put("serving", entry.getIndexId().equals(promoted));
                    row.put("evalMetrics", entry.getEvalMetrics());
                    row.put("builtAt", String.valueOf(entry.getBuiltAt()));
                    return row;
                })
                .toList();

        return ResponseEntity.ok(rows);
    }

    @GetMapping("/promoted")
    public ResponseEntity<?> promoted(@RequestParam("sourceTable") String sourceTable) {
        return registry.promotedIndex(sourceTable)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.ok(Map.of(
                        "sourceTable", sourceTable,
                        "promoted", false,
                        "note", "No promoted index; queries fall back to exact search")));
    }

    /** Full promotion history, newest last. Every promotion and rollback is retained. */
    @GetMapping("/history")
    public ResponseEntity<List<IndexAliasEntry>> history(@RequestParam("sourceTable") String sourceTable) {
        return ResponseEntity.ok(registry.aliases().history(sourceTable));
    }

    @GetMapping("/model-versions")
    public ResponseEntity<Map<String, Object>> modelVersions(@RequestParam("sourceTable") String sourceTable) {
        return ResponseEntity.ok(Map.of(
                "sourceTable", sourceTable,
                "modelVersions", vectorSyncReader.modelVersionsFor(sourceTable),
                "latestSourceSnapshotId", vectorSyncReader.latestSourceSnapshot(sourceTable),
                "latestSourceSequenceNumber", vectorSyncReader.latestSourceSequenceNumber(sourceTable)));
    }

    @PostMapping("/index/{indexId}/evict")
    public ResponseEntity<Map<String, Object>> evict(@PathVariable("indexId") String indexId) {
        indexCache.evict(indexId);
        return ResponseEntity.ok(Map.of("evicted", indexId, "openArtifacts", indexCache.openCount()));
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
