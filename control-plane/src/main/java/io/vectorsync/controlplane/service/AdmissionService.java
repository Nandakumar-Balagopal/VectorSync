package io.vectorsync.controlplane.service;

import io.vectorsync.controlplane.model.MaterializationEntity;
import io.vectorsync.controlplane.model.MaterializationEntity.State;
import io.vectorsync.controlplane.repository.MaterializationRepository;
import io.vectorsync.controlplane.service.iceberg.IcebergCatalogService;
import io.vectorsync.format.derive.ContentMap;
import io.vectorsync.format.derive.MaterializationSpec;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * The gate every derived dataset passes through, and the one place its lifecycle state changes.
 *
 * <p>Registration used to be a UUID and an INSERT. A spec naming a table that does not exist, a
 * column that was renamed two schema evolutions ago, or a model nothing can load was accepted with
 * a 200 and then failed forever inside the scheduler, where the operator saw a retry loop instead
 * of a reason. Admission moves that discovery to the request: the spec is resolved against the live
 * catalog and every problem is reported at once, because fixing one error only to be told about the
 * next one is the same round trip repeated.
 *
 * <p>Admission also answers "what will this cost" before anything is scheduled, using Iceberg
 * manifest metadata only. Counting rows by scanning them would make the estimate as expensive as a
 * chunk of the work it is estimating, and the numbers are already in the snapshot summary.
 *
 * <p>Lifecycle transitions live here rather than in the controller so that they all pass through
 * {@link MaterializationEntity#transitionTo(State)}. A pause that bypasses the state machine is how
 * a retired dataset ends up back in serving.
 */
@Service
@Slf4j
public class AdmissionService {

    /**
     * Chunkers the pipeline can actually execute. Admission rejects anything else: an unknown
     * chunker name is a spec that validates, schedules, and then dies in the worker, which is the
     * exact failure mode admission exists to remove. Adding a chunker means adding it here and
     * teaching {@link #chunksPerRow} how to size it.
     */
    public static final Set<String> KNOWN_CHUNKERS = Set.of("whole", "fixed");

    /** Applied when a request does not pin one. An hour is the coarsest useful reporting window. */
    private static final int DEFAULT_FRESHNESS_SLA_SECONDS = 3600;

    private final MaterializationRepository materializationRepository;
    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public AdmissionService(MaterializationRepository materializationRepository,
                            IcebergCatalogService catalogService) {
        this.materializationRepository = materializationRepository;
        this.catalogService = catalogService;
    }

    /**
     * Validates a spec against the live catalog, prices it, and on success persists it.
     *
     * <p>Validation accumulates. Every problem found is returned together, and the spec is rejected
     * if there is at least one.
     *
     * @param dryRun when true nothing is written: the caller gets the same verdict and cost
     *               estimate it would have got, which is what makes admission safe to call from a
     *               UI form as the operator types
     */
    @Transactional
    public AdmissionResult admit(AdmissionRequest request, boolean dryRun) {
        if (request == null) {
            throw new IllegalArgumentException("request body is required");
        }

        List<String> problems = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        String sourceTable = trimToEmpty(request.getSourceTable());
        if (sourceTable.isEmpty()) {
            problems.add("sourceTable is required");
        }
        if (trimToEmpty(request.getModelName()).isEmpty()) {
            problems.add("modelName is required: a materialization cannot pick its own embedder");
        }
        if (trimToEmpty(request.getModelRevision()).isEmpty()) {
            warnings.add("modelRevision is empty: the model family's weights can change under this "
                    + "materialization, so its vectors are not reproducible");
        }

        List<String> keyColumns = normalizeColumns(request.getKeyColumns());
        List<String> embeddingColumns = normalizeColumns(request.getEmbeddingColumns());
        if (keyColumns.isEmpty()) {
            problems.add("keyColumns is required: row identity cannot be guessed from the schema");
        }
        if (embeddingColumns.isEmpty()) {
            problems.add("embeddingColumns is required");
        }
        if (hasDuplicates(embeddingColumns)) {
            warnings.add("embeddingColumns repeats a column: the repeated text is embedded as part "
                    + "of the assembled content, which is legal but rarely intended");
        }
        validateColumnNames("key", keyColumns, problems);
        validateColumnNames("embedding", embeddingColumns, problems);

        validateChunking(request, problems, warnings);

        Table table = null;
        Snapshot snapshot = null;
        TableIdentifier identifier = null;
        if (!sourceTable.isEmpty()) {
            identifier = toIdentifier(sourceTable, request.getCatalogName());
            try {
                table = catalogService.getCatalog().loadTable(identifier);
            } catch (Exception e) {
                problems.add(String.format("source table %s does not resolve in the catalog: %s",
                        identifier, rootMessage(e)));
            }
        }

        if (table != null) {
            snapshot = table.currentSnapshot();
            if (snapshot == null) {
                // An empty table is not a failure of the spec, but a backfill pinned to no snapshot
                // has no anchor and incremental sync would have nothing to advance from.
                problems.add(String.format("source table %s has no current snapshot: there is "
                        + "nothing to materialize yet", identifier));
            }
            validateColumns(table, keyColumns, embeddingColumns, problems);
        }

        CostEstimate estimate = table == null || snapshot == null
                ? CostEstimate.unavailable()
                : estimateCost(table, snapshot, request, embeddingColumns.size());

        // Two spellings of one table -- "orders" with catalogName "default", and "default.orders" --
        // must resolve to one name before anything is persisted. The name is the content map's
        // partition value and half of the (source_table, config_id) uniqueness constraint, so two
        // spellings would split one dataset across two partitions and let the same materialization be
        // admitted twice. It does not affect configId, which identifies the derivation function only
        // and deliberately excludes the table so that identical text deduplicates across tables.
        String specSourceTable = table == null ? sourceTable : identifier.toString();

        MaterializationSpec spec = null;
        String configId = null;
        if (problems.isEmpty()) {
            try {
                spec = toSpec(request, specSourceTable, keyColumns, embeddingColumns);
                configId = spec.configId();
            } catch (RuntimeException e) {
                problems.add("spec is not constructible: " + rootMessage(e));
            }
        }

        Optional<MaterializationEntity> existing = configId == null
                ? Optional.empty()
                : materializationRepository.findBySourceTableAndConfigId(specSourceTable, configId);
        if (existing.isPresent()) {
            // Same spec means the same content map partition and the same store keys, so a second
            // materialization would duplicate every write without producing anything new. The
            // existing row's state is reported too, because the collision is often with a retired
            // dataset and "already exists" alone does not say where to look. A dry run reports it as
            // a warning: the point of the call is to find out, not to be refused.
            String message = String.format(
                    "an identical materialization of %s already exists (configId %s, id %s, state %s)",
                    specSourceTable, configId, existing.get().getId(), existing.get().getState());
            if (dryRun) {
                warnings.add(message);
            } else {
                problems.add(message);
            }
        }

        if (!problems.isEmpty()) {
            log.info("Admission rejected for {}: {}", sourceTable, problems);
            return AdmissionResult.builder()
                    .admitted(false)
                    .dryRun(dryRun)
                    .problems(List.copyOf(problems))
                    .warnings(List.copyOf(warnings))
                    .configId(configId)
                    .estimate(estimate)
                    .build();
        }

        if (dryRun) {
            return AdmissionResult.builder()
                    .admitted(true)
                    .dryRun(true)
                    .problems(List.of())
                    .warnings(List.copyOf(warnings))
                    .configId(configId)
                    .estimate(estimate)
                    .build();
        }

        MaterializationEntity persisted = persist(request, spec, snapshot);
        log.info("Admitted materialization {} of {} (configId {}) anchored at sequence {}",
                persisted.getId(), persisted.getSourceTable(), persisted.getConfigId(),
                persisted.getAnchorSequenceNumber());

        return AdmissionResult.builder()
                .admitted(true)
                .dryRun(false)
                .problems(List.of())
                .warnings(List.copyOf(warnings))
                .configId(persisted.getConfigId())
                .estimate(estimate)
                .materialization(persisted)
                .build();
    }

    public List<MaterializationEntity> list() {
        return materializationRepository.findAll();
    }

    public List<MaterializationEntity> listByState(State state) {
        return materializationRepository.findByState(state);
    }

    public Optional<MaterializationEntity> find(String id) {
        return materializationRepository.findById(id);
    }

    /**
     * State plus progress for one materialization.
     *
     * <p>Progress is reported as watermarks, not as counts of finished chunks. A chunk count means
     * scanning this configuration's content map partition, which is unbounded work behind a status
     * endpoint that dashboards poll; the sequence numbers answer "is it caught up" exactly and cost
     * one projected metadata read.
     */
    public Optional<MaterializationProgress> progress(String id) {
        return materializationRepository.findById(id).map(entity -> {
            long materialized = materializedSequenceNumber(entity);
            Instant updatedAt = entity.getUpdatedAt() == null ? entity.getCreatedAt() : entity.getUpdatedAt();
            long secondsSinceUpdate = updatedAt == null
                    ? 0L
                    : Math.max(0L, Duration.between(updatedAt, Instant.now()).getSeconds());
            int sla = entity.getFreshnessSlaSeconds();

            return MaterializationProgress.builder()
                    .id(entity.getId())
                    .sourceTable(entity.getSourceTable())
                    .configId(entity.getConfigId())
                    .modelVersion(entity.modelVersion())
                    .state(entity.getState())
                    .allowedTransitions(new ArrayList<>(entity.allowedTransitions()))
                    .anchorSnapshotId(entity.getAnchorSnapshotId())
                    .anchorSequenceNumber(entity.getAnchorSequenceNumber())
                    .incrementalWatermark(entity.getIncrementalWatermark())
                    .materializedSequenceNumber(materialized)
                    .caughtUp(isCaughtUp(entity))
                    .freshnessSlaSeconds(sla)
                    .secondsSinceUpdate(secondsSinceUpdate)
                    // Only a serving dataset can breach a freshness SLA; a paused or retiring one
                    // is stale because an operator said so, and reporting that as a breach trains
                    // people to ignore the signal.
                    .slaBreached(sla > 0 && secondsSinceUpdate > sla
                            && (entity.getState() == State.LIVE || entity.getState() == State.DEGRADED))
                    .purgeEligible(entity.isPurgeEligible())
                    .lastError(entity.getLastError())
                    .createdAt(entity.getCreatedAt())
                    .updatedAt(entity.getUpdatedAt())
                    .build();
        });
    }

    /** Stops scheduling. Nothing already written is touched, so resume is free. */
    @Transactional
    public Optional<MaterializationEntity> pause(String id) {
        return materializationRepository.findById(id).map(entity -> {
            if (entity.getState() == State.PAUSED) {
                return entity;
            }
            entity.transitionTo(State.PAUSED);
            return materializationRepository.save(entity);
        });
    }

    /**
     * Resumes a paused materialization.
     *
     * <p>The target state is derived, never supplied by the caller: a materialization with no
     * reported coverage at the anchor goes back to VALIDATED so the plan is recomputed against it,
     * and one that was already caught up goes straight to LIVE. Re-planning is cheap here precisely
     * because vectors are keyed by content -- replaying an interrupted backfill re-embeds only text
     * the store has never seen.
     */
    @Transactional
    public Optional<MaterializationEntity> resume(String id) {
        return materializationRepository.findById(id).map(entity -> {
            if (entity.getState() != State.PAUSED) {
                throw new IllegalStateException(String.format(
                        "Materialization %s is %s, not PAUSED", id, entity.getState()));
            }
            State target = isCaughtUp(entity) ? State.LIVE : State.VALIDATED;
            entity.transitionTo(target);
            entity.setLastError(null);
            return materializationRepository.save(entity);
        });
    }

    /**
     * Retires a materialization without deleting a single vector.
     *
     * <p>This is the important part. The embedding store is keyed by {@code (content_hash,
     * model_version, config_id)} and is deduplicated by construction: any other materialization
     * whose text hashed to the same value is being served from the very same rows. Deleting from
     * the store "for this source table" would therefore silently blank vectors belonging to
     * datasets that have nothing to do with this one, and because the store is the thing that makes
     * re-embedding cheap, the damage is only discovered when a search returns nothing.
     *
     * <p>So retire moves state and nothing else. {@code purge} does not delete either: it marks
     * this configuration's rows as candidates for the reclaim sweeper, which is the only component
     * that may compute "content referenced by no live content-map entry under any configuration"
     * and act on it. That computation needs a global view and a retention window, neither of which
     * a single HTTP request has.
     *
     * @param purge mark the configuration's content-map rows as reclaim candidates
     */
    @Transactional
    public Optional<MaterializationEntity> retire(String id, boolean purge) {
        return materializationRepository.findById(id).map(entity -> {
            if (entity.getState() == State.RETIRED) {
                if (purge && !entity.isPurgeEligible()) {
                    entity.setPurgeEligible(true);
                    entity.setUpdatedAt(Instant.now());
                    return materializationRepository.save(entity);
                }
                return entity;
            }

            // Nothing has been produced yet from a spec that never started, so there is no in-flight
            // work to drain and RETIRING would be a state it sat in until someone noticed.
            State target = entity.getState() == State.REGISTERED
                    || entity.getState() == State.VALIDATED
                    || entity.getState() == State.PAUSED
                    ? State.RETIRED
                    : State.RETIRING;

            if (entity.getState() != target) {
                entity.transitionTo(target);
            }
            if (purge) {
                entity.setPurgeEligible(true);
            }
            log.info("Retiring materialization {} of {} -> {} (purgeEligible={}); no vectors deleted",
                    entity.getId(), entity.getSourceTable(), entity.getState(), entity.isPurgeEligible());
            return materializationRepository.save(entity);
        });
    }

    /**
     * Completes a drain: RETIRING to RETIRED. Called by the sweeper once no worker holds in-flight
     * work for this configuration, not by the retire request, which cannot know that.
     */
    @Transactional
    public Optional<MaterializationEntity> completeRetire(String id) {
        return materializationRepository.findById(id).map(entity -> {
            if (entity.getState() == State.RETIRED) {
                return entity;
            }
            entity.transitionTo(State.RETIRED);
            return materializationRepository.save(entity);
        });
    }

    /** Claimed by a worker that is starting the anchored backfill. */
    @Transactional
    public Optional<MaterializationEntity> beginBackfill(String id) {
        return materializationRepository.findById(id).map(entity -> {
            // Idempotent. The planner calls this on every cycle until the backfill drains, and
            // BACKFILLING -> BACKFILLING is not a legal transition, so an unguarded call threw a 500
            // and logged a stack trace every interval for the whole duration of the backfill.
            if (entity.getState() == State.BACKFILLING) {
                return entity;
            }
            entity.transitionTo(State.BACKFILLING);
            entity.setUpdatedAt(Instant.now());
            return materializationRepository.save(entity);
        });
    }

    /** Backfill (or a migration) finished and incremental sync has reached {@code watermark}. */
    @Transactional
    public Optional<MaterializationEntity> markLive(String id, long watermark) {
        return materializationRepository.findById(id).map(entity -> {
            // Never move the watermark backwards: an out-of-order completion report would make
            // incremental sync re-read source versions it has already materialized.
            if (watermark > entity.getIncrementalWatermark()) {
                entity.setIncrementalWatermark(watermark);
            }
            if (entity.getState() != State.LIVE) {
                entity.transitionTo(State.LIVE);
            } else {
                entity.setUpdatedAt(Instant.now());
            }
            entity.setLastError(null);
            return materializationRepository.save(entity);
        });
    }

    /** Serving, but stale or incomplete. Keeps the dataset schedulable so it can recover. */
    @Transactional
    public Optional<MaterializationEntity> markDegraded(String id, String reason) {
        return materializationRepository.findById(id).map(entity -> {
            entity.setLastError(reason);
            if (entity.getState() != State.DEGRADED) {
                entity.transitionTo(State.DEGRADED);
            } else {
                entity.setUpdatedAt(Instant.now());
            }
            return materializationRepository.save(entity);
        });
    }

    private MaterializationEntity persist(AdmissionRequest request,
                                          MaterializationSpec spec,
                                          Snapshot snapshot) {
        Instant now = Instant.now();
        MaterializationEntity entity = MaterializationEntity.builder()
                .id(UUID.randomUUID().toString())
                .sourceTable(spec.getSourceTable())
                .catalogName(trimToNull(request.getCatalogName()))
                .keyColumns(MaterializationEntity.joinColumns(spec.getKeyColumns()))
                .embeddingColumns(MaterializationEntity.joinColumns(spec.getEmbeddingColumns()))
                .joinSeparator(spec.getJoinSeparator())
                .chunker(spec.getChunker())
                .chunkSize(spec.getChunkSize())
                .chunkOverlap(spec.getChunkOverlap())
                .modelName(spec.getModelName())
                .modelRevision(spec.getModelRevision())
                .embeddingVersion(spec.getEmbeddingVersion())
                .normalize(spec.isNormalize())
                .configId(spec.configId())
                .state(State.REGISTERED)
                .incrementalWatermark(0L)
                .freshnessSlaSeconds(request.getFreshnessSlaSeconds() > 0
                        ? request.getFreshnessSlaSeconds()
                        : DEFAULT_FRESHNESS_SLA_SECONDS)
                .priority(request.getPriority())
                .purgeEligible(false)
                .createdAt(now)
                .updatedAt(now)
                .build();

        // Flushed at REGISTERED so the row exists with its identity before it claims to be
        // schedulable, and so a collision on (source_table, config_id) surfaces here -- at commit
        // time it is no longer attributable to this request.
        try {
            materializationRepository.saveAndFlush(entity);
        } catch (DataIntegrityViolationException e) {
            throw new IllegalStateException(String.format(
                    "an identical materialization of %s already exists (configId %s)",
                    entity.getSourceTable(), entity.getConfigId()), e);
        }

        // Anchoring happens after validation confirmed a current snapshot exists. The pin is what
        // makes the backfill reproducible: it covers exactly this source version, and everything
        // above it belongs to incremental sync.
        entity.setAnchorSnapshotId(snapshot.snapshotId());
        entity.setAnchorSequenceNumber(snapshot.sequenceNumber());
        entity.transitionTo(State.VALIDATED);
        return materializationRepository.save(entity);
    }

    private void validateChunking(AdmissionRequest request, List<String> problems, List<String> warnings) {
        String chunker = request.getChunker() == null || request.getChunker().isBlank()
                ? "whole"
                : request.getChunker().trim();
        if (!KNOWN_CHUNKERS.contains(chunker)) {
            problems.add(String.format("unknown chunker '%s'; known: %s", chunker, KNOWN_CHUNKERS));
            return;
        }

        int size = request.getChunkSize();
        int overlap = request.getChunkOverlap();

        if ("whole".equals(chunker)) {
            if (size != 0 || overlap != 0) {
                // chunkSize feeds configId, so a value the "whole" chunker ignores still forks the
                // dataset: two configuration ids whose vectors are bit-identical, embedded twice.
                warnings.add("chunker 'whole' ignores chunkSize/chunkOverlap, but both are part of "
                        + "configId: leave them at 0 or this spec forks a duplicate dataset");
            }
            return;
        }

        if (size <= 0) {
            problems.add("chunkSize must be positive for chunker '" + chunker + "'");
        }
        if (overlap < 0) {
            problems.add("chunkOverlap must not be negative");
        }
        if (size > 0 && overlap >= size) {
            // Overlap at or above the window means each chunk starts no further on than the last,
            // so the chunker never reaches the end of the text.
            problems.add(String.format(
                    "chunkOverlap (%d) must be less than chunkSize (%d): a stride of zero or less "
                            + "never terminates", overlap, size));
        }
    }

    /**
     * Rejects column names that cannot survive the round trip through storage.
     *
     * <p>The column lists are persisted comma separated, so a name containing the separator splits
     * into two on read and {@link MaterializationEntity#toSpec()} rebuilds a spec whose
     * {@code configId} differs from the one this row was admitted under. The worker would then write
     * a content-map partition nobody reads and re-embed the whole dataset under a second
     * configuration id, which is the one failure the content-keyed store cannot absorb.
     */
    private static void validateColumnNames(String label, List<String> columns, List<String> problems) {
        for (String column : columns) {
            if (column.contains(MaterializationEntity.COLUMN_SEPARATOR)) {
                problems.add(String.format(
                        "%s column '%s' contains '%s', which separates the stored column lists: "
                                + "project it under a different name in a view first",
                        label, column, MaterializationEntity.COLUMN_SEPARATOR));
            }
        }
    }

    private void validateColumns(Table table,
                                 List<String> keyColumns,
                                 List<String> embeddingColumns,
                                 List<String> problems) {
        org.apache.iceberg.Schema schema = table.schema();

        for (String column : keyColumns) {
            Types.NestedField field = schema.findField(column);
            if (field == null) {
                problems.add(String.format("key column '%s' does not exist in %s", column, table.name()));
                continue;
            }
            if (field.isOptional()) {
                // A null key column produces a null component in the row identity, so every row
                // missing it collapses onto the same source_row_id and their chunks overwrite each
                // other in the content map.
                problems.add(String.format(
                        "key column '%s' is optional: row identity must never be null", column));
            }
        }

        for (String column : embeddingColumns) {
            Types.NestedField field = schema.findField(column);
            if (field == null) {
                problems.add(String.format("embedding column '%s' does not exist in %s",
                        column, table.name()));
                continue;
            }
            if (field.type().typeId() != Type.TypeID.STRING) {
                // Non-string columns would have to be rendered to text somewhere, and whatever did
                // the rendering would be an unhashed input to the content hash: change the
                // formatting and every vector silently becomes wrong without configId moving.
                problems.add(String.format(
                        "embedding column '%s' is %s, not string: cast or project it in a view first",
                        column, field.type()));
            }
        }
    }

    /**
     * Prices a materialization from manifest metadata alone.
     *
     * <p>The row and file counts are read from the snapshot summary, which Iceberg maintains on
     * every commit. The fallback sums {@code planFiles} record counts, which still reads only
     * manifests -- no data file is opened either way. Scanning to get an exact count would cost a
     * meaningful fraction of the job being estimated, and the estimate exists to decide whether to
     * run that job at all.
     */
    private CostEstimate estimateCost(Table table,
                                      Snapshot snapshot,
                                      AdmissionRequest request,
                                      int embeddingColumnCount) {
        Map<String, String> summary = snapshot.summary() == null ? Map.of() : snapshot.summary();
        Long rows = parseLong(summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
        Long files = parseLong(summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
        Long bytes = parseLong(summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
        String source = "snapshot-summary";
        List<String> notes = new ArrayList<>();

        if (rows == null || files == null) {
            long scannedRows = 0L;
            long scannedFiles = 0L;
            long scannedBytes = 0L;
            try (CloseableIterable<FileScanTask> tasks =
                         table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
                for (FileScanTask task : tasks) {
                    scannedRows += task.file().recordCount();
                    scannedBytes += task.file().fileSizeInBytes();
                    scannedFiles++;
                }
                rows = scannedRows;
                files = scannedFiles;
                bytes = bytes == null ? scannedBytes : bytes;
                source = "manifest-metadata";
                notes.add("snapshot summary lacked total-records/total-data-files, so counts were "
                        + "summed from manifest entries");
            } catch (Exception e) {
                log.warn("Cost estimate for {} fell back to unknown counts: {}",
                        table.name(), rootMessage(e));
                return CostEstimate.unavailable();
            }
        }

        double perRow = chunksPerRow(request, rows, bytes, embeddingColumnCount,
                table.schema().columns().size(), notes);
        long chunks = (long) Math.ceil(rows * perRow);

        notes.add("estimatedChunks is an upper bound on inference calls: vectors are keyed by "
                + "content hash, so repeated and unchanged text is served from the embedding store "
                + "and never embedded again");
        notes.add("no data file was read to produce this estimate");

        return CostEstimate.builder()
                .available(true)
                .estimatedRows(rows)
                .estimatedChunks(chunks)
                .estimatedFiles(files)
                .estimatedBytes(bytes == null ? 0L : bytes)
                .estimatedChunksPerRow(perRow)
                .source(source)
                .notes(List.copyOf(notes))
                .build();
    }

    /**
     * Chunks each source row is expected to produce.
     *
     * <p>"whole" is exactly one. A fixed-window chunker depends on text length, which is the one
     * thing manifest metadata does not carry, so length is approximated from average bytes per row
     * scaled by the share of columns being embedded. That is deliberately crude and it understates:
     * Parquet stores text compressed, so the real character count is higher. It is reported with
     * its provenance rather than presented as a measurement.
     */
    private double chunksPerRow(AdmissionRequest request,
                                long rows,
                                Long bytes,
                                int embeddingColumnCount,
                                int totalColumnCount,
                                List<String> notes) {
        String chunker = request.getChunker() == null || request.getChunker().isBlank()
                ? "whole"
                : request.getChunker().trim();
        if (!"fixed".equals(chunker)) {
            return 1.0d;
        }

        int stride = request.getChunkSize() - request.getChunkOverlap();
        if (stride <= 0) {
            return 1.0d;
        }
        if (rows <= 0 || bytes == null || bytes <= 0 || totalColumnCount <= 0) {
            notes.add("chunks per row assumed to be 1: the snapshot carries no size information to "
                    + "estimate text length from");
            return 1.0d;
        }

        double embeddedShare = Math.min(1.0d, (double) Math.max(1, embeddingColumnCount) / totalColumnCount);
        double estimatedCharsPerRow = ((double) bytes / rows) * embeddedShare;
        double perRow = Math.max(1.0d, Math.ceil(estimatedCharsPerRow / stride));
        notes.add(String.format(
                "chunks per row (%.2f) estimated from ~%.0f embedded characters per row over a "
                        + "stride of %d; compressed storage means the real count is likely higher",
                perRow, estimatedCharsPerRow, stride));
        return perRow;
    }

    /**
     * Whether coverage has been reported at or above the anchor.
     *
     * <p>Only {@code incrementalWatermark} can answer this, because it is written exactly once per
     * completed unit of work. The content map's highest sequence number cannot: a backfill stamps
     * every entry it writes with the anchor snapshot's sequence number, so the first committed batch
     * already pushes that maximum up to the anchor while most of the table is still unembedded.
     * Treating it as completeness would resume an interrupted backfill straight into LIVE and leave
     * the unwritten remainder of the dataset permanently missing from serving.
     */
    private static boolean isCaughtUp(MaterializationEntity entity) {
        return entity.getAnchorSequenceNumber() > 0
                && entity.getIncrementalWatermark() >= entity.getAnchorSequenceNumber();
    }

    /**
     * Highest source sequence number the content map has actually recorded for this configuration.
     *
     * <p>Read from the format layer rather than from {@code incremental_watermark} so that a status
     * view shows what the data says and not only what the bookkeeping claims; the two diverging is
     * the interesting signal. It is an observation of how far writing has reached, never a
     * completeness test -- see {@link #isCaughtUp}. Logs and returns zero on failure: this feeds a
     * status view, and an unavailable catalog must not turn a state lookup into a 500.
     */
    private long materializedSequenceNumber(MaterializationEntity entity) {
        try {
            Catalog catalog = catalogService.getCatalog();
            Table contentMap = ContentMap.loadIfExists(catalog, vectorNamespace);
            return ContentMap.latestSequenceNumber(contentMap, entity.getSourceTable(), entity.getConfigId());
        } catch (Exception e) {
            log.warn("Could not read content map progress for {}: {}", entity.getId(), rootMessage(e));
            return 0L;
        }
    }

    private MaterializationSpec toSpec(AdmissionRequest request,
                                       String sourceTable,
                                       List<String> keyColumns,
                                       List<String> embeddingColumns) {
        return MaterializationSpec.builder()
                .sourceTable(sourceTable)
                .keyColumns(keyColumns)
                .embeddingColumns(embeddingColumns)
                .joinSeparator(request.getJoinSeparator())
                .chunker(request.getChunker() == null || request.getChunker().isBlank()
                        ? null
                        : request.getChunker().trim())
                .chunkSize(request.getChunkSize())
                .chunkOverlap(request.getChunkOverlap())
                .modelName(trimToNull(request.getModelName()))
                .modelRevision(trimToNull(request.getModelRevision()))
                .embeddingVersion(trimToNull(request.getEmbeddingVersion()))
                .normalize(request.isNormalize())
                .build();
    }

    /**
     * Resolves a source table name the same way the worker does, so that admission validates the
     * identifier the pipeline will later load. A dotted name is fully qualified; otherwise the
     * request's catalog name is the namespace.
     */
    private static TableIdentifier toIdentifier(String sourceTable, String catalogName) {
        if (sourceTable.contains(".")) {
            return TableIdentifier.parse(sourceTable);
        }
        String namespace = catalogName == null || catalogName.isBlank() ? "default" : catalogName.trim();
        return TableIdentifier.of(Namespace.of(namespace), sourceTable);
    }

    private static List<String> normalizeColumns(List<String> columns) {
        if (columns == null) {
            return List.of();
        }
        List<String> normalized = new ArrayList<>(columns.size());
        for (String column : columns) {
            if (column != null && !column.isBlank()) {
                normalized.add(column.trim());
            }
        }
        return normalized;
    }

    private static boolean hasDuplicates(List<String> values) {
        Set<String> seen = new HashSet<>(values);
        return seen.size() != values.size();
    }

    private static Long parseLong(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private static String trimToNull(String value) {
        String trimmed = trimToEmpty(value);
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String rootMessage(Throwable e) {
        Throwable cursor = e;
        while (cursor.getCause() != null && cursor.getCause() != cursor) {
            cursor = cursor.getCause();
        }
        String message = cursor.getMessage();
        return message == null ? cursor.getClass().getSimpleName() : message;
    }

    /**
     * An admission request: the spec fields, plus the operational knobs that are not part of
     * {@code configId} because they do not change what is produced.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class AdmissionRequest {
        private String sourceTable;
        private String catalogName;
        private List<String> keyColumns;
        private List<String> embeddingColumns;
        private String joinSeparator;
        private String chunker;
        private int chunkSize;
        private int chunkOverlap;
        private String modelName;
        private String modelRevision;
        private String embeddingVersion;
        private boolean normalize;
        private int freshnessSlaSeconds;
        private int priority;

        /** Validate and price only. Nothing is written and no id is issued. */
        private boolean dryRun;
    }

    /**
     * The verdict. {@code problems} is empty exactly when {@code admitted} is true; {@code warnings}
     * never block, they describe specs that are legal but probably not what the operator meant.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class AdmissionResult {
        private boolean admitted;
        private boolean dryRun;
        private List<String> problems;
        private List<String> warnings;

        /** Present as soon as the spec is constructible, so a dry run can show the dataset identity. */
        private String configId;

        private CostEstimate estimate;

        /** Null on a dry run or a rejection. */
        private MaterializationEntity materialization;
    }

    /** What the backfill will cost, in units an operator can compare against a budget. */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class CostEstimate {
        private boolean available;
        private long estimatedRows;
        private long estimatedChunks;
        private long estimatedFiles;
        private long estimatedBytes;
        private double estimatedChunksPerRow;

        /** Where the counts came from: {@code snapshot-summary}, {@code manifest-metadata}, or none. */
        private String source;

        private List<String> notes;

        static CostEstimate unavailable() {
            return CostEstimate.builder()
                    .available(false)
                    .source("unavailable")
                    .notes(List.of("the source table did not resolve to a snapshot, so no counts "
                            + "could be read from its manifests"))
                    .build();
        }
    }

    /** State and watermarks for one materialization. */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class MaterializationProgress {
        private String id;
        private String sourceTable;
        private String configId;
        private String modelVersion;
        private State state;
        private List<State> allowedTransitions;
        private Long anchorSnapshotId;
        private long anchorSequenceNumber;
        private long incrementalWatermark;

        /**
         * Highest sequence number the content map holds for this configuration: how far writing has
         * reached, not how much of that version is covered. {@code caughtUp} is the completeness
         * answer.
         */
        private long materializedSequenceNumber;

        private boolean caughtUp;
        private int freshnessSlaSeconds;
        private long secondsSinceUpdate;
        private boolean slaBreached;
        private boolean purgeEligible;
        private String lastError;
        private Instant createdAt;
        private Instant updatedAt;
    }
}
