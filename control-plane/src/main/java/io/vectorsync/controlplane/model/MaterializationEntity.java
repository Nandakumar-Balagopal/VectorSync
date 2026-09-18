package io.vectorsync.controlplane.model;

import io.vectorsync.format.derive.MaterializationSpec;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The control-plane record of one derived embedding dataset: the spec that defines it, the source
 * version it was anchored at, and where it is in its lifecycle.
 *
 * <p>This is the durable counterpart of {@link MaterializationSpec}. The spec is the immutable
 * definition and {@code configId} is its identity; this row adds the operational state that cannot
 * live in a hash -- which snapshot the backfill was pinned to, how far incremental sync has got,
 * and whether the dataset is being served, migrated, or drained.
 *
 * <p>The spec fields are write-once. Editing one would change {@link MaterializationSpec#configId()}
 * while the already-written content-map and embedding-store rows keep pointing at the old
 * configuration id, so the row would describe a dataset that does not exist. A changed spec is a
 * new materialization admitted alongside this one, which is exactly what makes a model migration
 * reversible.
 */
@Entity
@Table(
        name = "materializations",
        // A materialization is identified by what it produces. Two admissions of the same spec are
        // the same dataset, and admitting it twice would have two schedulers writing the same
        // content-map partition at the same sequence numbers.
        uniqueConstraints = @UniqueConstraint(
                name = "uk_materializations_source_config",
                columnNames = {"source_table", "config_id"}),
        indexes = {
                @Index(name = "ix_materializations_state", columnList = "state"),
                @Index(name = "ix_materializations_source_table", columnList = "source_table")
        })
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MaterializationEntity {

    /**
     * Separator for the CSV-encoded column lists. Order is significant and is preserved. Public
     * because admission must reject column names containing it: such a name would split on read,
     * and the spec rebuilt from this row would hash to a different {@code configId} than the one it
     * was admitted under.
     */
    public static final String COLUMN_SEPARATOR = ",";

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private String id;

    @Column(name = "source_table", nullable = false, updatable = false)
    private String sourceTable;

    @Column(name = "catalog_name", updatable = false)
    private String catalogName;

    /**
     * Key columns in spec order, comma separated. Stored as text rather than a child table because
     * the list is only ever read as a whole, and order is part of {@code config_id}: a relational
     * child table would need its own ordinal column to preserve what a CSV preserves for free.
     */
    @Column(name = "key_columns", columnDefinition = "TEXT", nullable = false, updatable = false)
    private String keyColumns;

    @Column(name = "embedding_columns", columnDefinition = "TEXT", nullable = false, updatable = false)
    private String embeddingColumns;

    @Column(name = "join_separator", updatable = false)
    private String joinSeparator;

    @Column(name = "chunker", nullable = false, updatable = false)
    private String chunker;

    @Column(name = "chunk_size", nullable = false, updatable = false)
    private int chunkSize;

    @Column(name = "chunk_overlap", nullable = false, updatable = false)
    private int chunkOverlap;

    @Column(name = "model_name", nullable = false, updatable = false)
    private String modelName;

    @Column(name = "model_revision", updatable = false)
    private String modelRevision;

    @Column(name = "embedding_version", nullable = false, updatable = false)
    private String embeddingVersion;

    /** Column name avoids the SQL {@code NORMALIZE} keyword, which needs quoting on some engines. */
    @Column(name = "normalize_vectors", nullable = false, updatable = false)
    private boolean normalize;

    /**
     * {@link MaterializationSpec#configId()}, persisted rather than recomputed on read so that a
     * future change to the hashing recipe cannot silently re-point this row at a different dataset.
     */
    @Column(name = "config_id", nullable = false, updatable = false, length = 32)
    private String configId;

    /**
     * Stored as a string. An ordinal column would make the meaning of every historical row depend
     * on the declaration order of the enum, so inserting a state in the middle would rewrite
     * history.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false, length = 32)
    private State state;

    /**
     * The source snapshot the backfill is pinned to. Identity only -- snapshot ids are random longs
     * and must never be compared or ordered.
     */
    @Column(name = "anchor_snapshot_id")
    private Long anchorSnapshotId;

    /**
     * Iceberg's monotonic sequence number for {@link #anchorSnapshotId}. This is the value that
     * orders work: the backfill covers everything up to here and incremental sync takes over above
     * it.
     */
    @Column(name = "anchor_sequence_number")
    private long anchorSequenceNumber;

    /** Highest source sequence number incremental sync has materialized. Zero before backfill. */
    @Column(name = "incremental_watermark")
    private long incrementalWatermark;

    /** How stale the served dataset may get before it is reported as breaching its SLA. */
    @Column(name = "freshness_sla_seconds")
    private int freshnessSlaSeconds;

    /** Higher runs first when workers compete for embedding capacity. */
    @Column(name = "priority")
    private int priority;

    /**
     * Set by {@code retire(purge=true)}. Nothing here deletes anything: it only tells the reclaim
     * sweeper that this configuration's content-map rows are candidates once no other
     * materialization references the content they point at.
     */
    /**
     * True for the one materialization a reader should query for this source table.
     *
     * <p>Enforced by a partial unique index rather than here, because promotion is the operation
     * where two concurrent callers would both otherwise believe they won.
     */
    @Column(name = "serving", nullable = false)
    private boolean serving;

    @Column(name = "purge_eligible", nullable = false)
    private boolean purgeEligible;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    /**
     * Lifecycle of a derived dataset.
     *
     * <p>The legal transitions live in {@link #ALLOWED} and nowhere else. A state machine expressed
     * as scattered {@code setState} calls is not a state machine: every call site gets to invent its
     * own rules, and the first one that forgets a guard moves a retired dataset back into serving.
     */
    public enum State {
        /** Persisted, not yet checked against the catalog. */
        REGISTERED,
        /** Spec resolves against a real table with real columns; safe to schedule. */
        VALIDATED,
        /** Working through the anchor snapshot. */
        BACKFILLING,
        /** Caught up and serving; incremental sync is following the source. */
        LIVE,
        /** Producing a new model or config side by side with the live one. */
        MIGRATING,
        /** Operator-stopped. No work is scheduled. */
        PAUSED,
        /** Serving stale or partial data: past its freshness SLA, or sync is failing. */
        DEGRADED,
        /** Draining. Scheduling has stopped, in-flight work is allowed to finish. */
        RETIRING,
        /** Terminal. */
        RETIRED;

        /**
         * The whole state machine. Declared inside the enum rather than on the entity so that it is
         * initialized by the same class initializer as the constants -- a map held on the outer
         * class can be observed null by code that touched only {@code State}.
         */
        private static final Map<State, Set<State>> ALLOWED = new EnumMap<>(State.class);

        static {
            // Every state except RETIRED can be paused or retired: an operator must always be able
            // to stop a dataset that is burning inference budget, whatever it is currently doing.
            ALLOWED.put(REGISTERED, Set.of(VALIDATED, PAUSED, RETIRING, RETIRED));
            ALLOWED.put(VALIDATED, Set.of(BACKFILLING, PAUSED, RETIRING, RETIRED));
            ALLOWED.put(BACKFILLING, Set.of(LIVE, DEGRADED, PAUSED, RETIRING));
            ALLOWED.put(LIVE, Set.of(MIGRATING, DEGRADED, PAUSED, RETIRING));
            // MIGRATING returns to LIVE when the candidate configuration is promoted. It never goes
            // to BACKFILLING: the migration is itself the backfill of the candidate.
            ALLOWED.put(MIGRATING, Set.of(LIVE, DEGRADED, PAUSED, RETIRING));
            // A degraded dataset can go back to BACKFILLING because the repair for missing coverage
            // is to re-run the plan, and content-keyed dedup makes that cost novel content only.
            ALLOWED.put(DEGRADED, Set.of(LIVE, BACKFILLING, PAUSED, RETIRING));
            // Resume targets are derived from the watermark by the service, not chosen by a caller.
            ALLOWED.put(PAUSED, Set.of(VALIDATED, BACKFILLING, LIVE, MIGRATING, RETIRING, RETIRED));
            ALLOWED.put(RETIRING, Set.of(RETIRED));
            ALLOWED.put(RETIRED, Set.of());
        }

        public boolean isTerminal() {
            return ALLOWED.get(this).isEmpty();
        }

        public boolean canTransitionTo(State next) {
            return next != null && ALLOWED.get(this).contains(next);
        }

        public Set<State> allowedTransitions() {
            return ALLOWED.get(this);
        }

        /** True when a scheduler should be handing this materialization work. */
        public boolean isSchedulable() {
            return this == VALIDATED || this == BACKFILLING || this == LIVE
                    || this == MIGRATING || this == DEGRADED;
        }
    }

    /** The states this materialization may legally move to next. */
    public Set<State> allowedTransitions() {
        return state == null ? Set.of(State.REGISTERED) : state.allowedTransitions();
    }

    /**
     * Moves to {@code next}, refusing illegal moves.
     *
     * <p>Self-transitions are rejected rather than ignored. Pausing something already paused is a
     * caller bug worth surfacing at the service boundary, where it can be answered idempotently
     * with the current state instead of being silently absorbed here.
     *
     * @throws IllegalStateException if the transition is not in {@link #ALLOWED}
     */
    public void transitionTo(State next) {
        if (next == null) {
            throw new IllegalArgumentException("next state is required");
        }
        if (state == null || !state.canTransitionTo(next)) {
            throw new IllegalStateException(String.format(
                    "Illegal materialization transition %s -> %s for %s (%s); legal: %s",
                    state, next, id, sourceTable, allowedTransitions()));
        }
        this.state = next;
        this.updatedAt = Instant.now();
    }

    public List<String> keyColumnList() {
        return splitColumns(keyColumns);
    }

    public List<String> embeddingColumnList() {
        return splitColumns(embeddingColumns);
    }

    public void setKeyColumnList(List<String> columns) {
        this.keyColumns = joinColumns(columns);
    }

    public void setEmbeddingColumnList(List<String> columns) {
        this.embeddingColumns = joinColumns(columns);
    }

    /**
     * Rebuilds the spec this row describes.
     *
     * <p>Workers must derive content hashes from a spec, not from these columns directly, so that
     * the assembled text and the configuration id come from the one implementation that defines
     * them.
     */
    public MaterializationSpec toSpec() {
        return MaterializationSpec.builder()
                .sourceTable(sourceTable)
                .keyColumns(keyColumnList())
                .embeddingColumns(embeddingColumnList())
                .joinSeparator(joinSeparator)
                .chunker(chunker)
                .chunkSize(chunkSize)
                .chunkOverlap(chunkOverlap)
                .modelName(modelName)
                .modelRevision(modelRevision)
                .embeddingVersion(embeddingVersion)
                .normalize(normalize)
                .build();
    }

    /** {@code model:version}, the embedding store and index partition value. */
    public String modelVersion() {
        return modelName + ":" + embeddingVersion;
    }

    public static String joinColumns(List<String> columns) {
        return columns == null ? "" : String.join(COLUMN_SEPARATOR, columns);
    }

    private static List<String> splitColumns(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        return Arrays.stream(csv.split(COLUMN_SEPARATOR))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toList());
    }
}
