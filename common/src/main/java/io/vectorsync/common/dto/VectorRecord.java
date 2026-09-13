package io.vectorsync.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * One stored embedding, carrying the full lineage needed to reproduce it.
 *
 * <p>The embedding is held as {@code List<Double>} for Java-side convenience (providers return
 * doubles), but is stored as float32 on disk. No precision is lost: embedding models emit float32.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VectorRecord {

    /**
     * Deterministic identity derived from the lineage fields, so re-running a materialization
     * produces the same id and retries are idempotent rather than duplicating rows.
     */
    private String vectorId;

    // --- source lineage ---
    private String sourceTable;
    private String sourceRowId;

    /** Iceberg snapshot of the source table this embedding was derived from. */
    private long sourceSnapshotId;

    /** Position within the source row's chunk sequence; 0 when the whole row is one chunk. */
    private int chunkOrdinal;

    // --- embedding lineage ---
    private String embeddingModel;
    private String embeddingVersion;
    private int embeddingDim;

    /** Hash of the preprocessing configuration that produced {@link #text}. */
    private String preprocessingId;

    // --- payload ---
    private List<Double> embedding;
    private String text;
    private boolean deleted;

    /** User-supplied passthrough metadata. Lineage lives in typed columns, not here. */
    private Map<String, String> metadata;

    /** Wall-clock write time. Operational only; never used to decide which version is live. */
    private Instant createdAt;

    /** Partition value pairing model and version. */
    public String modelVersion() {
        return embeddingModel + ":" + embeddingVersion;
    }
}
