package io.vectorsync.format.index;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * One index artifact and its complete lineage.
 *
 * <p>This is what makes a vector index a reproducible data product rather than an opaque serving
 * object: given an entry you can say exactly which source snapshot, which embedding model and
 * version, and which algorithm configuration produced it — and re-derive it.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IndexManifestEntry {

    /** Deterministic over (coverage + algorithm configuration). */
    private String indexId;

    // --- what data this index covers ---
    private String sourceTable;

    /**
     * Source snapshot this index covers. Identity only -- Iceberg snapshot ids are random longs,
     * so two entries cannot be ordered by comparing them.
     */
    private long sourceSnapshotId;

    /**
     * Iceberg's monotonic snapshot sequence number for {@link #sourceSnapshotId}. This is what
     * orders indexes by the data version they cover; without it an index built over a superseded
     * snapshot is indistinguishable from a current one.
     */
    private long sourceSequenceNumber;
    private String embeddingModel;
    private String embeddingVersion;

    /**
     * The Iceberg partition of the vector table this index covers, or null for a whole-table
     * index. Partition-scoped indexes are what make builds parallel and incremental maintenance
     * mean "rebuild the affected partitions" rather than "rebuild everything".
     */
    private String partitionValue;

    // --- how it was built ---
    private String indexAlgorithm;
    private Map<String, String> indexParams;
    private String similarityMetric;
    private int dimension;

    // --- where the artifact lives ---
    /** Object-storage prefix holding the artifact files. */
    private String indexUri;
    private List<String> indexFiles;
    private long vectorCount;

    // --- lifecycle ---
    private IndexStatus status;
    private Map<String, String> evalMetrics;

    /** When the index artifact was built. Never rewritten by later status or eval updates. */
    private Instant builtAt;

    /**
     * When this manifest row was written. Stamped by the store on every put, and what resolution
     * orders by, so a later status change or evaluation update supersedes the build row without
     * losing the original build time.
     */
    private Instant updatedAt;

    private String errorMessage;

    public String modelVersion() {
        return embeddingModel + ":" + embeddingVersion;
    }

    public boolean isServable() {
        return status == IndexStatus.READY || status == IndexStatus.ARCHIVED;
    }
}
