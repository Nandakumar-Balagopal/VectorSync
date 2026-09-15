package io.vectorsync.controlplane.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * Authoritative record that a piece of content has a durable embedding.
 *
 * <p>This exists because a process-local set cannot answer the question it was being asked. Its
 * positives assumed every commit the process believed it made had survived, and when one had not it
 * reported a cache hit forever: inference was skipped and the content map committed a pointer to a
 * vector that does not exist. Nothing at runtime could detect or repair that, and the deduplication
 * property this project measures held only for a single worker.
 *
 * <p>Rows are written in the same transaction that marks a work item DONE, which is what makes the
 * two facts inseparable. Because the Iceberg appends precede completion, the error is
 * one-directional: this table may under-report, costing a redundant and byte-identical embedding on
 * retry, but it can never over-report, which would cost a missing vector.
 */
@Entity
@Table(name = "embedded_content")
@IdClass(EmbeddedContentEntity.Key.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EmbeddedContentEntity {

    @Id
    @Column(name = "model_version", nullable = false, length = 255)
    private String modelVersion;

    @Id
    @Column(name = "config_id", nullable = false, length = 64)
    private String configId;

    @Id
    @Column(name = "content_hash", nullable = false, length = 64)
    private String contentHash;

    /**
     * Recorded so a reader can reject a vector of the wrong width without opening the store.
     *
     * <p>Storing the dimension in the value rather than folding it into the key is deliberate: the
     * key must stay exactly the derivation identity, and a dimension mismatch for a key that should
     * be stable is a defect to surface, not a second cache entry to create.
     */
    @Column(name = "embedding_dim", nullable = false)
    private int embeddingDim;

    @Column(name = "first_seen_at", nullable = false)
    private Instant firstSeenAt;

    /** Composite key in probe order: the equality-scoped columns lead so one index scan serves a batch. */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Key implements Serializable {
        private String modelVersion;
        private String configId;
        private String contentHash;
    }
}
