package io.vectorsync.controlplane.repository;

import io.vectorsync.controlplane.model.EmbeddedContentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

@Repository
public interface EmbeddedContentRepository
        extends JpaRepository<EmbeddedContentEntity, EmbeddedContentEntity.Key> {

    /**
     * The dedup probe: which of these hashes already have a durable vector.
     *
     * <p>Returns only the hashes, not the rows. The caller needs set membership and nothing else,
     * and hydrating entities for a 500-key batch to then discard everything but one column is the
     * same class of waste as decoding an embedding to answer a boolean.
     *
     * <p>Equality on the two scope columns plus {@code IN} on the hash matches the leading edge of
     * the primary key, so this is one index scan regardless of batch size.
     */
    @Query("""
            SELECT e.contentHash FROM EmbeddedContentEntity e
            WHERE e.modelVersion = :modelVersion
              AND e.configId = :configId
              AND e.contentHash IN :contentHashes
            """)
    List<String> findExistingHashes(@Param("modelVersion") String modelVersion,
                                    @Param("configId") String configId,
                                    @Param("contentHashes") Collection<String> contentHashes);

    /**
     * Records a hash, ignoring a row that is already present.
     *
     * <p>{@code ON CONFLICT DO NOTHING} rather than a read-then-write: two workers can derive the
     * same novel content concurrently and both legitimately succeed, so a conflict is the expected
     * outcome of a healthy race and not an error. It also makes retrying a completion idempotent,
     * which is what lets the caller retry a failed completion without special handling.
     */
    @Modifying
    @Query(value = """
            INSERT INTO embedded_content
                (model_version, config_id, content_hash, embedding_dim, first_seen_at)
            VALUES (:modelVersion, :configId, :contentHash, :embeddingDim, :firstSeenAt)
            ON CONFLICT (model_version, config_id, content_hash) DO NOTHING
            """, nativeQuery = true)
    int recordIfAbsent(@Param("modelVersion") String modelVersion,
                       @Param("configId") String configId,
                       @Param("contentHash") String contentHash,
                       @Param("embeddingDim") int embeddingDim,
                       @Param("firstSeenAt") Instant firstSeenAt);

    @Query("""
            SELECT COUNT(e) FROM EmbeddedContentEntity e
            WHERE e.modelVersion = :modelVersion AND e.configId = :configId
            """)
    long countForScope(@Param("modelVersion") String modelVersion,
                       @Param("configId") String configId);
}
