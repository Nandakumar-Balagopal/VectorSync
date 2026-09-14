package io.vectorsync.format.derive;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.Instant;

/**
 * One vector in the embedding store: the embedding of a specific piece of content under a specific
 * model and configuration.
 *
 * <p>There is no row identity here, and that absence is the point. An embedding is a pure function
 * of {@code (content, model, configuration)}, so an entry is valid for every source row whose text
 * hashes to {@link #contentHash} -- in this table, in another table, now or after a schema change.
 * Rows are attached to vectors by {@link ContentMap}, not by this table.
 *
 * <p>Entries are immutable once written. A different model or configuration produces a different
 * store key rather than replacing anything, so two model versions coexist and a rollback is a read
 * of the older key rather than a re-embed.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EmbeddingEntry {

    /** Full SHA-256 hex of the canonical text, from {@link ContentHash}. */
    private String contentHash;

    /** {@code model:version}, the partition value. */
    private String modelVersion;

    /** {@link MaterializationSpec#configId()} -- everything else that shaped the input text. */
    private String configId;

    private int embeddingDim;

    /**
     * The vector itself, as primitive float32.
     *
     * <p>Not {@code List<Double>}: a 384-dim vector costs roughly 9KB as boxed doubles against
     * 1.6KB as a {@code float[]}, and a dedup pass or index build holds the whole batch resident,
     * so the boxed form is the difference between a build that fits in heap and one that does not.
     * Nothing meaningful is narrowed -- the Iceberg column is float32 and every embedding model
     * emits float32.
     */
    @ToString.Exclude
    private float[] embedding;

    /**
     * The exact text that was embedded. Optional: it is kept for debugging and for reranking that
     * wants the original passage, but a store entry is fully identified without it.
     */
    private String text;

    /** When this vector was materialized. Never used for ordering -- entries never supersede. */
    private Instant createdAt;

    /** The (content, model, config) triple this entry answers for. */
    public String storeKey() {
        return ContentHash.storeKey(contentHash, modelVersion, configId);
    }

    /** Partition bucket of {@link #contentHash}, derived rather than stored on the entry. */
    public String hashPrefix() {
        return ContentHash.prefix(contentHash);
    }
}
