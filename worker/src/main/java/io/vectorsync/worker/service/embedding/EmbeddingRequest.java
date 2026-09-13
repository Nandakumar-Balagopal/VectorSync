package io.vectorsync.worker.service.embedding;

/**
 * One text to embed, with the source identity the embedding service requires.
 *
 * <p>{@code vectorId} is derivable before the embedding exists, because identity is keyed on
 * source lineage rather than on the vector itself. That makes it usable as the correlation key
 * for a batch response, which is what the batch API expects.
 */
public record EmbeddingRequest(String vectorId,
                               String sourceTable,
                               String sourceRowId,
                               String text) {
}
