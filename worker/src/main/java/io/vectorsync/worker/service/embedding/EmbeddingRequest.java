package io.vectorsync.worker.service.embedding;

/**
 * One text to embed, with the source identity the embedding service requires and the model that
 * must produce it.
 *
 * <p>{@code vectorId} is derivable before the embedding exists, because identity is keyed on
 * source lineage rather than on the vector itself. That makes it usable as the correlation key
 * for a batch response.
 *
 * <p>{@code modelName} comes from the table's configuration, not from a service-wide default.
 * Without it, every table was embedded by whatever EMBEDDING_EXTERNAL_MODEL happened to be set
 * to while the manifest recorded the configured model -- so the recorded lineage was wrong.
 */
public record EmbeddingRequest(String vectorId,
                               String sourceTable,
                               String sourceRowId,
                               String modelName,
                               String text) {
}
