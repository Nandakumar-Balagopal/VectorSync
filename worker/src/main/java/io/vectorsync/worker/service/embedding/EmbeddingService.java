package io.vectorsync.worker.service.embedding;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface EmbeddingService {

    /** Embeds with the provider's configured default model. */
    List<Double> generateEmbedding(String text) throws EmbeddingException;

    /**
     * Embeds with an explicitly named model, so different tables can use different models.
     * Defaults to ignoring the name, which suits providers that serve a single model.
     */
    default List<Double> generateEmbedding(String text, String modelName) throws EmbeddingException {
        return generateEmbedding(text);
    }

    /**
     * Embeds a batch, returning embeddings keyed by {@link EmbeddingRequest#vectorId()}.
     *
     * <p>Keyed rather than positional because the embedding service groups records by provider and
     * model, so responses may not come back in request order.
     *
     * @throws EmbeddingException if any text fails; the batch is all-or-nothing so a partial
     *         result can never be mistaken for a complete materialization
     */
    default Map<String, List<Double>> generateEmbeddings(List<EmbeddingRequest> requests)
            throws EmbeddingException {
        Map<String, List<Double>> embeddings = new LinkedHashMap<>();
        for (EmbeddingRequest request : requests) {
            embeddings.put(request.vectorId(), generateEmbedding(request.text(), request.modelName()));
        }
        return embeddings;
    }
}
