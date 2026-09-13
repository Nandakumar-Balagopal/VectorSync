package io.vectorsync.worker.service.embedding;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface EmbeddingService {

    List<Double> generateEmbedding(String text) throws EmbeddingException;

    /**
     * Embeds a batch of texts, returning embeddings keyed by {@link EmbeddingRequest#vectorId()}.
     *
     * <p>Keyed rather than positional because the embedding service groups records by provider and
     * model, so responses may not come back in request order.
     *
     * <p>The default loops, which is correct but slow: materializing a snapshot used to make one
     * HTTP round trip per changed row. Providers that can embed a batch in one call should
     * override this.
     *
     * @throws EmbeddingException if any text fails; the batch is all-or-nothing so a partial
     *         result can never be mistaken for a complete materialization
     */
    default Map<String, List<Double>> generateEmbeddings(List<EmbeddingRequest> requests)
            throws EmbeddingException {
        Map<String, List<Double>> embeddings = new LinkedHashMap<>();
        for (EmbeddingRequest request : requests) {
            embeddings.put(request.vectorId(), generateEmbedding(request.text()));
        }
        return embeddings;
    }
}
