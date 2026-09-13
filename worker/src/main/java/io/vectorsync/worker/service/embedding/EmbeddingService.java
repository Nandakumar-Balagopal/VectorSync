package io.vectorsync.worker.service.embedding;

import java.util.ArrayList;
import java.util.List;

public interface EmbeddingService {

    List<Double> generateEmbedding(String text) throws EmbeddingException;

    /**
     * Embeds a batch of texts, returning one embedding per input in the same order.
     *
     * <p>The default loops, which is correct but slow: materializing a snapshot used to make one
     * HTTP round trip per changed row. Providers that can embed a batch in one call should
     * override this.
     *
     * @throws EmbeddingException if any text fails; the batch is all-or-nothing so a partial
     *         result can never be mistaken for a complete materialization
     */
    default List<List<Double>> generateEmbeddings(List<String> texts) throws EmbeddingException {
        List<List<Double>> embeddings = new ArrayList<>(texts.size());
        for (String text : texts) {
            embeddings.add(generateEmbedding(text));
        }
        return embeddings;
    }
}
