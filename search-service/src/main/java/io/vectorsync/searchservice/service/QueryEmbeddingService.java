package io.vectorsync.searchservice.service;

import java.util.List;

public interface QueryEmbeddingService {

    List<Double> generateEmbedding(String text) throws Exception;

    /**
     * Embeds a query with a named model.
     *
     * <p>The query must be embedded by the same model that produced the vectors being searched.
     * Mixing models is meaningless even when dimensions happen to agree, and across models of
     * different width it is a hard mismatch.
     */
    default List<Double> generateEmbedding(String text, String modelName) throws Exception {
        return generateEmbedding(text);
    }
}
