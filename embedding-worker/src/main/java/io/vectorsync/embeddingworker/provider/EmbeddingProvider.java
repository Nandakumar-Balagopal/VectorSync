package io.vectorsync.embeddingworker.provider;

import java.util.List;

/**
 * Interface for embedding generation providers.
 * Implementations can use mock embeddings, external APIs (OpenAI, Gemini), or local models.
 */
public interface EmbeddingProvider {
    /**
     * Generate an embedding vector for the given text.
     * 
     * @param text The input text to embed
     * @return A list of doubles representing the embedding vector
     * @throws EmbeddingException if embedding generation fails
     */
    List<Double> generateEmbedding(String text) throws EmbeddingException;
}

// Made with Bob
