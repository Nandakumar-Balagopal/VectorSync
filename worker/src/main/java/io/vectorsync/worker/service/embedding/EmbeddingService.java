package io.vectorsync.worker.service.embedding;

import java.util.List;

public interface EmbeddingService {
    List<Double> generateEmbedding(String text) throws EmbeddingException;
}
