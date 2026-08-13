package io.vectorsync.embeddingworker.provider;

import io.vectorsync.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Mock embedding provider for testing and development.
 * Generates random normalized vectors.
 */
@Service
@Slf4j
@ConditionalOnProperty(name = "embedding.provider", havingValue = "mock", matchIfMissing = true)
public class MockEmbeddingProvider implements EmbeddingProvider {

    private final Random random = new Random();

    @Override
    public List<Double> generateEmbedding(String text) throws EmbeddingException {
        if (text == null || text.isBlank()) {
            throw new EmbeddingException("Text cannot be null or empty");
        }

        log.debug("Generating mock embedding for text: {}", text.substring(0, Math.min(50, text.length())));

        List<Double> embedding = new ArrayList<>();
        for (int i = 0; i < Constants.EMBEDDING_DIMENSION; i++) {
            embedding.add(random.nextGaussian());
        }

        normalizeVector(embedding);
        return embedding;
    }

    private void normalizeVector(List<Double> vector) {
        double magnitude = 0.0;
        for (Double value : vector) {
            magnitude += value * value;
        }
        magnitude = Math.sqrt(magnitude);

        if (magnitude > 0) {
            for (int i = 0; i < vector.size(); i++) {
                vector.set(i, vector.get(i) / magnitude);
            }
        }
    }
}

// Made with Bob
