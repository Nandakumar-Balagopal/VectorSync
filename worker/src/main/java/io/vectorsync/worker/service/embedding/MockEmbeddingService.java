package io.vectorsync.worker.service.embedding;

import io.vectorsync.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

@Service
@Slf4j
@ConditionalOnProperty(name = "embedding.provider", havingValue = "mock", matchIfMissing = true)
public class MockEmbeddingService implements EmbeddingService {

    /**
     * Mock embeddings must be a pure function of their input, like the real thing.
     *
     * <p>This used a shared unseeded {@link Random}, so the same text produced a different vector on
     * every call. That is not a weaker mock, it is a mock that violates the invariant the whole
     * system is built on: content addressing assumes one text under one model has one vector, and a
     * non-deterministic embedder silently breaks every determinism guarantee while appearing to
     * work. It also made reproducibility untestable, since no two runs could agree.
     *
     * <p>Seeding per call from the text and model gives a stable vector per input, which is the
     * property that matters, without pretending to carry semantic meaning.
     */
    private static Random seededFor(String text, String model) {
        return new Random(((long) Objects.hashCode(text) << 32) ^ Objects.hashCode(model));
    }

    @Override
    public List<Double> generateEmbedding(String text) throws EmbeddingException {
        if (text == null || text.isBlank()) {
            throw new EmbeddingException("Text cannot be null or empty");
        }

        log.debug("Generating mock embedding for text: {}", text.substring(0, Math.min(50, text.length())));

        return generateEmbedding(text, null);
    }

    @Override
    public List<Double> generateEmbedding(String text, String model) throws EmbeddingException {
        if (text == null || text.isBlank()) {
            throw new EmbeddingException("Text cannot be null or empty");
        }

        Random random = seededFor(text, model);
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
