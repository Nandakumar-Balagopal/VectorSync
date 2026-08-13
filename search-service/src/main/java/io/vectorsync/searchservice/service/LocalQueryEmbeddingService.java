package io.vectorsync.searchservice.service;

import io.vectorsync.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
@Slf4j
@ConditionalOnProperty(name = "embedding.provider", havingValue = "local")
public class LocalQueryEmbeddingService implements QueryEmbeddingService {

    @Value("${embedding.local.model-path:}")
    private String modelPath;

    @Value("${embedding.local.model-name:local-embedding-v1}")
    private String modelName;

    private final Random random = new Random();

    @Override
    public List<Double> generateEmbedding(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Text cannot be null or empty");
        }

        if (modelPath == null || modelPath.isBlank()) {
            throw new IllegalArgumentException("Local embedding provider is selected but model-path is missing");
        }

        log.warn("Local embedding provider configured for model {} at {}. Using mock output until local runtime is wired.",
                modelName, modelPath);

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
