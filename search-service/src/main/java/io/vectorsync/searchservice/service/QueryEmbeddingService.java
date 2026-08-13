package io.vectorsync.searchservice.service;

import java.util.List;

public interface QueryEmbeddingService {
    List<Double> generateEmbedding(String text) throws Exception;
}
