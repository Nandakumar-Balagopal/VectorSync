package io.vectorsync.worker.service.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@ConditionalOnProperty(name = "embedding.provider", havingValue = "external")
public class ExternalEmbeddingService implements EmbeddingService {

    @Value("${embedding.external.type:gemini}")
    private String providerType;

    @Value("${embedding.external.api-url:}")
    private String apiUrl;

    @Value("${embedding.external.api-key:}")
    private String apiKey;

    @Value("${embedding.external.model:gemini-embedding-001}")
    private String modelName;

    @Value("${embedding.external.http.api-key-header:}")
    private String apiKeyHeader;

    @Value("${embedding.external.http.api-key-prefix:}")
    private String apiKeyPrefix;

    @Value("${embedding.external.http.request-template:}")
    private String requestTemplate;

    @Value("${embedding.external.http.response-json-pointer:}")
    private String responseJsonPointer;

    /**
     * Batch endpoint of the VectorSync Python embedding service. When set, a whole snapshot's
     * texts are embedded in one request instead of one request per row.
     */
    @Value("${embedding.external.batch-api-url:}")
    private String batchApiUrl;

    @Value("${embedding.external.batch-size:64}")
    private int batchSize;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public ExternalEmbeddingService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.restTemplate = new RestTemplate();
    }

    /**
     * Uses the batch endpoint when one is configured, falling back to per-text calls otherwise so
     * that arbitrary managed providers still work.
     */
    @Override
    public List<List<Double>> generateEmbeddings(List<String> texts) throws EmbeddingException {
        if (texts.isEmpty()) {
            return List.of();
        }
        if (batchApiUrl == null || batchApiUrl.isBlank()) {
            return EmbeddingService.super.generateEmbeddings(texts);
        }

        List<List<Double>> embeddings = new ArrayList<>(texts.size());
        for (int start = 0; start < texts.size(); start += batchSize) {
            embeddings.addAll(callBatchApi(texts.subList(start, Math.min(start + batchSize, texts.size()))));
        }
        return embeddings;
    }

    /**
     * Speaks the Python service's {@code /api/v1/embed} contract. Results are correlated by the
     * vector_id echoed back, not by position, because the service groups records by provider and
     * model and so may reorder them.
     */
    private List<List<Double>> callBatchApi(List<String> texts) throws EmbeddingException {
        try {
            List<Map<String, String>> records = new ArrayList<>(texts.size());
            for (int i = 0; i < texts.size(); i++) {
                records.add(Map.of(
                        "vector_id", String.valueOf(i),
                        "text", texts.get(i),
                        "model_name", modelName,
                        "provider", "self_hosted"));
            }

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            applyApiKeyHeader(headers);

            String body = objectMapper.writeValueAsString(Map.of(
                    "request_id", UUID.randomUUID().toString(),
                    "records", records));

            String response = restTemplate.postForObject(
                    batchApiUrl, new HttpEntity<>(body, headers), String.class);

            JsonNode results = objectMapper.readTree(response).at("/results");
            if (!results.isArray()) {
                throw new EmbeddingException("Invalid batch embedding response: missing /results");
            }

            Map<String, List<Double>> byId = new HashMap<>();
            for (JsonNode result : results) {
                String error = result.path("error").asText(null);
                if (error != null && !error.isBlank()) {
                    throw new EmbeddingException("Batch embedding failed: " + error);
                }
                byId.put(result.path("vector_id").asText(), objectMapper.convertValue(
                        result.path("embedding"),
                        objectMapper.getTypeFactory().constructCollectionType(List.class, Double.class)));
            }

            List<List<Double>> ordered = new ArrayList<>(texts.size());
            for (int i = 0; i < texts.size(); i++) {
                List<Double> embedding = byId.get(String.valueOf(i));
                if (embedding == null || embedding.isEmpty()) {
                    throw new EmbeddingException("Batch embedding response missing entry " + i);
                }
                ordered.add(embedding);
            }
            return ordered;
        } catch (EmbeddingException e) {
            throw e;
        } catch (Exception e) {
            throw new EmbeddingException("Batch embedding request failed", e);
        }
    }

    @Override
    public List<Double> generateEmbedding(String text) throws EmbeddingException {
        if (text == null || text.isBlank()) {
            throw new EmbeddingException("Text cannot be null or empty");
        }

        if (apiUrl == null || apiUrl.isBlank() || apiKey == null || apiKey.isBlank()) {
            throw new EmbeddingException("External embedding provider is selected but api-url or api-key is missing");
        }

        try {
            String normalizedType = providerType == null ? "" : providerType.trim().toLowerCase();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            String body;
            switch (normalizedType) {
                case "openai":
                    headers.set("Authorization", "Bearer " + apiKey);
                    body = "{\"model\":\"" + escape(modelName) + "\",\"input\":\"" + escape(text) + "\"}";
                    return parseEmbedding(responseJsonPointer, restTemplate.postForObject(apiUrl, new HttpEntity<>(body, headers), String.class));
                case "http":
                    applyApiKeyHeader(headers);
                    body = buildTemplateBody(text);
                    return parseEmbedding(responseJsonPointer, restTemplate.postForObject(apiUrl, new HttpEntity<>(body, headers), String.class));
                case "gemini":
                default:
                    headers.set("X-goog-api-key", apiKey);
                    body = "{\"model\":\"models/" + escape(modelName) + "\",\"content\":{\"parts\":[{\"text\":\"" + escape(text) + "\"}]}}";
                    return parseEmbedding("/embedding/values", restTemplate.postForObject(apiUrl, new HttpEntity<>(body, headers), String.class));
            }
        } catch (Exception e) {
            throw new EmbeddingException("External embedding request failed", e);
        }
    }

    private List<Double> parseEmbedding(String jsonPointer, String response) throws Exception {
        JsonNode root = objectMapper.readTree(response);
        String pointer = jsonPointer == null || jsonPointer.isBlank() ? "/embedding/values" : jsonPointer;
        JsonNode valuesNode = root.at(pointer);

        if (!valuesNode.isArray()) {
            throw new EmbeddingException("Invalid embedding response: missing values at " + pointer);
        }

        return objectMapper.convertValue(
                valuesNode,
                objectMapper.getTypeFactory().constructCollectionType(List.class, Double.class)
        );
    }

    private void applyApiKeyHeader(HttpHeaders headers) {
        if (apiKeyHeader == null || apiKeyHeader.isBlank()) {
            return;
        }
        String value = apiKeyPrefix == null || apiKeyPrefix.isBlank()
                ? apiKey
                : apiKeyPrefix + apiKey;
        headers.set(apiKeyHeader, value);
    }

    private String buildTemplateBody(String text) {
        if (requestTemplate == null || requestTemplate.isBlank()) {
            return "{\"input\":\"" + escape(text) + "\"}";
        }
        return requestTemplate
                .replace("__TEXT__", escape(text))
                .replace("${text}", escape(text))
                .replace("{text}", escape(text));
    }

    private String escape(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
