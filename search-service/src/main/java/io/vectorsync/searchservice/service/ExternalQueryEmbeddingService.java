package io.vectorsync.searchservice.service;

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
import java.util.List;

@Service
@Slf4j
@ConditionalOnProperty(name = "embedding.provider", havingValue = "external")
public class ExternalQueryEmbeddingService implements QueryEmbeddingService {

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

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public ExternalQueryEmbeddingService(ObjectMapper objectMapper, RestTemplate restTemplate) {
        this.objectMapper = objectMapper;
        this.restTemplate = restTemplate;
    }

    private String resolveModel(String requested) {
        return requested == null || requested.isBlank() ? modelName : requested;
    }

    @Override
    public List<Double> generateEmbedding(String text) {
        return generateEmbedding(text, null);
    }

    @Override
    public List<Double> generateEmbedding(String text, String requestedModel) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Text cannot be null or empty");
        }

        if (apiUrl == null || apiUrl.isBlank() || apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("External embedding provider is selected but api-url or api-key is missing");
        }

        try {
            String normalizedType = providerType == null ? "" : providerType.trim().toLowerCase();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            String body;
            switch (normalizedType) {
                case "openai":
                    headers.set("Authorization", "Bearer " + apiKey);
                    body = "{\"model\":\"" + escape(resolveModel(requestedModel)) + "\",\"input\":\"" + escape(text) + "\"}";
                    return parseEmbedding(responseJsonPointer, restTemplate.postForObject(apiUrl, new HttpEntity<>(body, headers), String.class));
                case "http":
                    applyApiKeyHeader(headers);
                    body = buildTemplateBody(text, resolveModel(requestedModel));
                    return parseEmbedding(responseJsonPointer, restTemplate.postForObject(apiUrl, new HttpEntity<>(body, headers), String.class));
                case "gemini":
                default:
                    headers.set("X-goog-api-key", apiKey);
                    body = "{\"model\":\"models/" + escape(resolveModel(requestedModel)) + "\",\"content\":{\"parts\":[{\"text\":\"" + escape(text) + "\"}]}}";
                    return parseEmbedding("/embedding/values", restTemplate.postForObject(apiUrl, new HttpEntity<>(body, headers), String.class));
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("External embedding request failed", e);
        }
    }

    private List<Double> parseEmbedding(String jsonPointer, String response) throws Exception {
        JsonNode root = objectMapper.readTree(response);
        String pointer = jsonPointer == null || jsonPointer.isBlank() ? "/embedding/values" : jsonPointer;
        JsonNode valuesNode = root.at(pointer);

        if (!valuesNode.isArray()) {
            throw new IllegalArgumentException("Invalid embedding response: missing values at " + pointer);
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

    private String buildTemplateBody(String text, String model) {
        if (requestTemplate == null || requestTemplate.isBlank()) {
            return "{\"input\":\"" + escape(text) + "\"}";
        }
        return requestTemplate
                .replace("__MODEL__", escape(model))
                .replace("__TEXT__", escape(text))
                .replace("${text}", escape(text))
                .replace("{text}", escape(text));
    }

    private String escape(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
