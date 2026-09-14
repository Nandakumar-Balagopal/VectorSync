package io.vectorsync.worker.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.worker.config.HttpClientConfig;
import io.vectorsync.worker.service.iceberg.SourceFileWork;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The worker's view of the control plane: materializations to run, and the queue they run through.
 *
 * <p>The split it crosses is deliberate. Planning needs the source catalog, which only the data
 * plane reaches; the work queue needs durable transactional state, which only the control plane has.
 * So the worker plans and hands a file list over, then leases it back one batch at a time. The
 * alternative -- the control plane planning directly against every source catalog -- would put
 * object-storage IO inside the transaction boundary that holds the queue.
 */
@Component
@Slf4j
public class DerivationControlClient {

    @Value("${control.api.url:http://localhost:8080}")
    private String controlApiUrl;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public DerivationControlClient(
            @Qualifier(HttpClientConfig.CONTROL_API_CLIENT) RestTemplate restTemplate,
            ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * A materialization as the worker needs it: the spec to derive by, plus the anchor and watermark
     * that say where to start.
     */
    @Data
    @Builder
    public static class Materialization {
        private String id;
        private String state;
        private String sourceTable;
        private String configId;
        private long anchorSnapshotId;
        private long anchorSequenceNumber;
        private long incrementalWatermark;
        private MaterializationSpec spec;
    }

    @Data
    @Builder
    public static class LeasedItem {
        private String id;
        private String materializationId;
        private String sourceTable;
        private String dataFilePath;
        private long recordCount;
        private long snapshotId;
        private long sequenceNumber;
        private long committedAtMillis;
        private String kind;
    }

    @Data
    @Builder
    public static class QueueDepth {
        private long pending;
        private long leased;
        private long done;
        private long failed;

        /** Nothing queued or in flight. Says nothing about success. */
        public boolean drained() {
            return pending == 0 && leased == 0;
        }

        /** Drained with no terminal failures: the only state that may advance a watermark. */
        public boolean complete() {
            return drained() && failed == 0;
        }
    }

    /** Materializations the worker should act on, in the states where work is expected. */
    public List<Materialization> runnable() {
        List<Materialization> runnable = new ArrayList<>();
        for (String state : List.of("VALIDATED", "BACKFILLING", "LIVE")) {
            runnable.addAll(list(state));
        }
        return runnable;
    }

    private List<Materialization> list(String state) {
        try {
            JsonNode body = restTemplate.exchange(
                    controlApiUrl + "/api/materializations?state=" + state,
                    HttpMethod.GET, null, JsonNode.class).getBody();

            List<Materialization> parsed = new ArrayList<>();
            if (body != null && body.isArray()) {
                for (JsonNode node : body) {
                    Materialization materialization = toMaterialization(node);
                    if (materialization != null) {
                        parsed.add(materialization);
                    }
                }
            }
            return parsed;
        } catch (Exception e) {
            log.warn("Could not list {} materializations: {}", state, e.getMessage());
            return List.of();
        }
    }

    /**
     * Rebuilds the spec from the persisted row.
     *
     * <p>Skips rather than throws on a row it cannot parse. One malformed materialization must not
     * stop the worker from servicing every other one, which is the same reason the scheduler
     * isolates failures per table.
     */
    private Materialization toMaterialization(JsonNode node) {
        try {
            MaterializationSpec spec = MaterializationSpec.builder()
                    .sourceTable(text(node, "sourceTable"))
                    .keyColumns(csv(text(node, "keyColumns")))
                    .embeddingColumns(csv(text(node, "embeddingColumns")))
                    .joinSeparator(text(node, "joinSeparator"))
                    .chunker(text(node, "chunker"))
                    .chunkSize(node.path("chunkSize").asInt(0))
                    .chunkOverlap(node.path("chunkOverlap").asInt(0))
                    .modelName(text(node, "modelName"))
                    .modelRevision(text(node, "modelRevision"))
                    .embeddingVersion(text(node, "embeddingVersion"))
                    .normalize(node.path("normalize").asBoolean(false))
                    .build();

            String persistedConfigId = text(node, "configId");
            if (persistedConfigId != null && !persistedConfigId.equals(spec.configId())) {
                // The row and the spec rebuilt from it must agree, or the worker would write into a
                // different partition than the one the materialization was admitted under and its
                // vectors would be invisible to everything that reads by the persisted id.
                log.error("Materialization {} has configId {} but its fields hash to {}; skipping",
                        text(node, "id"), persistedConfigId, spec.configId());
                return null;
            }

            return Materialization.builder()
                    .id(text(node, "id"))
                    .state(text(node, "state"))
                    .sourceTable(spec.getSourceTable())
                    .configId(spec.configId())
                    .anchorSnapshotId(node.path("anchorSnapshotId").asLong(0L))
                    .anchorSequenceNumber(node.path("anchorSequenceNumber").asLong(0L))
                    .incrementalWatermark(node.path("incrementalWatermark").asLong(0L))
                    .spec(spec)
                    .build();
        } catch (Exception e) {
            log.warn("Skipping unparseable materialization: {}", e.getMessage());
            return null;
        }
    }

    /** @return number of newly queued items; repeats of already-queued files are not counted */
    public int enqueue(String materializationId, List<SourceFileWork> work, String kind) {
        if (work.isEmpty()) {
            return 0;
        }

        List<Map<String, Object>> descriptors = new ArrayList<>(work.size());
        for (SourceFileWork file : work) {
            Map<String, Object> descriptor = new LinkedHashMap<>();
            descriptor.put("sourceTable", file.sourceTable());
            descriptor.put("dataFilePath", file.dataFilePath());
            descriptor.put("recordCount", file.recordCount());
            descriptor.put("snapshotId", file.snapshotId());
            descriptor.put("sequenceNumber", file.sequenceNumber());
            descriptor.put("committedAtMillis", file.committedAtMillis());
            descriptor.put("kind", kind);
            descriptors.add(descriptor);
        }

        JsonNode response = post("/api/queue/enqueue", Map.of(
                "materializationId", materializationId,
                "descriptors", descriptors));
        return response == null ? 0 : response.path("inserted").asInt(0);
    }

    public List<LeasedItem> lease(String owner, int limit, long leaseSeconds, String materializationId) {
        JsonNode response = post("/api/queue/lease", Map.of(
                "owner", owner, "limit", limit, "leaseSeconds", leaseSeconds,
                "materializationId", materializationId));

        List<LeasedItem> items = new ArrayList<>();
        if (response == null) {
            return items;
        }
        JsonNode array = response.isArray() ? response : response.path("items");
        for (JsonNode node : array) {
            items.add(LeasedItem.builder()
                    .id(text(node, "id"))
                    .materializationId(text(node, "materializationId"))
                    .sourceTable(text(node, "sourceTable"))
                    .dataFilePath(text(node, "dataFilePath"))
                    .recordCount(node.path("recordCount").asLong(0L))
                    .snapshotId(node.path("snapshotId").asLong(0L))
                    .sequenceNumber(node.path("sequenceNumber").asLong(0L))
                    .committedAtMillis(node.path("committedAtMillis").asLong(0L))
                    .kind(text(node, "kind"))
                    .build());
        }
        return items;
    }

    public void complete(String itemId) {
        post("/api/queue/" + itemId + "/complete", Map.of());
    }

    public void fail(String itemId, String owner, String error) {
        post("/api/queue/" + itemId + "/fail", Map.of("owner", owner, "error", truncate(error)));
    }

    public QueueDepth depth(String materializationId) {
        try {
            JsonNode body = restTemplate.exchange(
                    controlApiUrl + "/api/queue/stats?materializationId=" + materializationId,
                    HttpMethod.GET, null, JsonNode.class).getBody();
            if (body == null) {
                return null;
            }
            return QueueDepth.builder()
                    .pending(body.path("pending").asLong(0L))
                    .leased(body.path("leased").asLong(0L))
                    .done(body.path("done").asLong(0L))
                    .failed(body.path("failed").asLong(0L))
                    .build();
        } catch (Exception e) {
            log.warn("Could not read queue depth for {}: {}", materializationId, e.getMessage());
            return null;
        }
    }

    public void beginBackfill(String id) {
        post("/api/materializations/" + id + "/backfill", Map.of());
    }

    /**
     * @return true only when the control plane confirmed the transition
     *
     * <p>{@link #post} swallows transport and 4xx/5xx failures into a warning, which is right for a
     * fire-and-forget call and wrong here: the caller reports "watermark advanced" from this, and a
     * swallowed failure made that metric claim progress the control plane never recorded.
     */
    public boolean markLive(String id, long watermark) {
        return post("/api/materializations/" + id + "/live?watermark=" + watermark, Map.of()) != null;
    }

    public void markDegraded(String id, String reason) {
        post("/api/materializations/" + id + "/degraded?reason="
                + java.net.URLEncoder.encode(truncate(reason), java.nio.charset.StandardCharsets.UTF_8),
                Map.of());
    }

    private JsonNode post(String path, Object body) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            String json = objectMapper.writeValueAsString(body);
            return restTemplate.exchange(controlApiUrl + path, HttpMethod.POST,
                    new HttpEntity<>(json, headers),
                    new ParameterizedTypeReference<JsonNode>() {}).getBody();
        } catch (Exception e) {
            log.warn("POST {} failed: {}", path, e.getMessage());
            return null;
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isMissingNode() || value.isNull() ? null : value.asText();
    }

    private static List<String> csv(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<String> parts = new ArrayList<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                parts.add(trimmed);
            }
        }
        return parts;
    }

    private static String truncate(String value) {
        if (value == null) {
            return "";
        }
        return value.length() <= 2000 ? value : value.substring(0, 2000);
    }
}
