package io.vectorsync.worker.client;

import io.vectorsync.common.dto.TableConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class ControlApiClient {

    @Value("${control.api.url:http://localhost:8080}")
    private String controlApiUrl;

    private final RestTemplate restTemplate;

    public ControlApiClient() {
        this.restTemplate = new RestTemplate();
    }

    public List<TableConfig> getTableConfigs() {
        try {
            String url = controlApiUrl + "/api/tables";
            ResponseEntity<List<TableConfig>> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<List<TableConfig>>() {}
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.warn("Could not fetch table configs from control-api: {}", e.getMessage());
        }

        return List.of();
    }

    public SyncStateResponse getSyncState(String tableId) {
        try {
            String url = controlApiUrl + "/api/sync-state/" + tableId;
            return restTemplate.getForObject(url, SyncStateResponse.class);
        } catch (Exception e) {
            log.warn("Could not fetch sync state for {}: {}", tableId, e.getMessage());
            return null;
        }
    }

    public void updateSyncState(String tableId, long snapshotId, java.time.Instant syncTime) {
        try {
            String url = controlApiUrl + "/api/sync-state/" + tableId;
            Map<String, Object> payload = Map.of(
                    "lastSnapshotId", snapshotId,
                    "lastSyncAt", syncTime
            );

            restTemplate.exchange(url, HttpMethod.POST, new HttpEntity<>(payload), Void.class);
        } catch (Exception e) {
            log.warn("Could not update sync state for {}: {}", tableId, e.getMessage());
        }
    }
}
