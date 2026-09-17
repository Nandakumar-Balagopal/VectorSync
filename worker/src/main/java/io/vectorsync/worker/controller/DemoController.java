package io.vectorsync.worker.controller;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.worker.client.ControlApiClient;
import io.vectorsync.worker.service.DemoSeedService;
import io.vectorsync.worker.service.SyncOrchestrationService;
import io.vectorsync.worker.service.VectorStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/demo")
@Slf4j
public class DemoController {

    private final DemoSeedService demoSeedService;
    private final ControlApiClient controlApiClient;
    private final SyncOrchestrationService syncOrchestrationService;
    private final VectorStoreService vectorStoreService;

    public DemoController(DemoSeedService demoSeedService,
                          ControlApiClient controlApiClient,
                          SyncOrchestrationService syncOrchestrationService,
                          VectorStoreService vectorStoreService) {
        this.demoSeedService = demoSeedService;
        this.controlApiClient = controlApiClient;
        this.syncOrchestrationService = syncOrchestrationService;
        this.vectorStoreService = vectorStoreService;
    }

    @PostMapping("/seed")
    public ResponseEntity<DemoSeedService.DemoSeedResult> seed() {
        return ResponseEntity.ok(demoSeedService.seedProductsTable());
    }

    /** Seeds an arbitrary source table, for exercising the flow across several tables. */
    /** Copy-on-write replace, for the mutation benchmark. See DemoSeedService.replaceRows. */
    @PostMapping("/tables/replace")
    public ResponseEntity<?> replaceTable(@RequestBody SeedTableRequest request) {
        try {
            return ResponseEntity.ok(
                    demoSeedService.replaceRows(request.tableName(), request.rows()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    @PostMapping("/tables")
    public ResponseEntity<?> seedTable(@RequestBody SeedTableRequest request) {
        try {
            return ResponseEntity.ok(demoSeedService.seedTable(request.tableName(), request.rows()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    public record SeedTableRequest(String tableName, List<DemoSeedService.SeedRow> rows) {
    }

    /** Appends to an existing table so its snapshot ancestry survives, unlike a re-seed. */
    @PostMapping("/tables/append")
    public ResponseEntity<?> appendToTable(@RequestBody SeedTableRequest request) {
        try {
            return ResponseEntity.ok(demoSeedService.appendToTable(request.tableName(), request.rows()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    @PostMapping("/products")
    public ResponseEntity<DemoSeedService.DemoMutationResult> appendProduct(@RequestBody DemoProductRequest request) {
        return ResponseEntity.ok(demoSeedService.appendProduct(
                request.id(),
                request.name(),
                request.description(),
                request.category(),
                request.price()
        ));
    }

    @PostMapping("/products/{id}")
    public ResponseEntity<DemoSeedService.DemoMutationResult> updateProduct(@PathVariable("id") String id,
                                                                            @RequestBody DemoProductRequest request) {
        return ResponseEntity.ok(demoSeedService.updateProduct(
                id,
                request.name(),
                request.description(),
                request.category(),
                request.price()
        ));
    }

    @DeleteMapping("/products/{id}")
    public ResponseEntity<DemoSeedService.DemoMutationResult> deleteProduct(@PathVariable("id") String id) {
        return ResponseEntity.ok(demoSeedService.deleteProduct(id));
    }

    @PostMapping("/sync")
    public ResponseEntity<Map<String, Object>> sync() {
        List<TableConfig> tableConfigs = controlApiClient.getTableConfigs();
        int synced = 0;

        for (TableConfig config : tableConfigs) {
            if (config.isEnabled()) {
                syncOrchestrationService.syncTable(config);
                synced++;
            }
        }

        long vectorCount = vectorStoreService.getVectorCount();
        Map<String, Object> response = new HashMap<>();
        response.put("tablesSynced", synced);
        response.put("vectorCount", vectorCount);
        return ResponseEntity.ok(response);
    }

    public record DemoProductRequest(String id,
                                     String name,
                                     String description,
                                     String category,
                                     Double price) {
    }
}
