package io.vectorsync.searchservice.controller;

import io.vectorsync.common.Constants;
import io.vectorsync.searchservice.service.index.IndexRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Explicit, operator-initiated rebuilds of the index metadata tables.
 *
 * <p>The manifest and alias log are derived: every entry can be reproduced by rebuilding indexes
 * from the vector table. They are still dropped only on request, because the alias log is also the
 * promotion audit trail and losing it silently would be worse than an explicit refusal.
 */
@RestController
@RequestMapping("/api/admin")
@Slf4j
public class IndexAdminController {

    private final IndexRegistry registry;

    public IndexAdminController(IndexRegistry registry) {
        this.registry = registry;
    }

    /**
     * Drops the index manifest and alias tables so they are recreated at the current format
     * version. Index artifact files in object storage are left in place; they become unreferenced
     * and can be cleaned up separately.
     */
    @PostMapping("/index-tables/rebuild")
    public ResponseEntity<Map<String, Object>> rebuildIndexTables() {
        log.warn("Operator requested index metadata rebuild; dropping manifest and alias tables");

        boolean manifestDropped = registry.manifest().drop();
        boolean aliasDropped = registry.aliases().drop();

        return ResponseEntity.ok(Map.of(
                "manifestDropped", manifestDropped,
                "aliasDropped", aliasDropped,
                "formatVersion", Constants.VECTOR_FORMAT_VERSION,
                "message", "Index metadata dropped. Rebuild indexes and promote again; "
                        + "promotion history is not recoverable."));
    }
}
