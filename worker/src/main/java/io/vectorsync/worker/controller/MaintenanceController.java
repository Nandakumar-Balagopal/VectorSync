package io.vectorsync.worker.controller;

import io.vectorsync.worker.service.maintenance.MaintenanceService;
import io.vectorsync.worker.service.maintenance.MaintenanceService.MaintenanceReport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Compaction and snapshot expiry, on demand.
 *
 * <p>Exists alongside the scheduler because the two answer different questions. The scheduler keeps
 * tables from degrading unattended; this endpoint lets an operator see the degradation
 * ({@code GET /status}) and act on it at a chosen moment -- before a benchmark, or after a bulk load
 * that produced thousands of small files and should not wait for the next hourly tick.
 */
@RestController
@RequestMapping("/api/maintenance")
@Slf4j
public class MaintenanceController {

    private final MaintenanceService maintenance;

    public MaintenanceController(MaintenanceService maintenance) {
        this.maintenance = maintenance;
    }

    /**
     * Fragmentation per derived table, changing nothing.
     *
     * <p>{@code avgRecordsPerFile} is the number to read: it falls as a table is derived
     * incrementally, and it falls long before query latency makes the problem visible.
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(maintenance.status());
    }

    /**
     * @param compact whether to rewrite data files. Defaults to true, unlike the scheduler's
     *                every-nth-tick behaviour: someone calling this by hand is asking for the
     *                expensive operation, since the cheap one happens on a timer anyway.
     */
    @PostMapping("/run")
    public ResponseEntity<MaintenanceReport> run(
            @RequestParam(name = "compact", defaultValue = "true") boolean compact) {
        MaintenanceReport report = maintenance.run(compact);
        // 409, not 500: another pass holding the lock is an expected outcome of asking twice, and a
        // caller should be able to tell it apart from a failure without parsing the note.
        return report.ran()
                ? ResponseEntity.ok(report)
                : ResponseEntity.status(409).body(report);
    }
}
