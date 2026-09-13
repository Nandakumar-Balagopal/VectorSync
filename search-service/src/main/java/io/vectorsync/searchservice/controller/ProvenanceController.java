package io.vectorsync.searchservice.controller;

import io.vectorsync.searchservice.service.ProvenanceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/provenance")
@Slf4j
public class ProvenanceController {

    private final ProvenanceService provenanceService;

    public ProvenanceController(ProvenanceService provenanceService) {
        this.provenanceService = provenanceService;
    }

    /** Why a search result exists: index, alias, model version, embedding, source snapshot, row. */
    @GetMapping("/vector/{vectorId}")
    public ResponseEntity<?> explain(@PathVariable("vectorId") String vectorId) {
        try {
            return ResponseEntity.ok(provenanceService.explain(vectorId));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(404).body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Provenance lookup failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Provenance lookup failed", "message", String.valueOf(e.getMessage())));
        }
    }

    /** Every stored embedding version of one source row, oldest snapshot first. */
    @GetMapping("/row")
    public ResponseEntity<List<Map<String, Object>>> history(
            @RequestParam("sourceTable") String sourceTable,
            @RequestParam("sourceRowId") String sourceRowId) {
        return ResponseEntity.ok(provenanceService.history(sourceTable, sourceRowId));
    }
}
