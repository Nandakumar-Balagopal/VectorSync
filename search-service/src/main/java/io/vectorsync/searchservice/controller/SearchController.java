package io.vectorsync.searchservice.controller;

import io.vectorsync.common.dto.SearchResult;
import io.vectorsync.searchservice.service.SearchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/search")
@Slf4j
public class SearchController {

    private final SearchService searchService;

    public SearchController(SearchService searchService) {
        this.searchService = searchService;
    }

    @PostMapping
    public ResponseEntity<SearchResponse> search(@RequestBody SearchRequest request) {
        try {
            long startTime = System.currentTimeMillis();

            List<SearchResult> results = searchService.search(
                    request.getQuery(),
                    request.getTopK(),
                    request.getSourceTable()
            );

            long executionTime = System.currentTimeMillis() - startTime;

            SearchResponse response = SearchResponse.builder()
                    .query(request.getQuery())
                    .executionTimeMs(executionTime)
                    .totalResults(results.size())
                    .results(results)
                    .build();

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error processing search request: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Search API is healthy");
    }
    
    @GetMapping("/index/stats")
    public ResponseEntity<Map<String, Object>> getIndexStats() {
        try {
            Map<String, Object> stats = searchService.getIndexStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Error getting index stats: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
    
    @PostMapping("/index/rebuild")
    public ResponseEntity<Map<String, Object>> rebuildIndex() {
        try {
            long startTime = System.currentTimeMillis();
            searchService.rebuildIndex();
            long duration = System.currentTimeMillis() - startTime;
            
            Map<String, Object> response = Map.of(
                "status", "success",
                "message", "Index rebuilt successfully",
                "durationMs", duration,
                "stats", searchService.getIndexStats()
            );
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error rebuilding index: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }
}
