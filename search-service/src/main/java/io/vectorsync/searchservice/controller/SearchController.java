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

    /** Serves through the promoted index, falling back to exact search when nothing is promoted. */
    @PostMapping
    public ResponseEntity<SearchResponse> search(@RequestBody SearchRequest request) {
        try {
            long startTime = System.currentTimeMillis();

            List<SearchResult> results = searchService.search(
                    request.getQuery(),
                    request.getTopK(),
                    request.getSourceTable()
            );

            SearchResponse response = SearchResponse.builder()
                    .query(request.getQuery())
                    .executionTimeMs(System.currentTimeMillis() - startTime)
                    .totalResults(results.size())
                    .results(results)
                    .build();

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error processing search request: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Queries a specific index version rather than the promoted one, which is how a candidate is
     * compared against production before promotion.
     */
    @PostMapping("/index/{indexId}")
    public ResponseEntity<?> searchIndex(@PathVariable("indexId") String indexId,
                                         @RequestBody SearchRequest request) {
        try {
            int k = request.getTopK() == null ? 10 : request.getTopK();
            List<SearchResult> results = searchService.searchIndexId(indexId, request.getQuery(), k);

            return ResponseEntity.ok(SearchResponse.builder()
                    .query(request.getQuery())
                    .totalResults(results.size())
                    .results(results)
                    .build());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            log.error("Error searching index {}: {}", indexId, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /** Exhaustive cosine scan; the ground truth index recall is measured against. */
    @PostMapping("/exact")
    public ResponseEntity<?> searchExact(@RequestBody SearchRequest request) {
        try {
            int k = request.getTopK() == null ? 10 : request.getTopK();
            List<SearchResult> results = searchService.searchExact(
                    request.getQuery(), k, request.getSourceTable());

            return ResponseEntity.ok(SearchResponse.builder()
                    .query(request.getQuery())
                    .totalResults(results.size())
                    .results(results)
                    .build());
        } catch (Exception e) {
            log.error("Error in exact search: {}", e.getMessage(), e);
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
            return ResponseEntity.ok(searchService.getIndexStats());
        } catch (Exception e) {
            log.error("Error getting index stats: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
