package io.vectorsync.searchservice.controller;

import io.vectorsync.common.dto.SearchResult;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SearchResponse {
    private String query;
    /**
     * The embedding space that actually answered, as "model:version". Echoed because a caller who
     * omits the version gets one resolved for them, and a similarity score means nothing without
     * knowing which model produced it.
     */
    private String modelVersion;
    private long executionTimeMs;
    private int totalResults;
    private List<SearchResult> results;
}
