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
    private long executionTimeMs;
    private int totalResults;
    private List<SearchResult> results;
}
