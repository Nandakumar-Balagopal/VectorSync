package io.vectorsync.searchservice.service.index;

import lombok.Builder;
import lombok.Data;

/**
 * Result from HNSW index search containing vector metadata and similarity score.
 */
@Data
@Builder
public class VectorSearchResult {
    private String vectorId;
    private String sourceTable;
    private String sourceRowId;
    private String text;
    private Double similarity;
}

// Made with Bob
