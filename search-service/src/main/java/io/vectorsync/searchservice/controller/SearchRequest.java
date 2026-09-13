package io.vectorsync.searchservice.controller;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SearchRequest {
    private String query;
    private Integer topK;
    private String sourceTable;

    /**
     * Optional {@code model:version} scope for exact search. Without it a scan spans every
     * materialized version, which mixes embedding spaces and returns each row once per version.
     */
    private String modelVersion;
}
