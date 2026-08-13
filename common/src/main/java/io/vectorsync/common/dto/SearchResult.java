package io.vectorsync.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SearchResult {
    private String vectorId;
    private String sourceTable;
    private String sourceRowId;
    private String text;
    private double similarity;
}
