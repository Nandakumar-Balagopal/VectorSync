package io.vectorsync.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VectorRecord {
    private String vectorId;
    private String sourceTable;
    private String sourceRowId;
    private List<Double> embedding;
    private String text;
    private Map<String, String> metadata;
    private String modelName;
    private Instant createdAt;
    private boolean deleted;
}
