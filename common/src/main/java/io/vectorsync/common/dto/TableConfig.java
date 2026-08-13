package io.vectorsync.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableConfig {
    private String tableId;
    private String catalog;
    private String tableName;
    private List<String> embeddingColumns;
    private String modelName;
    private boolean enabled;
    private Instant createdAt;
}
