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

    /**
     * Pins the embedding version materialized for this table. Bumping it makes a new set of
     * embeddings coexist with the old one rather than overwriting it, which is what allows a
     * model migration to be evaluated before promotion.
     */
    private String embeddingVersion;

    private boolean enabled;
    private Instant createdAt;

    /** Separator used when concatenating embedding columns into the text for one chunk. */
    public static final String TEXT_JOIN_SEPARATOR = " | ";

    public String embeddingVersionOrDefault() {
        return embeddingVersion == null || embeddingVersion.isBlank()
                ? io.vectorsync.common.Constants.DEFAULT_EMBEDDING_VERSION
                : embeddingVersion;
    }
}
