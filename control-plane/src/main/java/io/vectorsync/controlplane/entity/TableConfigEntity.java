package io.vectorsync.controlplane.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Entity
@Table(name = "table_configs")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableConfigEntity {
    @Id
    @Column(name = "table_id")
    private String tableId;

    @Column(name = "catalog", nullable = false)
    private String catalog;

    @Column(name = "table_name", nullable = false)
    private String tableName;

    @Column(name = "embedding_columns", columnDefinition = "TEXT")
    private String embeddingColumns;

    @Column(name = "model_name", nullable = false)
    private String modelName;

    @Column(name = "enabled", nullable = false)
    private boolean enabled;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;
}
