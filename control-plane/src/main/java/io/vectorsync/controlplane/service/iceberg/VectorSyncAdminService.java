package io.vectorsync.controlplane.service.iceberg;

import io.vectorsync.common.Constants;
import io.vectorsync.format.vector.VectorTableSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class VectorSyncAdminService {

    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public VectorSyncAdminService(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public void deleteEmbeddingsForTable(String sourceTable) {
        if (sourceTable == null || sourceTable.isBlank()) {
            throw new IllegalArgumentException("Source table name is required to delete embeddings");
        }

        Table table = VectorTableSchema.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (table == null) {
            return;
        }

        log.info("Deleting embeddings for source table {}", sourceTable);
        try {
            table.newDelete()
                    .deleteFromRowFilter(Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable))
                    .commit();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to delete embeddings for table " + sourceTable, e);
        }
    }
}
