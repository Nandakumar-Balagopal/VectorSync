package io.vectorsync.controlplane.service.iceberg;

import io.vectorsync.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
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

        Table table = loadVectorTable();
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

    private Table loadVectorTable() {
        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(vectorNamespace), Constants.VECTOR_TABLE_NAME);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Vector table existence check failed: {}", e.getMessage());
            return null;
        }

        if (!tableExists) {
            log.warn("Vector table {}.{} does not exist yet", vectorNamespace, Constants.VECTOR_TABLE_NAME);
            return null;
        }

        return catalog.loadTable(identifier);
    }
}
