package io.vectorsync.worker.service.iceberg;

import io.vectorsync.common.dto.TableConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class IcebergTableService {

    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public IcebergTableService(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public Table loadTable(TableConfig tableConfig) {
        Catalog catalog = catalogService.getCatalog();
        return catalog.loadTable(toTableIdentifier(tableConfig));
    }

    private TableIdentifier toTableIdentifier(TableConfig config) {
        if (config.getTableName().contains(".")) {
            return TableIdentifier.parse(config.getTableName());
        }

        String namespace = config.getCatalog() == null || config.getCatalog().isBlank()
                ? "default"
                : config.getCatalog();

        return TableIdentifier.of(Namespace.of(namespace), config.getTableName());
    }
}
