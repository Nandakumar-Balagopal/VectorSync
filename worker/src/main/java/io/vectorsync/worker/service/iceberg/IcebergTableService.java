package io.vectorsync.worker.service.iceberg;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.TableConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class IcebergTableService {

    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public IcebergTableService(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public Table loadTable(TableConfig tableConfig) {
        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = toTableIdentifier(tableConfig);
        return catalog.loadTable(identifier);
    }

    public Table loadOrCreateVectorTable() {
        Catalog catalog = catalogService.getCatalog();
        Namespace namespace = Namespace.of(vectorNamespace);
        TableIdentifier identifier = TableIdentifier.of(namespace, Constants.VECTOR_TABLE_NAME);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Vector table existence check failed: {}", e.getMessage());
            tableExists = false;
        }

        if (tableExists) {
            Table existing = catalog.loadTable(identifier);
            
            // Check if table has the correct partition spec (only source_table, no created_at)
            Integer createdAtId = existing.schema().findField(Constants.CREATED_AT_COLUMN).fieldId();
            boolean hasCreatedAtPartition = existing.spec().fields().stream()
                    .anyMatch(field -> field.sourceId() == createdAtId);
            
            if (hasCreatedAtPartition) {
                // Old partition spec detected - needs migration
                log.warn("Vector table has old partition spec (includes created_at). Dropping and recreating with new spec.");
                log.warn("This will delete all existing vectors. For production, implement proper migration.");
                catalog.dropTable(identifier, true);
                tableExists = false;
            } else {
                // Table has correct spec, use it
                log.info("Vector table exists with correct partition spec");
                return existing;
            }
        }

        if (!tableExists) {
            log.info("Creating vector table {}.{}", vectorNamespace, Constants.VECTOR_TABLE_NAME);
            Schema schema = new Schema(
                    Types.NestedField.required(1, Constants.VECTOR_ID_COLUMN, Types.StringType.get()),
                    Types.NestedField.required(2, Constants.SOURCE_TABLE_COLUMN, Types.StringType.get()),
                    Types.NestedField.required(3, Constants.SOURCE_ROW_ID_COLUMN, Types.StringType.get()),
                    Types.NestedField.required(4, Constants.EMBEDDING_COLUMN,
                            Types.ListType.ofRequired(5, Types.DoubleType.get())),
                    Types.NestedField.required(6, Constants.TEXT_COLUMN, Types.StringType.get()),
                    Types.NestedField.optional(7, Constants.METADATA_COLUMN,
                            Types.MapType.ofOptional(8, 9, Types.StringType.get(), Types.StringType.get())),
                    Types.NestedField.required(10, Constants.MODEL_NAME_COLUMN, Types.StringType.get()),
                    Types.NestedField.required(11, Constants.CREATED_AT_COLUMN, Types.TimestampType.withZone())
            );

                PartitionSpec spec = PartitionSpec.builderFor(schema)
                    .identity(Constants.SOURCE_TABLE_COLUMN)
                    .build();

            catalog.createTable(identifier, schema, spec);
        }

        return catalog.loadTable(identifier);
    }

    public boolean dropVectorTable() {
        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(vectorNamespace), Constants.VECTOR_TABLE_NAME);

        try {
            if (catalog.tableExists(identifier)) {
                catalog.dropTable(identifier, true);
                log.info("Dropped vector table {}", identifier);
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed to drop vector table {}: {}", identifier, e.getMessage());
        }

        return false;
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
