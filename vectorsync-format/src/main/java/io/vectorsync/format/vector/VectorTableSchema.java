package io.vectorsync.format.vector;

import io.vectorsync.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

/**
 * Canonical schema, partitioning, and lifecycle for the vector table.
 *
 * <p>Field IDs are part of the on-disk contract and must never be renumbered: Iceberg resolves
 * columns by ID, not by name, so changing one silently re-points existing data files.
 */
@Slf4j
public final class VectorTableSchema {

    private VectorTableSchema() {
    }

    public static Schema schema() {
        return new Schema(
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
    }

    public static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .identity(Constants.SOURCE_TABLE_COLUMN)
                .build();
    }

    public static TableIdentifier identifier(String namespace) {
        return TableIdentifier.of(Namespace.of(namespace), Constants.VECTOR_TABLE_NAME);
    }

    /**
     * Loads the vector table, creating it when absent.
     *
     * <p>TODO(format-v2): the legacy-partition-spec branch below drops and recreates the table,
     * destroying every stored vector. Retained here only to preserve existing behaviour during the
     * format extraction; it must be replaced with a real migration before the vector table can be
     * called a system of record.
     */
    public static Table loadOrCreate(Catalog catalog, String namespace) {
        TableIdentifier identifier = identifier(namespace);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Vector table existence check failed: {}", e.getMessage());
            tableExists = false;
        }

        if (tableExists) {
            Table existing = catalog.loadTable(identifier);

            Integer createdAtId = existing.schema().findField(Constants.CREATED_AT_COLUMN).fieldId();
            boolean hasCreatedAtPartition = existing.spec().fields().stream()
                    .anyMatch(field -> field.sourceId() == createdAtId);

            if (hasCreatedAtPartition) {
                log.warn("Vector table has old partition spec (includes created_at). Dropping and recreating with new spec.");
                log.warn("This will delete all existing vectors. For production, implement proper migration.");
                catalog.dropTable(identifier, true);
                tableExists = false;
            } else {
                log.info("Vector table exists with correct partition spec");
                return existing;
            }
        }

        if (!tableExists) {
            log.info("Creating vector table {}.{}", namespace, Constants.VECTOR_TABLE_NAME);
            Schema schema = schema();
            catalog.createTable(identifier, schema, partitionSpec(schema));
        }

        return catalog.loadTable(identifier);
    }

    /** Returns the vector table, or {@code null} when it does not exist yet. */
    public static Table loadIfExists(Catalog catalog, String namespace) {
        TableIdentifier identifier = identifier(namespace);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Vector table existence check failed: {}", e.getMessage());
            return null;
        }

        if (!tableExists) {
            log.warn("Vector table {}.{} does not exist yet", namespace, Constants.VECTOR_TABLE_NAME);
            return null;
        }

        return catalog.loadTable(identifier);
    }

    /** Drops the vector table if present. Returns true when a table was actually dropped. */
    public static boolean drop(Catalog catalog, String namespace) {
        TableIdentifier identifier = identifier(namespace);

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
}
