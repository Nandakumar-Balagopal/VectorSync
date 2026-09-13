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

import java.util.Map;

/**
 * Canonical schema, partitioning, and lifecycle for the vector table.
 *
 * <p>Field IDs are part of the on-disk contract and must never be renumbered: Iceberg resolves
 * columns by ID rather than name, so changing one silently re-points existing data files.
 *
 * <p>Format v2 promotes lineage out of the {@code metadata} string map into typed columns. A map
 * value cannot be partitioned, sorted, or range-pruned, and stringified longs compare as strings —
 * so lineage in a map cannot support the versioned queries this design depends on.
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
                Types.NestedField.required(4, Constants.SOURCE_SNAPSHOT_ID_COLUMN, Types.LongType.get()),
                Types.NestedField.required(5, Constants.CHUNK_ORDINAL_COLUMN, Types.IntegerType.get()),
                Types.NestedField.required(6, Constants.EMBEDDING_MODEL_COLUMN, Types.StringType.get()),
                Types.NestedField.required(7, Constants.EMBEDDING_VERSION_COLUMN, Types.StringType.get()),
                Types.NestedField.required(8, Constants.MODEL_VERSION_COLUMN, Types.StringType.get()),
                Types.NestedField.required(9, Constants.EMBEDDING_DIM_COLUMN, Types.IntegerType.get()),
                Types.NestedField.required(10, Constants.PREPROCESSING_ID_COLUMN, Types.StringType.get()),
                // float32, not float64: every embedding model emits float32, so doubles doubled
                // storage for zero precision. At 500M rows x 384 dims that is ~1.5TB saved, which
                // is what makes two model versions coexisting affordable.
                Types.NestedField.required(11, Constants.EMBEDDING_COLUMN,
                        Types.ListType.ofRequired(12, Types.FloatType.get())),
                // Optional because a tombstone carries no text.
                Types.NestedField.optional(13, Constants.TEXT_COLUMN, Types.StringType.get()),
                Types.NestedField.required(14, Constants.DELETED_COLUMN, Types.BooleanType.get()),
                Types.NestedField.optional(15, Constants.METADATA_COLUMN,
                        Types.MapType.ofOptional(16, 17, Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.required(18, Constants.CREATED_AT_COLUMN, Types.TimestampType.withZone())
        );
    }

    /**
     * Partitioned by source table and model version so that reading one embedding version prunes
     * partitions instead of scanning every version ever materialized.
     */
    public static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .identity(Constants.SOURCE_TABLE_COLUMN)
                .identity(Constants.MODEL_VERSION_COLUMN)
                .build();
    }

    public static TableIdentifier identifier(String namespace) {
        return TableIdentifier.of(Namespace.of(namespace), Constants.VECTOR_TABLE_NAME);
    }

    /**
     * Loads the vector table, creating it when absent.
     *
     * <p>Refuses to touch a table written in an older format rather than dropping it. Embeddings
     * are derived data and can be rebuilt, but destroying them must be an explicit operator
     * decision — silently dropping the table on a layout mismatch is incompatible with calling it
     * a system of record.
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
            requireCurrentFormat(existing, identifier);
            return existing;
        }

        log.info("Creating vector table {}.{} at format version {}",
                namespace, Constants.VECTOR_TABLE_NAME, Constants.VECTOR_FORMAT_VERSION);
        Schema schema = schema();
        catalog.createTable(
                identifier,
                schema,
                partitionSpec(schema),
                Map.of(Constants.FORMAT_VERSION_PROPERTY, String.valueOf(Constants.VECTOR_FORMAT_VERSION)));

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

        Table table = catalog.loadTable(identifier);
        requireCurrentFormat(table, identifier);
        return table;
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

    static int formatVersionOf(Table table) {
        String raw = table.properties().get(Constants.FORMAT_VERSION_PROPERTY);
        if (raw == null) {
            return 1;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    private static void requireCurrentFormat(Table table, TableIdentifier identifier) {
        int version = formatVersionOf(table);
        if (version == Constants.VECTOR_FORMAT_VERSION) {
            return;
        }

        throw new IllegalStateException(String.format(
                "Vector table %s is at format version %d but this build requires version %d. "
                        + "Embeddings are derived data and must be rebuilt: drop the table "
                        + "explicitly (POST /api/admin/vector-table/rebuild on the worker) and "
                        + "re-run a full sync.",
                identifier, version, Constants.VECTOR_FORMAT_VERSION));
    }
}
