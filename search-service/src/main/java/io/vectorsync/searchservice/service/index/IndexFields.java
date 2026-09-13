package io.vectorsync.searchservice.service.index;

/**
 * Lucene document field names for an index artifact.
 *
 * <p>A shared write/read contract, like the Iceberg table schema: the builder writes these names
 * and the searcher reads them, so they live in one place rather than being duplicated or exposed
 * from the builder.
 */
public final class IndexFields {

    public static final String VECTOR = "vector";
    public static final String VECTOR_ID = "vectorId";
    public static final String SOURCE_TABLE = "sourceTable";
    public static final String SOURCE_ROW_ID = "sourceRowId";
    public static final String TEXT = "text";
    public static final String MODEL_VERSION = "modelVersion";
    public static final String SOURCE_SNAPSHOT_ID = "sourceSnapshotId";

    private IndexFields() {
    }
}
