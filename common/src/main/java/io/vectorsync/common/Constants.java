package io.vectorsync.common;

public class Constants {

    /** Bumped whenever the vector table's physical layout changes incompatibly. */
    public static final int VECTOR_FORMAT_VERSION = 3;

    /** Table property recording the format version a vector table was created with. */
    public static final String FORMAT_VERSION_PROPERTY = "vectorsync.format-version";

    public static final String VECTOR_TABLE_NAME = "vector_embeddings";
    public static final String INDEX_MANIFEST_TABLE_NAME = "vector_index_manifest";
    public static final String INDEX_ALIAS_TABLE_NAME = "vector_index_alias";

    // vector_embeddings columns
    public static final String VECTOR_ID_COLUMN = "vector_id";
    public static final String SOURCE_TABLE_COLUMN = "source_table";
    public static final String SOURCE_ROW_ID_COLUMN = "source_row_id";
    public static final String SOURCE_SNAPSHOT_ID_COLUMN = "source_snapshot_id";
    public static final String SOURCE_SEQUENCE_NUMBER_COLUMN = "source_sequence_number";
    public static final String SOURCE_COMMITTED_AT_COLUMN = "source_committed_at";
    public static final String CHUNK_ORDINAL_COLUMN = "chunk_ordinal";
    public static final String EMBEDDING_MODEL_COLUMN = "embedding_model";
    public static final String EMBEDDING_VERSION_COLUMN = "embedding_version";
    public static final String EMBEDDING_DIM_COLUMN = "embedding_dim";
    public static final String PREPROCESSING_ID_COLUMN = "preprocessing_id";
    public static final String EMBEDDING_COLUMN = "embedding";
    public static final String TEXT_COLUMN = "text";
    public static final String DELETED_COLUMN = "deleted";
    public static final String METADATA_COLUMN = "metadata";
    public static final String CREATED_AT_COLUMN = "created_at";

    /**
     * Synthetic partition column holding "{embedding_model}:{embedding_version}", so that reading
     * a single embedding version is partition pruning rather than a full scan.
     */
    public static final String MODEL_VERSION_COLUMN = "model_version";

    public static final String TABLE_CONFIGS_TABLE = "table_configs";
    public static final String SYNC_STATE_TABLE = "sync_state";

    public static final int DEFAULT_TOP_K = 10;
    public static final int EMBEDDING_DIMENSION = 384;  // all-MiniLM-L6-v2 dimension

    /** Default embedding version applied when a table config does not pin one. */
    public static final String DEFAULT_EMBEDDING_VERSION = "v1";

    private Constants() {
    }
}
