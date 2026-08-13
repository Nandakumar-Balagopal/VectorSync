package io.vectorsync.common;

public class Constants {
    public static final String VECTOR_TABLE_NAME = "vector_embeddings";
    public static final String VECTOR_ID_COLUMN = "vector_id";
    public static final String SOURCE_TABLE_COLUMN = "source_table";
    public static final String SOURCE_ROW_ID_COLUMN = "source_row_id";
    public static final String EMBEDDING_COLUMN = "embedding";
    public static final String TEXT_COLUMN = "text";
    public static final String METADATA_COLUMN = "metadata";
    public static final String MODEL_NAME_COLUMN = "model_name";
    public static final String CREATED_AT_COLUMN = "created_at";

    public static final String TABLE_CONFIGS_TABLE = "table_configs";
    public static final String SYNC_STATE_TABLE = "sync_state";

    public static final int DEFAULT_TOP_K = 10;
    public static final int EMBEDDING_DIMENSION = 384;  // all-MiniLM-L6-v2 dimension

    private Constants() {
    }
}
