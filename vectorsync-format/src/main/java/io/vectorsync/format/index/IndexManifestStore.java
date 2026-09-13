package io.vectorsync.format.index;

import io.vectorsync.common.Constants;
import io.vectorsync.format.io.IcebergAppender;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The index manifest: an Iceberg table of index artifacts and their lineage.
 *
 * <p>Deliberately an ordinary Iceberg table rather than Puffin blobs. Puffin has no standardized
 * ANN blob type and no engine reads one, so it would buy zero interoperability today while
 * coupling index lifecycle to table-metadata commits. As a plain table, any Iceberg engine can
 * query the manifest with SQL right now, and this stays the migration path if the Iceberg spec
 * later standardizes a vector index blob.
 *
 * <p>Append-only, like the vector table: a new build appends an entry rather than mutating one,
 * so the full build history is auditable.
 */
@Slf4j
public final class IndexManifestStore {

    private static final String INDEX_ID = "index_id";
    private static final String SOURCE_TABLE = "source_table";
    private static final String SOURCE_SNAPSHOT_ID = "source_snapshot_id";
    private static final String EMBEDDING_MODEL = "embedding_model";
    private static final String EMBEDDING_VERSION = "embedding_version";
    private static final String MODEL_VERSION = "model_version";
    private static final String PARTITION_VALUE = "partition_value";
    private static final String INDEX_ALGORITHM = "index_algorithm";
    private static final String INDEX_PARAMS = "index_params";
    private static final String SIMILARITY_METRIC = "similarity_metric";
    private static final String DIMENSION = "dimension";
    private static final String INDEX_URI = "index_uri";
    private static final String INDEX_FILES = "index_files";
    private static final String VECTOR_COUNT = "vector_count";
    private static final String STATUS = "status";
    private static final String EVAL_METRICS = "eval_metrics";
    private static final String BUILT_AT = "built_at";
    private static final String ERROR_MESSAGE = "error_message";
    private static final String UPDATED_AT = "updated_at";

    /**
     * Oldest-first, so a later row supersedes an earlier one for the same index id. Ordered by
     * row write time rather than build time, because a status change or an evaluation update
     * writes a new row for an already-built index. Status progression is a defensive tiebreak in
     * case two rows land within the clock's resolution.
     */
    private static final Comparator<IndexManifestEntry> OLDEST_FIRST =
            Comparator.comparing(IndexManifestEntry::getUpdatedAt,
                            Comparator.nullsFirst(Comparator.<Instant>naturalOrder()))
                    .thenComparingInt(entry -> entry.getStatus() == null
                            ? 0
                            : entry.getStatus().progressionRank());

    private final Catalog catalog;
    private final String namespace;

    public IndexManifestStore(Catalog catalog, String namespace) {
        this.catalog = catalog;
        this.namespace = namespace;
    }

    public static Schema schema() {
        return new Schema(
                Types.NestedField.required(1, INDEX_ID, Types.StringType.get()),
                Types.NestedField.required(2, SOURCE_TABLE, Types.StringType.get()),
                Types.NestedField.required(3, SOURCE_SNAPSHOT_ID, Types.LongType.get()),
                Types.NestedField.required(4, EMBEDDING_MODEL, Types.StringType.get()),
                Types.NestedField.required(5, EMBEDDING_VERSION, Types.StringType.get()),
                Types.NestedField.required(6, MODEL_VERSION, Types.StringType.get()),
                Types.NestedField.optional(7, PARTITION_VALUE, Types.StringType.get()),
                Types.NestedField.required(8, INDEX_ALGORITHM, Types.StringType.get()),
                Types.NestedField.optional(9, INDEX_PARAMS,
                        Types.MapType.ofOptional(10, 11, Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.required(12, SIMILARITY_METRIC, Types.StringType.get()),
                Types.NestedField.required(13, DIMENSION, Types.IntegerType.get()),
                Types.NestedField.required(14, INDEX_URI, Types.StringType.get()),
                Types.NestedField.required(15, INDEX_FILES,
                        Types.ListType.ofRequired(16, Types.StringType.get())),
                Types.NestedField.required(17, VECTOR_COUNT, Types.LongType.get()),
                Types.NestedField.required(18, STATUS, Types.StringType.get()),
                Types.NestedField.optional(19, EVAL_METRICS,
                        Types.MapType.ofOptional(20, 21, Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.required(22, BUILT_AT, Types.TimestampType.withZone()),
                Types.NestedField.optional(23, ERROR_MESSAGE, Types.StringType.get()),
                Types.NestedField.required(24, UPDATED_AT, Types.TimestampType.withZone())
        );
    }

    public static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .identity(SOURCE_TABLE)
                .identity(MODEL_VERSION)
                .build();
    }

    public TableIdentifier identifier() {
        return TableIdentifier.of(Namespace.of(namespace), Constants.INDEX_MANIFEST_TABLE_NAME);
    }

    public Table loadOrCreate() {
        TableIdentifier identifier = identifier();
        try {
            if (catalog.tableExists(identifier)) {
                return catalog.loadTable(identifier);
            }
        } catch (Exception e) {
            log.warn("Index manifest existence check failed: {}", e.getMessage());
        }

        log.info("Creating index manifest table {}", identifier);
        Schema schema = schema();
        try {
            catalog.createTable(identifier, schema, partitionSpec(schema));
        } catch (Exception e) {
            // Another writer may have created it between the check and the create.
            log.warn("Index manifest creation raced or failed, reloading: {}", e.getMessage());
        }
        return catalog.loadTable(identifier);
    }

    /** Records a new index artifact. Appends; never mutates an existing entry. */
    public void put(IndexManifestEntry entry) {
        // Stamped here rather than by callers, so no caller can accidentally write a row that
        // ties with the one it means to supersede.
        entry.setUpdatedAt(Instant.now());

        Table table = loadOrCreate();
        IcebergAppender.append(table, List.of(toRecord(table.schema(), entry)));
        log.info("Recorded index {} for {} {} status={}",
                entry.getIndexId(), entry.getSourceTable(), entry.modelVersion(), entry.getStatus());
    }

    /**
     * Returns the newest entry per index id, so a later status or evaluation update supersedes the
     * original build record.
     */
    public List<IndexManifestEntry> list() {
        Table table;
        try {
            table = loadOrCreate();
        } catch (Exception e) {
            log.warn("Index manifest unavailable: {}", e.getMessage());
            return List.of();
        }

        List<IndexManifestEntry> entries = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                try {
                    entries.add(fromRecord(row));
                } catch (Exception e) {
                    log.warn("Skipping unreadable manifest row: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read index manifest: {}", e.getMessage());
            return List.of();
        }

        Map<String, IndexManifestEntry> newestById = new LinkedHashMap<>();
        entries.stream()
                .sorted(OLDEST_FIRST)
                .forEach(entry -> newestById.put(entry.getIndexId(), entry));

        return List.copyOf(newestById.values());
    }

    public Optional<IndexManifestEntry> findById(String indexId) {
        return list().stream()
                .filter(entry -> entry.getIndexId().equals(indexId))
                .findFirst();
    }

    public List<IndexManifestEntry> findForTable(String sourceTable) {
        return list().stream()
                .filter(entry -> entry.getSourceTable().equals(sourceTable))
                .toList();
    }

    /** The newest READY index for a table and model version, if any. */
    public Optional<IndexManifestEntry> findLatestReady(String sourceTable, String modelVersion) {
        return list().stream()
                .filter(entry -> entry.getSourceTable().equals(sourceTable))
                .filter(entry -> entry.modelVersion().equals(modelVersion))
                .filter(entry -> entry.getStatus() == IndexStatus.READY)
                .max(Comparator.comparingLong(IndexManifestEntry::getSourceSnapshotId)
                        .thenComparing(IndexManifestEntry::getBuiltAt,
                                Comparator.nullsFirst(Comparator.naturalOrder())));
    }

    static Record toRecord(Schema schema, IndexManifestEntry entry) {
        Record record = GenericRecord.create(schema);
        record.setField(INDEX_ID, entry.getIndexId());
        record.setField(SOURCE_TABLE, entry.getSourceTable());
        record.setField(SOURCE_SNAPSHOT_ID, entry.getSourceSnapshotId());
        record.setField(EMBEDDING_MODEL, entry.getEmbeddingModel());
        record.setField(EMBEDDING_VERSION, entry.getEmbeddingVersion());
        record.setField(MODEL_VERSION, entry.modelVersion());
        record.setField(PARTITION_VALUE, entry.getPartitionValue());
        record.setField(INDEX_ALGORITHM, entry.getIndexAlgorithm());
        record.setField(INDEX_PARAMS, entry.getIndexParams() == null ? Map.of() : entry.getIndexParams());
        record.setField(SIMILARITY_METRIC, entry.getSimilarityMetric());
        record.setField(DIMENSION, entry.getDimension());
        record.setField(INDEX_URI, entry.getIndexUri());
        record.setField(INDEX_FILES, entry.getIndexFiles() == null ? List.of() : entry.getIndexFiles());
        record.setField(VECTOR_COUNT, entry.getVectorCount());
        record.setField(STATUS, entry.getStatus() == null ? IndexStatus.FAILED.name() : entry.getStatus().name());
        record.setField(EVAL_METRICS, entry.getEvalMetrics() == null ? Map.of() : entry.getEvalMetrics());
        Instant builtAt = entry.getBuiltAt() == null ? Instant.now() : entry.getBuiltAt();
        record.setField(BUILT_AT, OffsetDateTime.ofInstant(builtAt, ZoneOffset.UTC));
        record.setField(ERROR_MESSAGE, entry.getErrorMessage());
        Instant updatedAt = entry.getUpdatedAt() == null ? Instant.now() : entry.getUpdatedAt();
        record.setField(UPDATED_AT, OffsetDateTime.ofInstant(updatedAt, ZoneOffset.UTC));
        return record;
    }

    @SuppressWarnings("unchecked")
    static IndexManifestEntry fromRecord(Record record) {
        return IndexManifestEntry.builder()
                .indexId(asString(record.getField(INDEX_ID)))
                .sourceTable(asString(record.getField(SOURCE_TABLE)))
                .sourceSnapshotId(asLong(record.getField(SOURCE_SNAPSHOT_ID)))
                .embeddingModel(asString(record.getField(EMBEDDING_MODEL)))
                .embeddingVersion(asString(record.getField(EMBEDDING_VERSION)))
                .partitionValue(asString(record.getField(PARTITION_VALUE)))
                .indexAlgorithm(asString(record.getField(INDEX_ALGORITHM)))
                .indexParams(toStringMap(record.getField(INDEX_PARAMS)))
                .similarityMetric(asString(record.getField(SIMILARITY_METRIC)))
                .dimension(asInt(record.getField(DIMENSION)))
                .indexUri(asString(record.getField(INDEX_URI)))
                .indexFiles(record.getField(INDEX_FILES) instanceof List<?> list
                        ? list.stream().map(String::valueOf).toList()
                        : List.of())
                .vectorCount(asLong(record.getField(VECTOR_COUNT)))
                .status(IndexStatus.parse(asString(record.getField(STATUS))))
                .evalMetrics(toStringMap(record.getField(EVAL_METRICS)))
                .builtAt(toInstant(record.getField(BUILT_AT)))
                .updatedAt(toInstant(record.getField(UPDATED_AT)))
                .errorMessage(asString(record.getField(ERROR_MESSAGE)))
                .build();
    }

    private static Instant toInstant(Object value) {
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (value instanceof Long micros) {
            return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L),
                    Math.floorMod(micros, 1_000_000L) * 1_000L);
        }
        return null;
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static long asLong(Object value) {
        return value instanceof Number number ? number.longValue() : 0L;
    }

    private static int asInt(Object value) {
        return value instanceof Number number ? number.intValue() : 0;
    }

    private static Map<String, String> toStringMap(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return new HashMap<>();
        }
        Map<String, String> result = new HashMap<>();
        map.forEach((key, mapValue) -> {
            if (key != null && mapValue != null) {
                result.put(key.toString(), mapValue.toString());
            }
        });
        return result;
    }
}
