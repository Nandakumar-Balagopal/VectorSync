package io.vectorsync.format.derive;

import io.vectorsync.format.catalog.Namespaces;
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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tier-1 table of vectors keyed by content: {@code (content_hash, model_version, config_id)}.
 *
 * <p>This table is the reason a model migration is affordable. The old design keyed a vector by the
 * row it arrived on, so re-embedding meant one inference call per row and an edit to any column --
 * even a column that was never embedded -- paid for inference again. Keyed by content, a batch first
 * asks {@link #findExistingHashes} what it already has and embeds only the remainder, so a backfill
 * costs novel content and a duplicated passage across a million rows costs one vector.
 *
 * <p>Immutable and deduplicated. Nothing in here is ever superseded: an entry is a fact about
 * content under a model, and a different model or configuration is a different key rather than a
 * replacement. That is what lets two embedding versions serve side by side and a rollback be a read
 * instead of a rebuild.
 *
 * <p>Field IDs are part of the on-disk contract and must never be renumbered -- Iceberg resolves
 * columns by ID rather than name, so changing one silently re-points existing data files.
 */
@Slf4j
public final class EmbeddingStore {

    /**
     * Columns sufficient to answer "do I already have this?". The embedding is deliberately absent:
     * the dedup probe runs on every batch, and decoding a 384-float list per candidate to then throw
     * it away is the single most expensive way to answer a boolean.
     *
     * <p>{@code config_id} is present only because it is a filter column and <em>not</em> a
     * partition column. Iceberg builds the residual-filter evaluator against the projected schema,
     * so projecting a filter column away makes the scan fail with "Cannot find field" at read time.
     * Partition columns ({@code model_version}, {@code hash_prefix}) are safe to omit because
     * partition pruning removes them from the residual entirely.
     */
    private static final List<String> PROBE_COLUMNS =
            List.of(Constants.CONTENT_HASH_COLUMN, Constants.CONFIG_ID_COLUMN);

    /** Everything needed to rebuild an {@link EmbeddingEntry}. Same residual rule as above. */
    private static final List<String> LOAD_COLUMNS = List.of(
            Constants.CONTENT_HASH_COLUMN,
            Constants.CONFIG_ID_COLUMN,
            Constants.EMBEDDING_DIM_COLUMN,
            Constants.EMBEDDING_COLUMN,
            Constants.TEXT_COLUMN,
            Constants.CREATED_AT_COLUMN);

    private EmbeddingStore() {
    }

    public static Schema schema() {
        return new Schema(
                Types.NestedField.required(1, Constants.CONTENT_HASH_COLUMN, Types.StringType.get()),
                Types.NestedField.required(2, Constants.MODEL_VERSION_COLUMN, Types.StringType.get()),
                Types.NestedField.required(3, Constants.CONFIG_ID_COLUMN, Types.StringType.get()),
                // Stored, not computed at read time: an identity partition on a materialized column
                // prunes on a plain equality predicate, which every engine pushes down. A bucket
                // transform would prune too but ties the layout to Iceberg's hash function.
                Types.NestedField.required(4, Constants.HASH_PREFIX_COLUMN, Types.StringType.get()),
                Types.NestedField.required(5, Constants.EMBEDDING_DIM_COLUMN, Types.IntegerType.get()),
                // float32, not float64: every embedding model emits float32, so doubles doubled
                // storage for zero precision.
                Types.NestedField.required(6, Constants.EMBEDDING_COLUMN,
                        Types.ListType.ofRequired(7, Types.FloatType.get())),
                // Optional. The vector is the product; the text is kept for debugging and reranking
                // and a caller may legitimately choose not to persist source text here.
                Types.NestedField.optional(8, Constants.TEXT_COLUMN, Types.StringType.get()),
                Types.NestedField.required(9, Constants.CREATED_AT_COLUMN, Types.TimestampType.withZone())
        );
    }

    /**
     * Partitioned by model version and then by hash prefix.
     *
     * <p>Model version first because reading or migrating one embedding version must not scan every
     * version ever materialized. Hash prefix second because content hashes are uniformly
     * distributed, so a fixed-width prefix yields 256 evenly sized buckets without a global sort,
     * and a point lookup on a known hash touches one of them instead of the whole store.
     */
    public static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .identity(Constants.MODEL_VERSION_COLUMN)
                .identity(Constants.HASH_PREFIX_COLUMN)
                .build();
    }

    public static TableIdentifier identifier(String namespace) {
        return TableIdentifier.of(Namespace.of(namespace), Constants.EMBEDDING_STORE_TABLE_NAME);
    }

    /**
     * Loads the embedding store, creating it when absent.
     *
     * <p>Refuses to touch a table written in another format rather than dropping it. Embeddings are
     * derived and can be rebuilt, but they are also the expensive part of the system, so destroying
     * them must be an explicit operator decision.
     */
    public static Table loadOrCreate(Catalog catalog, String namespace) {
        TableIdentifier identifier = identifier(namespace);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Embedding store existence check failed: {}", e.getMessage());
            tableExists = false;
        }

        if (tableExists) {
            Table existing = catalog.loadTable(identifier);
            requireCurrentFormat(existing);
            return existing;
        }

        log.info("Creating embedding store {}.{} at format version {}",
                namespace, Constants.EMBEDDING_STORE_TABLE_NAME, Constants.VECTOR_FORMAT_VERSION);
        Schema schema = schema();
        try {
            // Non-Hadoop catalogs reject createTable into a namespace that does not exist.
            Namespaces.ensureExists(catalog, identifier);
            catalog.createTable(
                    identifier,
                    schema,
                    partitionSpec(schema),
                    Map.of(Constants.FORMAT_VERSION_PROPERTY,
                            String.valueOf(Constants.VECTOR_FORMAT_VERSION)));
        } catch (Exception e) {
            // Two workers can start a sync at once and race here; the loser just reloads.
            log.warn("Embedding store creation raced or failed, reloading: {}", e.getMessage());
        }

        Table created = catalog.loadTable(identifier);
        requireCurrentFormat(created);
        return created;
    }

    /** Returns the embedding store, or {@code null} when it does not exist yet. */
    public static Table loadIfExists(Catalog catalog, String namespace) {
        TableIdentifier identifier = identifier(namespace);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Embedding store existence check failed: {}", e.getMessage());
            return null;
        }

        if (!tableExists) {
            log.warn("Embedding store {}.{} does not exist yet",
                    namespace, Constants.EMBEDDING_STORE_TABLE_NAME);
            return null;
        }

        Table table = catalog.loadTable(identifier);
        requireCurrentFormat(table);
        return table;
    }

    /** Drops the embedding store if present. Returns true when a table was actually dropped. */
    public static boolean drop(Catalog catalog, String namespace) {
        TableIdentifier identifier = identifier(namespace);

        try {
            if (catalog.tableExists(identifier)) {
                catalog.dropTable(identifier, true);
                log.info("Dropped embedding store {}", identifier);
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed to drop embedding store {}: {}", identifier, e.getMessage());
        }

        return false;
    }

    /**
     * Appends new vectors in one commit, so a batch is either wholly visible or wholly absent.
     *
     * <p>Callers are expected to have filtered the batch through {@link #findExistingHashes} first.
     * Appending a hash that is already present is harmless -- the embedding is a pure function of
     * the key, so the duplicate is byte-identical -- but it wastes inference and storage.
     */
    public static void append(Table table, List<EmbeddingEntry> entries) {
        if (table == null || entries == null || entries.isEmpty()) {
            return;
        }

        Schema schema = table.schema();
        List<Record> records = new ArrayList<>(entries.size());
        for (EmbeddingEntry entry : entries) {
            records.add(toRecord(schema, entry));
        }

        IcebergAppender.append(table, records);
        log.debug("Appended {} embeddings to the store", records.size());
    }

    /**
     * The dedup probe: which of these hashes does the store already hold for this model and
     * configuration? The hot path -- called once per batch for the whole pipeline.
     *
     * <p>Every filter is pushed into the scan. {@code model_version} and {@code hash_prefix} are
     * identity partitions, so they prune files before any are opened; the prefix set is derived from
     * the requested hashes and is what keeps a probe for 500 hashes from scanning all 256 buckets.
     * The {@code IN} on {@code content_hash} then prunes further on Parquet column statistics and
     * filters the residual rows. Only the key columns are projected: deserializing a vector to
     * answer a boolean is exactly the defect this design removes.
     */
    public static Set<String> findExistingHashes(Table table,
                                                 Collection<String> contentHashes,
                                                 String modelVersion,
                                                 String configId) {
        Set<String> requested = distinctHashes(contentHashes);
        if (table == null || requested.isEmpty()) {
            return Set.of();
        }

        Set<String> found = new HashSet<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .where(Expressions.in(Constants.HASH_PREFIX_COLUMN, prefixesOf(requested)))
                .where(Expressions.in(Constants.CONTENT_HASH_COLUMN, requested))
                .select(PROBE_COLUMNS)
                .build()) {

            for (Record row : rows) {
                String hash = asString(row.getField(Constants.CONTENT_HASH_COLUMN));
                // Guard against a wider IN match than asked for: an engine is free to satisfy an
                // IN predicate approximately and leave exact matching to the residual.
                if (hash != null && requested.contains(hash)) {
                    found.add(hash);
                }
            }
        } catch (Exception e) {
            // Failing closed (reporting nothing cached) would re-embed the whole batch and silently
            // multiply cost, so this is loud rather than lenient.
            throw new IllegalStateException(String.format(
                    "Embedding store dedup probe failed for model %s config %s", modelVersion, configId), e);
        }

        log.debug("Dedup probe: {} of {} hashes already embedded for {} / {}",
                found.size(), requested.size(), modelVersion, configId);
        return found;
    }

    /**
     * Loads the vectors for a set of content hashes under one model and configuration.
     *
     * <p>Same pushdown as the probe, but the embedding is projected because the caller wants it.
     * Keyed by content hash alone: the model version and configuration are already pinned by the
     * arguments, so within one call the hash is a unique key.
     */
    public static Map<String, EmbeddingEntry> load(Table table,
                                                   Collection<String> contentHashes,
                                                   String modelVersion,
                                                   String configId) {
        Set<String> requested = distinctHashes(contentHashes);
        if (table == null || requested.isEmpty()) {
            return Map.of();
        }

        Map<String, EmbeddingEntry> byHash = new HashMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .where(Expressions.in(Constants.HASH_PREFIX_COLUMN, prefixesOf(requested)))
                .where(Expressions.in(Constants.CONTENT_HASH_COLUMN, requested))
                .select(LOAD_COLUMNS)
                .build()) {

            for (Record row : rows) {
                String hash = asString(row.getField(Constants.CONTENT_HASH_COLUMN));
                if (hash == null || !requested.contains(hash)) {
                    continue;
                }
                // Two writers racing on the same novel content both append, and the results are
                // byte-identical by construction, so the first one read wins arbitrarily and
                // correctly.
                byHash.putIfAbsent(hash, fromRecord(row, hash, modelVersion, configId));
            }
        } catch (Exception e) {
            throw new IllegalStateException(String.format(
                    "Failed to load embeddings for model %s config %s", modelVersion, configId), e);
        }

        return byHash;
    }

    /**
     * Refuses a store written at a different format version, with the remediation spelled out.
     *
     * <p>Without this the mismatch surfaces later as "Cannot set unknown field" from deep inside a
     * Parquet write, which tells an operator nothing about what to do.
     */
    public static void requireCurrentFormat(Table table) {
        int version = formatVersionOf(table);
        if (version == Constants.VECTOR_FORMAT_VERSION) {
            return;
        }

        throw new IllegalStateException(String.format(
                "Embedding store %s is at format version %d but this build requires version %d. "
                        + "Embeddings are derived data and must be rebuilt: drop the store "
                        + "explicitly (POST /api/admin/vector-table/rebuild on the worker) and "
                        + "re-run a full sync.",
                table.name(), version, Constants.VECTOR_FORMAT_VERSION));
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

    static Record toRecord(Schema schema, EmbeddingEntry entry) {
        if (entry.getContentHash() == null || entry.getContentHash().isBlank()) {
            throw new IllegalArgumentException("Embedding entry without a content hash has no identity");
        }

        float[] embedding = entry.getEmbedding();
        if (embedding == null || embedding.length == 0) {
            throw new IllegalArgumentException(
                    "Embedding entry " + entry.getContentHash() + " carries no vector");
        }
        // The declared dimension is what index builds size their buffers from, so a declared value
        // that disagrees with the array is rejected here rather than corrupting a later build.
        if (entry.getEmbeddingDim() > 0 && entry.getEmbeddingDim() != embedding.length) {
            throw new IllegalArgumentException(String.format(
                    "Embedding entry %s declares dimension %d but carries %d floats",
                    entry.getContentHash(), entry.getEmbeddingDim(), embedding.length));
        }

        Record record = GenericRecord.create(schema);
        record.setField(Constants.CONTENT_HASH_COLUMN, entry.getContentHash());
        record.setField(Constants.MODEL_VERSION_COLUMN, entry.getModelVersion());
        record.setField(Constants.CONFIG_ID_COLUMN, entry.getConfigId());
        // Derived here, never taken from the caller: a hash and a prefix that disagree would make
        // the row invisible to every partition-pruned lookup.
        record.setField(Constants.HASH_PREFIX_COLUMN, ContentHash.prefix(entry.getContentHash()));
        record.setField(Constants.EMBEDDING_DIM_COLUMN, embedding.length);
        record.setField(Constants.EMBEDDING_COLUMN, toFloatList(embedding));
        record.setField(Constants.TEXT_COLUMN, entry.getText());

        Instant createdAt = entry.getCreatedAt() == null ? Instant.now() : entry.getCreatedAt();
        record.setField(Constants.CREATED_AT_COLUMN, OffsetDateTime.ofInstant(createdAt, ZoneOffset.UTC));
        return record;
    }

    static EmbeddingEntry fromRecord(Record record, String contentHash, String modelVersion, String configId) {
        float[] embedding = toFloatArray(record.getField(Constants.EMBEDDING_COLUMN));
        return EmbeddingEntry.builder()
                .contentHash(contentHash)
                .modelVersion(modelVersion)
                .configId(configId)
                .embeddingDim(embedding.length)
                .embedding(embedding)
                .text(asString(record.getField(Constants.TEXT_COLUMN)))
                .createdAt(toInstant(record.getField(Constants.CREATED_AT_COLUMN)))
                .build();
    }

    private static Set<String> distinctHashes(Collection<String> contentHashes) {
        if (contentHashes == null || contentHashes.isEmpty()) {
            return Set.of();
        }
        Set<String> distinct = new LinkedHashSet<>(contentHashes.size());
        for (String hash : contentHashes) {
            if (hash != null && !hash.isBlank()) {
                distinct.add(hash);
            }
        }
        return distinct;
    }

    /** The partitions the requested hashes can possibly live in. At most 256 values. */
    private static Set<String> prefixesOf(Collection<String> contentHashes) {
        Set<String> prefixes = new LinkedHashSet<>();
        for (String hash : contentHashes) {
            prefixes.add(ContentHash.prefix(hash));
        }
        return prefixes;
    }

    private static List<Float> toFloatList(float[] embedding) {
        List<Float> values = new ArrayList<>(embedding.length);
        for (float value : embedding) {
            values.add(value);
        }
        return values;
    }

    private static float[] toFloatArray(Object value) {
        if (!(value instanceof List<?> list)) {
            return new float[0];
        }
        float[] embedding = new float[list.size()];
        int index = 0;
        for (Object element : list) {
            embedding[index++] = element instanceof Number number ? number.floatValue() : 0f;
        }
        return embedding;
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
}
