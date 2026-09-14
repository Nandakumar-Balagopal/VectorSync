package io.vectorsync.format.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.vector.VectorIds;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Tier 2: the flat serving projection, and the builder that materializes it from Tier 1.
 *
 * <p>Tier 1 is normalized because dedup and lineage demand it -- {@link ContentMap} points at
 * content and {@link EmbeddingStore} holds the vector for that content exactly once. That shape is
 * right for storage and wrong for serving: a brute-force similarity scan would have to join two
 * tables on {@code content_hash} for every candidate row, and a join is precisely what a scan of a
 * few million rows cannot afford to pay per query. This table is that join, already done: one row
 * per live row/chunk with its vector inlined, partitioned so a query touches one model version.
 *
 * <p>It is derived and disposable. Every column here is reproducible from Tier 1, which is what
 * makes {@link #drop} safe and why a rebuild is an ordinary operation rather than an incident. The
 * duplication is deliberate: a popular passage shared by a thousand rows is stored once in the
 * embedding store and a thousand times here, because serving reads want locality, not dedup.
 *
 * <p>Field IDs are part of the on-disk contract and must never be renumbered -- Iceberg resolves
 * columns by ID rather than name, so changing one silently re-points existing data files.
 */
@Slf4j
public final class ProjectionBuilder {

    /** Every serving projection is named {@code vectors_<source>_<configId>}. */
    public static final String TABLE_NAME_PREFIX = "vectors_";

    /**
     * How much of the source table name survives in the projection's name. Catalogs impose
     * identifier limits (Hive 128 characters, Glue 255), and a fully qualified source name can be
     * long. Truncating cannot create a collision: {@code config_id} hashes the source table, so two
     * different source tables never share a suffix.
     */
    private static final int MAX_SOURCE_NAME_CHARS = 80;

    /**
     * Rows per output data file, and therefore per embedding-load block.
     *
     * <p>Sized for the builder's heap rather than for an ideal file size. The vectors for a block
     * are held in memory while its rows are written, so this bounds that working set to roughly
     * {@code BLOCK_ROWS x dimension x 4} bytes of float arrays.
     */
    private static final int BLOCK_ROWS = 50_000;

    /**
     * Content hashes per embedding-store lookup. The whole point of Tier 1 is that a probe is a
     * partition-pruned batch read, so vectors are fetched a thousand at a time; a lookup per
     * projected row would turn one rebuild into millions of scan plans.
     */
    private static final int EMBEDDING_LOAD_BATCH = 1_000;

    /**
     * Snapshot summary keys. Written in the same commit as the data, so a reader can tell how
     * current a projection snapshot is without a second metadata read -- table properties would
     * need their own commit and could therefore disagree with the data they describe.
     */
    private static final String SUMMARY_ROWS = "vectorsync.projection.rows";
    private static final String SUMMARY_VECTORS = "vectorsync.projection.distinct-vectors";
    private static final String SUMMARY_DIM = "vectorsync.projection.embedding-dim";
    private static final String SUMMARY_CONFIG_ID = "vectorsync.projection.config-id";
    private static final String SUMMARY_SEQUENCE = "vectorsync.projection.source-sequence-number";
    private static final String SUMMARY_UNRESOLVED = "vectorsync.projection.unresolved-rows";

    private ProjectionBuilder() {
    }

    /**
     * What one {@link #build} produced.
     *
     * @param tableIdentifier the projection that was replaced
     * @param rowsWritten     live row/chunks in the projection after the commit
     * @param distinctVectors distinct content hashes behind those rows; the ratio to
     *                        {@code rowsWritten} is the dedup factor this architecture buys
     * @param embeddingDim    vector width, 0 when the projection is empty
     * @param unresolvedRows  live mappings whose content had no vector under this model and
     *                        configuration, and which are therefore absent from serving. Nonzero
     *                        means the embedding pass has not caught up with the content map.
     */
    public record Projection(TableIdentifier tableIdentifier,
                             long rowsWritten,
                             long distinctVectors,
                             int embeddingDim,
                             long unresolvedRows) {
    }

    public static Schema schema() {
        return new Schema(
                // Same identity the old row-keyed vector table used, so provenance records and
                // evaluation runs that reference a vector id keep resolving after the split.
                Types.NestedField.required(1, Constants.VECTOR_ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(2, Constants.SOURCE_TABLE_COLUMN, Types.StringType.get()),
                Types.NestedField.required(3, Constants.SOURCE_ROW_ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(4, Constants.CHUNK_ORDINAL_COLUMN, Types.IntegerType.get()),
                // Carried, not joined away: it is how a serving hit is traced back to the exact
                // Tier-1 vector, and how a cache can recognize two rows as the same content.
                Types.NestedField.required(5, Constants.CONTENT_HASH_COLUMN, Types.StringType.get()),
                Types.NestedField.required(6, Constants.MODEL_VERSION_COLUMN, Types.StringType.get()),
                Types.NestedField.required(7, Constants.CONFIG_ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(8, Constants.EMBEDDING_DIM_COLUMN, Types.IntegerType.get()),
                // float32, not float64: every embedding model emits float32, so doubles doubled
                // storage for zero precision -- and this is the table that stores the vector once
                // per row rather than once per content.
                Types.NestedField.required(9, Constants.EMBEDDING_COLUMN,
                        Types.ListType.ofRequired(10, Types.FloatType.get())),
                // Optional: the embedding store is allowed not to retain source text.
                Types.NestedField.optional(11, Constants.TEXT_COLUMN, Types.StringType.get()),
                Types.NestedField.required(12, Constants.SOURCE_SNAPSHOT_ID_COLUMN, Types.LongType.get()),
                Types.NestedField.required(13, Constants.SOURCE_SEQUENCE_NUMBER_COLUMN, Types.LongType.get()),
                Types.NestedField.required(14, Constants.CREATED_AT_COLUMN, Types.TimestampType.withZone())
        );
    }

    /**
     * Partitioned by source table and model version, matching the vector table it serves in place
     * of. Both values are constant within one projection today; the partitioning exists so that a
     * migration can land a second model version alongside the first and a rebuild of one version
     * rewrites only its own partition.
     */
    public static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .identity(Constants.SOURCE_TABLE_COLUMN)
                .identity(Constants.MODEL_VERSION_COLUMN)
                .build();
    }

    /**
     * {@code vectors_<sanitized source table>_<config id>}.
     *
     * <p>The configuration id is in the name on purpose. Two specs over the same source table are
     * two different datasets, so they get two different tables and can be compared, promoted, or
     * abandoned independently instead of overwriting each other.
     */
    public static String tableName(MaterializationSpec spec) {
        return TABLE_NAME_PREFIX + sanitize(spec.getSourceTable()) + "_" + spec.configId();
    }

    public static TableIdentifier identifier(String namespace, MaterializationSpec spec) {
        return TableIdentifier.of(Namespace.of(namespace), tableName(spec));
    }

    /** Loads the projection for a spec, creating it empty when absent. */
    public static Table loadOrCreate(Catalog catalog, String namespace, MaterializationSpec spec) {
        TableIdentifier identifier = identifier(namespace, spec);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Serving projection existence check failed: {}", e.getMessage());
            tableExists = false;
        }

        if (tableExists) {
            Table existing = catalog.loadTable(identifier);
            requireCurrentFormat(existing);
            return existing;
        }

        log.info("Creating serving projection {} at format version {}",
                identifier, Constants.VECTOR_FORMAT_VERSION);
        Schema schema = schema();
        try {
            catalog.createTable(
                    identifier,
                    schema,
                    partitionSpec(schema),
                    Map.of(Constants.FORMAT_VERSION_PROPERTY,
                            String.valueOf(Constants.VECTOR_FORMAT_VERSION)));
        } catch (Exception e) {
            // Two workers can start a rebuild at once and race here; the loser just reloads.
            log.warn("Serving projection creation raced or failed, reloading: {}", e.getMessage());
        }

        Table created = catalog.loadTable(identifier);
        requireCurrentFormat(created);
        return created;
    }

    /** Returns the projection for a spec, or {@code null} when it has never been built. */
    public static Table loadIfExists(Catalog catalog, String namespace, MaterializationSpec spec) {
        TableIdentifier identifier = identifier(namespace, spec);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Serving projection existence check failed: {}", e.getMessage());
            return null;
        }

        if (!tableExists) {
            log.warn("Serving projection {} has not been built yet", identifier);
            return null;
        }

        Table table = catalog.loadTable(identifier);
        requireCurrentFormat(table);
        return table;
    }

    /**
     * Drops the projection if present. Returns true when a table was actually dropped.
     *
     * <p>Unlike the Tier-1 tables, this needs no ceremony: the projection holds no information that
     * does not exist in the content map and the embedding store, so dropping it costs a rebuild and
     * never costs inference.
     */
    public static boolean drop(Catalog catalog, String namespace, MaterializationSpec spec) {
        TableIdentifier identifier = identifier(namespace, spec);

        try {
            if (catalog.tableExists(identifier)) {
                catalog.dropTable(identifier, true);
                log.info("Dropped serving projection {}", identifier);
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed to drop serving projection {}: {}", identifier, e.getMessage());
        }

        return false;
    }

    /** Materializes the projection from the current state of Tier 1. */
    public static Projection build(Catalog catalog, String namespace, MaterializationSpec spec) {
        return buildAsOf(catalog, namespace, spec, Long.MAX_VALUE);
    }

    /**
     * Materializes the projection as Tier 1 stood at a source sequence number.
     *
     * <p>Rebuilding an earlier version is the reproducibility claim made concrete: the content map
     * is a history, so pinning the sequence number pins the exact set of content hashes, and the
     * embedding store is immutable, so the vectors behind them cannot have drifted either.
     *
     * @param asOfSourceSequenceNumber source version to project, {@link Long#MAX_VALUE} for current
     * @throws IllegalStateException if Tier 1 is missing, unreadable, or so far behind the content
     *                               map that committing would empty a projection that is serving
     */
    public static Projection buildAsOf(Catalog catalog,
                                       String namespace,
                                       MaterializationSpec spec,
                                       long asOfSourceSequenceNumber) {
        String configId = spec.configId();
        String modelVersion = spec.modelVersion();

        Table contentMap = ContentMap.loadIfExists(catalog, namespace);
        Table embeddingStore = EmbeddingStore.loadIfExists(catalog, namespace);
        if (contentMap == null || embeddingStore == null) {
            // Refusing beats committing an empty projection: an absent Tier 1 means the pipeline
            // has not run, not that the source table became empty.
            throw new IllegalStateException(String.format(
                    "Cannot build the serving projection for %s / %s: Tier 1 is not initialized "
                            + "(content map present: %s, embedding store present: %s). Run a sync first.",
                    spec.getSourceTable(), configId, contentMap != null, embeddingStore != null));
        }

        List<ContentMapEntry> live = new ArrayList<>(
                ContentMap.liveEntriesAsOf(contentMap, spec.getSourceTable(), configId, asOfSourceSequenceNumber));

        // Sorted by content hash, which does two things at once: identical content lands in one
        // block so its vector is fetched once, and a block's hashes cluster into few hash_prefix
        // partitions of the embedding store, so each batched load prunes hard instead of touching
        // all 256 buckets. Row id and ordinal only make the order total, and therefore the file
        // layout deterministic across rebuilds.
        live.sort(Comparator.comparing(ContentMapEntry::getContentHash)
                .thenComparing(ContentMapEntry::getSourceRowId)
                .thenComparingInt(ContentMapEntry::getChunkOrdinal));

        Table projection = loadOrCreate(catalog, namespace, spec);
        Schema schema = projection.schema();
        PartitionKey partitionKey = partitionKeyFor(schema, projection.spec(), spec);
        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(projection, 1, System.currentTimeMillis())
                        .format(FileFormat.PARQUET)
                        .build();

        Instant builtAt = Instant.now();
        List<DataFile> dataFiles = new ArrayList<>();
        long rows = 0L;
        long distinctVectors = 0L;
        long unresolvedRows = 0L;
        long sourceSequenceNumber = 0L;
        long oldestUnresolvedSequenceNumber = Long.MAX_VALUE;
        int embeddingDim = 0;
        String previousHash = null;

        for (int start = 0; start < live.size(); start += BLOCK_ROWS) {
            List<ContentMapEntry> block = live.subList(start, Math.min(live.size(), start + BLOCK_ROWS));
            Map<String, EmbeddingEntry> vectors = loadVectors(embeddingStore, block, modelVersion, configId);

            List<ContentMapEntry> resolvable = new ArrayList<>(block.size());
            for (ContentMapEntry entry : block) {
                if (vectors.containsKey(entry.getContentHash())) {
                    resolvable.add(entry);
                } else {
                    unresolvedRows++;
                    oldestUnresolvedSequenceNumber =
                            Math.min(oldestUnresolvedSequenceNumber, entry.getSourceSequenceNumber());
                }
            }
            if (resolvable.isEmpty()) {
                continue;
            }

            WrittenBlock written = writeBlock(schema, projection.spec(), outputFileFactory, partitionKey,
                    resolvable, vectors, spec, builtAt, embeddingDim, previousHash);

            dataFiles.add(written.dataFile());
            rows += written.rows();
            distinctVectors += written.distinctVectors();
            sourceSequenceNumber = Math.max(sourceSequenceNumber, written.maxSourceSequenceNumber());
            embeddingDim = written.embeddingDim();
            previousHash = written.lastContentHash();
        }

        if (rows == 0 && !live.isEmpty()) {
            // The content map says these rows exist but no vector was found for any of them.
            // Committing here would delete a working projection because the embedding pass is
            // behind or reading the wrong model version, so this fails instead.
            throw new IllegalStateException(String.format(
                    "Refusing to empty the serving projection %s: %d live mappings resolved to no "
                            + "vectors under model %s config %s. Run the embedding pass for this spec "
                            + "before rebuilding the projection.",
                    projection.name(), live.size(), modelVersion, configId));
        }
        if (unresolvedRows > 0) {
            log.warn("Serving projection {} omits {} of {} live mappings with no vector under {} / {}",
                    projection.name(), unresolvedRows, live.size(), modelVersion, configId);
        }

        // The published watermark must be the newest source version the projection covers in full,
        // not the newest row that happened to resolve. With a mapping missing at
        // sequence 50 and other rows resolved at 100, publishing 100 would tell a scheduler the
        // projection is current and the omitted row would never be picked up once its embedding
        // landed.
        long coveredSequenceNumber = oldestUnresolvedSequenceNumber == Long.MAX_VALUE
                ? sourceSequenceNumber
                : Math.max(0L, Math.min(sourceSequenceNumber, oldestUnresolvedSequenceNumber - 1));

        commitReplacement(projection, dataFiles, spec, rows, distinctVectors, embeddingDim,
                coveredSequenceNumber, unresolvedRows);

        log.info("Built serving projection {}: {} rows over {} distinct vectors of dimension {} at source sequence {}",
                projection.name(), rows, distinctVectors, embeddingDim, coveredSequenceNumber);

        return new Projection(identifier(namespace, spec), rows, distinctVectors, embeddingDim, unresolvedRows);
    }

    /**
     * Replaces this spec's slice of the projection in a single snapshot: the row filter deletes what
     * was there and the new files land in the same commit, so a reader sees the old projection or
     * the new one and never a half-built mixture of both.
     *
     * <p>The filter names only identity-partition columns, which is load-bearing rather than
     * incidental. Iceberg deletes whole files by row filter and refuses -- "Cannot delete file where
     * some, but not all, rows match the filter" -- when a filter cannot be proven to cover an entire
     * file. A predicate on any non-partition column here would fail at commit time.
     */
    private static void commitReplacement(Table projection,
                                          List<DataFile> dataFiles,
                                          MaterializationSpec spec,
                                          long rows,
                                          long distinctVectors,
                                          int embeddingDim,
                                          long sourceSequenceNumber,
                                          long unresolvedRows) {
        Expression replaced = Expressions.and(
                Expressions.equal(Constants.SOURCE_TABLE_COLUMN, spec.getSourceTable()),
                Expressions.equal(Constants.MODEL_VERSION_COLUMN, spec.modelVersion()));

        OverwriteFiles overwrite = projection.newOverwrite().overwriteByRowFilter(replaced);
        dataFiles.forEach(overwrite::addFile);
        if (!dataFiles.isEmpty()) {
            // Catches a row written under the wrong partition values before it becomes a file that
            // the next rebuild's filter cannot delete. Enabled only when files were added: the
            // validation reads the spec of the added files, so on a pure delete (a source table
            // whose every row is gone) it fails with "Cannot determine partition spec".
            overwrite.validateAddedFilesMatchOverwriteFilter();
        }

        overwrite.set(SUMMARY_ROWS, Long.toString(rows));
        overwrite.set(SUMMARY_VECTORS, Long.toString(distinctVectors));
        overwrite.set(SUMMARY_DIM, Integer.toString(embeddingDim));
        overwrite.set(SUMMARY_CONFIG_ID, spec.configId());
        overwrite.set(SUMMARY_SEQUENCE, Long.toString(sourceSequenceNumber));
        // Published so an incomplete projection is visible to whoever reads the summary: the count
        // returned to the caller is gone once the build's process ends, and rows/sequence alone
        // read as a healthy projection.
        overwrite.set(SUMMARY_UNRESOLVED, Long.toString(unresolvedRows));

        overwrite.commit();
    }

    /**
     * Writes one block as a single Parquet data file without committing it.
     *
     * <p>Deliberately not {@code IcebergAppender}: that helper commits its own append, and this
     * table needs the write and the delete of the previous contents to arrive in one snapshot. The
     * careful parts of the appender are reproduced here for that reason -- file length and metrics
     * are only valid after close, and {@code withPartition} is mandatory, because Iceberg serves
     * identity-partition columns as constants folded from the partition tuple rather than reading
     * them from the file, so omitting it makes {@code source_table} and {@code model_version} read
     * back null.
     */
    private static WrittenBlock writeBlock(Schema schema,
                                           PartitionSpec partitionSpec,
                                           OutputFileFactory outputFileFactory,
                                           PartitionKey partitionKey,
                                           List<ContentMapEntry> block,
                                           Map<String, EmbeddingEntry> vectors,
                                           MaterializationSpec spec,
                                           Instant builtAt,
                                           int expectedDim,
                                           String previousHash) {
        EncryptedOutputFile encryptedOutputFile = outputFileFactory.newOutputFile(partitionKey);
        OutputFile outputFile = encryptedOutputFile.encryptingOutputFile();

        int dimension = expectedDim;
        long distinctVectors = 0L;
        long maxSourceSequenceNumber = 0L;
        String lastHash = previousHash;
        long fileSize;
        Metrics metrics;

        try {
            FileAppender<Record> appender = Parquet.write(outputFile)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build();

            try (appender) {
                for (ContentMapEntry entry : block) {
                    EmbeddingEntry vector = vectors.get(entry.getContentHash());
                    float[] embedding = vector.getEmbedding();
                    if (embedding.length == 0) {
                        // Zero doubles as the "width not established yet" sentinel below, so an
                        // empty vector would slip past the width check and serve as a row that no
                        // similarity expression can score -- array arithmetic pads the shorter side
                        // and returns null rather than failing.
                        throw new IllegalStateException(String.format(
                                "Embedding store holds an empty vector for content %s under model %s config %s",
                                entry.getContentHash(), spec.modelVersion(), spec.configId()));
                    }

                    if (dimension == 0) {
                        dimension = embedding.length;
                    } else if (dimension != embedding.length) {
                        // A mixed-width array column cannot be scanned by any similarity
                        // expression, so this stops here rather than producing a table whose
                        // queries fail per row.
                        throw new IllegalStateException(String.format(
                                "Embedding store holds mixed dimensions for model %s config %s: "
                                        + "content %s has %d floats, expected %d",
                                spec.modelVersion(), spec.configId(), entry.getContentHash(),
                                embedding.length, dimension));
                    }

                    appender.add(toRecord(schema, entry, vector, spec, builtAt));

                    // The input is sorted by content hash, so a change of hash is a new distinct
                    // vector. Counting this way keeps a rebuild of a billion rows from holding a
                    // set of every hash it has seen.
                    if (!entry.getContentHash().equals(lastHash)) {
                        distinctVectors++;
                        lastHash = entry.getContentHash();
                    }
                    maxSourceSequenceNumber = Math.max(maxSourceSequenceNumber, entry.getSourceSequenceNumber());
                }
            }

            // Both are only valid once the appender is closed.
            fileSize = appender.length();
            metrics = appender.metrics();
        } catch (Exception e) {
            // Nothing is committed on the way out, so a failed block leaves the previous projection
            // intact and only orphans the partial file for the catalog's cleanup.
            throw new IllegalStateException(
                    "Failed to write serving projection block for " + spec.getSourceTable(), e);
        }

        DataFile dataFile = DataFiles.builder(partitionSpec)
                .withEncryptedOutputFile(encryptedOutputFile)
                .withPartition(partitionKey)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(block.size())
                .withMetrics(metrics)
                .withFormat(FileFormat.PARQUET)
                .build();

        return new WrittenBlock(dataFile, block.size(), distinctVectors, dimension,
                maxSourceSequenceNumber, lastHash);
    }

    static Record toRecord(Schema schema,
                           ContentMapEntry entry,
                           EmbeddingEntry vector,
                           MaterializationSpec spec,
                           Instant builtAt) {
        Record record = GenericRecord.create(schema);
        // Derived from lineage rather than generated, so a rebuild produces the same ids and an
        // evaluation result or provenance record that references one keeps resolving. The
        // configuration id takes the old preprocessing id's place: it hashes strictly more of the
        // pipeline, so ids stay unique and stay stable for an unchanged spec.
        record.setField(Constants.VECTOR_ID_COLUMN, VectorIds.vectorId(
                entry.getSourceTable(),
                entry.getSourceRowId(),
                entry.getSourceSnapshotId(),
                entry.getChunkOrdinal(),
                spec.getModelName(),
                spec.getEmbeddingVersion(),
                spec.configId()));
        record.setField(Constants.SOURCE_TABLE_COLUMN, entry.getSourceTable());
        record.setField(Constants.SOURCE_ROW_ID_COLUMN, entry.getSourceRowId());
        record.setField(Constants.CHUNK_ORDINAL_COLUMN, entry.getChunkOrdinal());
        record.setField(Constants.CONTENT_HASH_COLUMN, entry.getContentHash());
        // From the spec, not from the entry: the projection's partition values must agree with the
        // spec the overwrite filter is built from, or the next rebuild cannot delete these files.
        record.setField(Constants.MODEL_VERSION_COLUMN, spec.modelVersion());
        record.setField(Constants.CONFIG_ID_COLUMN, spec.configId());
        record.setField(Constants.EMBEDDING_DIM_COLUMN, vector.getEmbedding().length);
        record.setField(Constants.EMBEDDING_COLUMN, toFloatList(vector.getEmbedding()));
        record.setField(Constants.TEXT_COLUMN, vector.getText());
        record.setField(Constants.SOURCE_SNAPSHOT_ID_COLUMN, entry.getSourceSnapshotId());
        record.setField(Constants.SOURCE_SEQUENCE_NUMBER_COLUMN, entry.getSourceSequenceNumber());
        // The build time, identical for every row in one rebuild, so the column answers "how old is
        // this projection" instead of "when was this content first embedded".
        record.setField(Constants.CREATED_AT_COLUMN, OffsetDateTime.ofInstant(builtAt, ZoneOffset.UTC));
        return record;
    }

    /** Fetches a block's vectors in batches, never one lookup per row. */
    private static Map<String, EmbeddingEntry> loadVectors(Table embeddingStore,
                                                           List<ContentMapEntry> block,
                                                           String modelVersion,
                                                           String configId) {
        Set<String> distinct = new LinkedHashSet<>();
        for (ContentMapEntry entry : block) {
            distinct.add(entry.getContentHash());
        }

        List<String> hashes = new ArrayList<>(distinct);
        Map<String, EmbeddingEntry> vectors = new HashMap<>(hashes.size());
        for (int start = 0; start < hashes.size(); start += EMBEDDING_LOAD_BATCH) {
            List<String> batch = hashes.subList(start, Math.min(hashes.size(), start + EMBEDDING_LOAD_BATCH));
            vectors.putAll(EmbeddingStore.load(embeddingStore, batch, modelVersion, configId));
        }
        return vectors;
    }

    /**
     * The partition tuple for the whole build.
     *
     * <p>Computed once from the spec rather than per record: a build covers exactly one
     * {@code (source_table, model_version)} pair, which is the same pair the overwrite filter
     * deletes. Only the partition columns of the template are read by the partition accessors.
     */
    private static PartitionKey partitionKeyFor(Schema schema,
                                                PartitionSpec partitionSpec,
                                                MaterializationSpec spec) {
        Record template = GenericRecord.create(schema);
        template.setField(Constants.SOURCE_TABLE_COLUMN, spec.getSourceTable());
        template.setField(Constants.MODEL_VERSION_COLUMN, spec.modelVersion());

        PartitionKey partitionKey = new PartitionKey(partitionSpec, schema);
        partitionKey.partition(template);
        return partitionKey;
    }

    /**
     * Refuses a projection written at a different format version, with the remediation spelled out.
     *
     * <p>Without this the mismatch surfaces later as "Cannot set unknown field" from deep inside a
     * Parquet write, which tells an operator nothing about what to do. The remedy here is cheaper
     * than for Tier 1: the projection is derived, so it can simply be dropped and rebuilt.
     */
    public static void requireCurrentFormat(Table table) {
        int version = formatVersionOf(table);
        if (version == Constants.VECTOR_FORMAT_VERSION) {
            return;
        }

        throw new IllegalStateException(String.format(
                "Serving projection %s is at format version %d but this build requires version %d. "
                        + "The projection is derived from the content map and the embedding store "
                        + "and costs no inference to rebuild: drop it explicitly "
                        + "(POST /api/admin/vector-table/rebuild on the worker) and rebuild.",
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

    /**
     * Lowercased, with every run of characters that is not a letter, digit, or underscore collapsed
     * to a single underscore. A source table is usually qualified ({@code db.schema.orders}), and
     * dots would otherwise be read as namespace separators by the catalog.
     */
    static String sanitize(String sourceTable) {
        String cleaned = sourceTable.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9_]+", "_")
                .replaceAll("^_+|_+$", "");
        if (cleaned.isEmpty()) {
            return "source";
        }
        return cleaned.length() <= MAX_SOURCE_NAME_CHARS
                ? cleaned
                : cleaned.substring(0, MAX_SOURCE_NAME_CHARS);
    }

    private static List<Float> toFloatList(float[] embedding) {
        List<Float> values = new ArrayList<>(embedding.length);
        for (float value : embedding) {
            values.add(value);
        }
        return values;
    }

    private record WrittenBlock(DataFile dataFile,
                                long rows,
                                long distinctVectors,
                                int embeddingDim,
                                long maxSourceSequenceNumber,
                                String lastContentHash) {
    }
}
