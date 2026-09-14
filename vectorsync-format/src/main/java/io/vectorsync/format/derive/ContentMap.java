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
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tier-1 table mapping source rows to content: {@code (source_table, source_row_id, chunk_ordinal,
 * config_id)} at a source version, pointing at a {@code content_hash}.
 *
 * <p>Append-only with tombstones, which is what makes the mapping a history rather than a cache. An
 * edit appends an entry at a higher source sequence number; a delete appends one with
 * {@code deleted} set and no content hash. Nothing is rewritten, so the same question asked at a
 * given source version always returns the same answer -- the property the whole reproducibility
 * claim rests on.
 *
 * <p>Together with {@link EmbeddingStore} this replaces the single row-keyed vector table. The split
 * matters because the two halves change at different rates: rows are edited constantly while the
 * text they carry usually does not change, so the expensive half stays untouched and only these
 * cheap pointer rows are written.
 *
 * <p>Field IDs are part of the on-disk contract and must never be renumbered -- Iceberg resolves
 * columns by ID rather than name, so changing one silently re-points existing data files.
 */
@Slf4j
public final class ContentMap {

    /**
     * Oldest-first, so a later entry supersedes an earlier one for the same chunk.
     *
     * <p>Ordered by Iceberg's snapshot <em>sequence number</em>, which the spec guarantees increases
     * monotonically per table. Never by {@code source_snapshot_id}: Iceberg snapshot ids are random
     * longs, so comparing them numerically reorders history at random and a tombstone can lose to
     * the row it was meant to delete. That bug has been fixed three times in this repository; do not
     * reintroduce it. Commit time and then {@code created_at} break ties, which only arise when one
     * source version is materialized more than once.
     */
    private static final Comparator<ContentMapEntry> OLDEST_FIRST =
            Comparator.comparingLong(ContentMapEntry::getSourceSequenceNumber)
                    .thenComparingLong(ContentMapEntry::getSourceCommittedAtMillis)
                    .thenComparing(ContentMapEntry::getCreatedAt,
                            Comparator.nullsFirst(Comparator.naturalOrder()));

    private ContentMap() {
    }

    public static Schema schema() {
        return new Schema(
                Types.NestedField.required(1, Constants.SOURCE_TABLE_COLUMN, Types.StringType.get()),
                Types.NestedField.required(2, Constants.SOURCE_ROW_ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(3, Constants.CHUNK_ORDINAL_COLUMN, Types.IntegerType.get()),
                // Optional because a tombstone carries no content: the row is gone, so there is no
                // text to point at. A required column here would force writers to invent a sentinel
                // hash, which would then look like real content to the embedding store.
                Types.NestedField.optional(4, Constants.CONTENT_HASH_COLUMN, Types.StringType.get()),
                Types.NestedField.required(5, Constants.CONFIG_ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(6, Constants.MODEL_VERSION_COLUMN, Types.StringType.get()),
                Types.NestedField.required(7, Constants.SOURCE_SNAPSHOT_ID_COLUMN, Types.LongType.get()),
                Types.NestedField.required(8, Constants.SOURCE_SEQUENCE_NUMBER_COLUMN, Types.LongType.get()),
                Types.NestedField.required(9, Constants.SOURCE_COMMITTED_AT_COLUMN, Types.LongType.get()),
                Types.NestedField.required(10, Constants.DELETED_COLUMN, Types.BooleanType.get()),
                Types.NestedField.required(11, Constants.CREATED_AT_COLUMN, Types.TimestampType.withZone())
        );
    }

    /**
     * Partitioned by source table and configuration, the two dimensions every read scopes to.
     * Without the configuration partition, resolving one spec's mapping would scan every spec ever
     * run against that table, including the ones a migration is trying to leave behind.
     */
    public static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .identity(Constants.SOURCE_TABLE_COLUMN)
                .identity(Constants.CONFIG_ID_COLUMN)
                .build();
    }

    public static TableIdentifier identifier(String namespace) {
        return TableIdentifier.of(Namespace.of(namespace), Constants.CONTENT_MAP_TABLE_NAME);
    }

    /**
     * Loads the content map, creating it when absent.
     *
     * <p>Refuses to touch a table written in another format rather than dropping it. This table is
     * the row-to-vector history, so destroying it must be an explicit operator decision even though
     * it can be rederived from the source.
     */
    public static Table loadOrCreate(Catalog catalog, String namespace) {
        TableIdentifier identifier = identifier(namespace);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Content map existence check failed: {}", e.getMessage());
            tableExists = false;
        }

        if (tableExists) {
            Table existing = catalog.loadTable(identifier);
            requireCurrentFormat(existing);
            return existing;
        }

        log.info("Creating content map {}.{} at format version {}",
                namespace, Constants.CONTENT_MAP_TABLE_NAME, Constants.VECTOR_FORMAT_VERSION);
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
            log.warn("Content map creation raced or failed, reloading: {}", e.getMessage());
        }

        Table created = catalog.loadTable(identifier);
        requireCurrentFormat(created);
        return created;
    }

    /** Returns the content map, or {@code null} when it does not exist yet. */
    public static Table loadIfExists(Catalog catalog, String namespace) {
        TableIdentifier identifier = identifier(namespace);

        boolean tableExists;
        try {
            tableExists = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Content map existence check failed: {}", e.getMessage());
            return null;
        }

        if (!tableExists) {
            log.warn("Content map {}.{} does not exist yet",
                    namespace, Constants.CONTENT_MAP_TABLE_NAME);
            return null;
        }

        Table table = catalog.loadTable(identifier);
        requireCurrentFormat(table);
        return table;
    }

    /** Drops the content map if present. Returns true when a table was actually dropped. */
    public static boolean drop(Catalog catalog, String namespace) {
        TableIdentifier identifier = identifier(namespace);

        try {
            if (catalog.tableExists(identifier)) {
                catalog.dropTable(identifier, true);
                log.info("Dropped content map {}", identifier);
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed to drop content map {}: {}", identifier, e.getMessage());
        }

        return false;
    }

    /**
     * Appends mapping entries in one commit, so a sync batch and its tombstones become visible
     * together. A partially visible batch would show a row pointing at content that the batch was
     * about to delete.
     */
    public static void append(Table table, List<ContentMapEntry> entries) {
        if (table == null || entries == null || entries.isEmpty()) {
            return;
        }

        Schema schema = table.schema();
        List<Record> records = new ArrayList<>(entries.size());
        for (ContentMapEntry entry : entries) {
            records.add(toRecord(schema, entry));
        }

        IcebergAppender.append(table, records);
        log.debug("Appended {} content map entries", records.size());
    }

    /** The current mapping: one entry per live chunk, tombstoned chunks removed. */
    public static List<ContentMapEntry> liveEntries(Table table, String sourceTable, String configId) {
        return liveEntriesAsOf(table, sourceTable, configId, Long.MAX_VALUE);
    }

    /**
     * The mapping as it stood at a source sequence number, ignoring anything derived from a later
     * one. This is what makes a materialized embedding set reproducible: the same source table and
     * the same target version always resolve to the same content hashes.
     *
     * <p>Takes a sequence number, not a snapshot id, because only sequence numbers are ordered.
     */
    public static List<ContentMapEntry> liveEntriesAsOf(Table table,
                                                        String sourceTable,
                                                        String configId,
                                                        long asOfSourceSequenceNumber) {
        if (table == null) {
            return List.of();
        }

        Expression filter = Expressions.and(
                Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable),
                Expressions.equal(Constants.CONFIG_ID_COLUMN, configId));
        if (asOfSourceSequenceNumber < Long.MAX_VALUE) {
            // Pushed down rather than applied after the scan: a long-lived table holds every
            // version of every row, and an as-of read of an early version must not pay for the
            // history that came after it. Omitted when unbounded so the common "current state"
            // read leaves no residual predicate to evaluate per row.
            filter = Expressions.and(filter, Expressions.lessThanOrEqual(
                    Constants.SOURCE_SEQUENCE_NUMBER_COLUMN, asOfSourceSequenceNumber));
        }

        List<ContentMapEntry> history = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(filter)
                .build()) {

            for (Record row : rows) {
                history.add(fromRecord(row));
            }
        } catch (Exception e) {
            throw new IllegalStateException(String.format(
                    "Failed to read content map for %s / %s", sourceTable, configId), e);
        }

        // Sorting oldest-first and overwriting into a map is how history collapses: the last write
        // per chunk key is by definition the newest entry for that chunk.
        Map<String, ContentMapEntry> newestByChunk = new LinkedHashMap<>();
        history.stream()
                .sorted(OLDEST_FIRST)
                .forEach(entry -> newestByChunk.put(entry.chunkKey(), entry));

        // Tombstones are dropped only after resolution. Filtering them out earlier would let an
        // older live entry win and resurrect a deleted row.
        return newestByChunk.values().stream()
                .filter(ContentMapEntry::isLive)
                .toList();
    }

    /**
     * Highest source sequence number this mapping covers, or 0 when nothing has been materialized.
     * How current the derived data is for a table and configuration.
     *
     * <p>Called on every status request, so it projects the one column it needs. That is safe here
     * because both filter columns are identity partitions: partition pruning satisfies them before
     * any file is opened, leaving no residual predicate that would need them in the projection.
     */
    public static long latestSequenceNumber(Table table, String sourceTable, String configId) {
        if (table == null) {
            return 0L;
        }

        long latest = 0L;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .select(Constants.SOURCE_SEQUENCE_NUMBER_COLUMN)
                .build()) {

            for (Record row : rows) {
                latest = Math.max(latest, asLong(row.getField(Constants.SOURCE_SEQUENCE_NUMBER_COLUMN)));
            }
        } catch (Exception e) {
            log.warn("Failed to read content map watermark for {} / {}: {}",
                    sourceTable, configId, e.getMessage());
            return 0L;
        }

        return latest;
    }

    /**
     * Refuses a content map written at a different format version, with the remediation spelled out.
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
                "Content map %s is at format version %d but this build requires version %d. The "
                        + "row-to-content mapping is derived data and must be rebuilt: drop it "
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

    static Record toRecord(Schema schema, ContentMapEntry entry) {
        if (isBlank(entry.getSourceTable()) || isBlank(entry.getSourceRowId())) {
            throw new IllegalArgumentException(
                    "Content map entry without a source table and row id has no identity");
        }
        if (isBlank(entry.getConfigId())) {
            throw new IllegalArgumentException(
                    "Content map entry for " + entry.getSourceRowId() + " has no config id");
        }
        // A live entry that points nowhere is indistinguishable from a tombstone on read, so it is
        // rejected at the boundary instead of silently vanishing from liveEntries.
        if (!entry.isDeleted() && isBlank(entry.getContentHash())) {
            throw new IllegalArgumentException(
                    "Live content map entry for " + entry.getSourceRowId() + " has no content hash");
        }

        Record record = GenericRecord.create(schema);
        record.setField(Constants.SOURCE_TABLE_COLUMN, entry.getSourceTable());
        record.setField(Constants.SOURCE_ROW_ID_COLUMN, entry.getSourceRowId());
        record.setField(Constants.CHUNK_ORDINAL_COLUMN, entry.getChunkOrdinal());
        record.setField(Constants.CONTENT_HASH_COLUMN, entry.getContentHash());
        record.setField(Constants.CONFIG_ID_COLUMN, entry.getConfigId());
        record.setField(Constants.MODEL_VERSION_COLUMN, entry.getModelVersion());
        record.setField(Constants.SOURCE_SNAPSHOT_ID_COLUMN, entry.getSourceSnapshotId());
        record.setField(Constants.SOURCE_SEQUENCE_NUMBER_COLUMN, entry.getSourceSequenceNumber());
        record.setField(Constants.SOURCE_COMMITTED_AT_COLUMN, entry.getSourceCommittedAtMillis());
        record.setField(Constants.DELETED_COLUMN, entry.isDeleted());

        Instant createdAt = entry.getCreatedAt() == null ? Instant.now() : entry.getCreatedAt();
        record.setField(Constants.CREATED_AT_COLUMN, OffsetDateTime.ofInstant(createdAt, ZoneOffset.UTC));
        return record;
    }

    static ContentMapEntry fromRecord(Record record) {
        return ContentMapEntry.builder()
                .sourceTable(asString(record.getField(Constants.SOURCE_TABLE_COLUMN)))
                .sourceRowId(asString(record.getField(Constants.SOURCE_ROW_ID_COLUMN)))
                .chunkOrdinal(asInt(record.getField(Constants.CHUNK_ORDINAL_COLUMN)))
                .contentHash(asString(record.getField(Constants.CONTENT_HASH_COLUMN)))
                .configId(asString(record.getField(Constants.CONFIG_ID_COLUMN)))
                .modelVersion(asString(record.getField(Constants.MODEL_VERSION_COLUMN)))
                .sourceSnapshotId(asLong(record.getField(Constants.SOURCE_SNAPSHOT_ID_COLUMN)))
                .sourceSequenceNumber(asLong(record.getField(Constants.SOURCE_SEQUENCE_NUMBER_COLUMN)))
                .sourceCommittedAtMillis(asLong(record.getField(Constants.SOURCE_COMMITTED_AT_COLUMN)))
                .deleted(Boolean.TRUE.equals(record.getField(Constants.DELETED_COLUMN)))
                .createdAt(toInstant(record.getField(Constants.CREATED_AT_COLUMN)))
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

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
