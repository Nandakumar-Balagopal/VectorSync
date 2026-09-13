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
import java.util.List;
import java.util.Optional;

/**
 * Which index each alias currently serves.
 *
 * <p>This is the promotion primitive: swapping production traffic between index versions is one
 * Iceberg commit appending a new alias entry, and rolling back is another append naming the
 * previous index. Nothing is mutated, so the promotion history doubles as an audit trail.
 *
 * <p>Ordering is by {@code updated_at}, which is appropriate here in a way it is not for vector
 * resolution: a promotion genuinely is an operational event in wall-clock time, not a data
 * version. This assumes a single promoting writer (the control plane); concurrent promoters on
 * skewed clocks could disagree about which is newest.
 */
@Slf4j
public final class IndexAliasStore {

    public static final String PRODUCTION = "production";

    private static final String ALIAS_NAME = "alias_name";
    private static final String SOURCE_TABLE = "source_table";
    private static final String INDEX_ID = "index_id";
    private static final String UPDATED_AT = "updated_at";
    private static final String UPDATED_BY = "updated_by";
    private static final String NOTE = "note";

    private final Catalog catalog;
    private final String namespace;

    public IndexAliasStore(Catalog catalog, String namespace) {
        this.catalog = catalog;
        this.namespace = namespace;
    }

    public static Schema schema() {
        return new Schema(
                Types.NestedField.required(1, ALIAS_NAME, Types.StringType.get()),
                Types.NestedField.required(2, SOURCE_TABLE, Types.StringType.get()),
                Types.NestedField.required(3, INDEX_ID, Types.StringType.get()),
                Types.NestedField.required(4, UPDATED_AT, Types.TimestampType.withZone()),
                Types.NestedField.optional(5, UPDATED_BY, Types.StringType.get()),
                Types.NestedField.optional(6, NOTE, Types.StringType.get())
        );
    }

    public static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .identity(SOURCE_TABLE)
                .build();
    }

    public TableIdentifier identifier() {
        return TableIdentifier.of(Namespace.of(namespace), Constants.INDEX_ALIAS_TABLE_NAME);
    }

    public Table loadOrCreate() {
        TableIdentifier identifier = identifier();
        try {
            if (catalog.tableExists(identifier)) {
                return catalog.loadTable(identifier);
            }
        } catch (Exception e) {
            log.warn("Index alias existence check failed: {}", e.getMessage());
        }

        log.info("Creating index alias table {}", identifier);
        Schema schema = schema();
        try {
            catalog.createTable(identifier, schema, partitionSpec(schema));
        } catch (Exception e) {
            log.warn("Index alias creation raced or failed, reloading: {}", e.getMessage());
        }
        return catalog.loadTable(identifier);
    }

    /**
     * Points an alias at an index. Atomic: a single Iceberg commit, so serving never observes a
     * half-applied promotion.
     */
    public IndexAliasEntry promote(String aliasName,
                                   String sourceTable,
                                   String indexId,
                                   String updatedBy,
                                   String note) {
        IndexAliasEntry entry = IndexAliasEntry.builder()
                .aliasName(aliasName)
                .sourceTable(sourceTable)
                .indexId(indexId)
                .updatedAt(Instant.now())
                .updatedBy(updatedBy)
                .note(note)
                .build();

        Table table = loadOrCreate();
        IcebergAppender.append(table, List.of(toRecord(table.schema(), entry)));

        log.info("Alias '{}' for {} now serves index {} ({})", aliasName, sourceTable, indexId, note);
        return entry;
    }

    /** The index an alias currently resolves to. */
    public Optional<IndexAliasEntry> resolve(String aliasName, String sourceTable) {
        return history(sourceTable).stream()
                .filter(entry -> entry.getAliasName().equals(aliasName))
                .max(Comparator.comparing(IndexAliasEntry::getUpdatedAt,
                        Comparator.nullsFirst(Comparator.naturalOrder())));
    }

    public Optional<IndexAliasEntry> resolveProduction(String sourceTable) {
        return resolve(PRODUCTION, sourceTable);
    }

    /**
     * The index an alias pointed at before its current target, which is what a rollback promotes.
     */
    public Optional<IndexAliasEntry> previous(String aliasName, String sourceTable) {
        List<IndexAliasEntry> ordered = history(sourceTable).stream()
                .filter(entry -> entry.getAliasName().equals(aliasName))
                .sorted(Comparator.comparing(IndexAliasEntry::getUpdatedAt,
                        Comparator.nullsFirst(Comparator.naturalOrder())).reversed())
                .toList();

        String current = ordered.isEmpty() ? null : ordered.get(0).getIndexId();
        return ordered.stream()
                .filter(entry -> !entry.getIndexId().equals(current))
                .findFirst();
    }

    /** Full promotion history for a table, newest last. */
    public List<IndexAliasEntry> history(String sourceTable) {
        Table table;
        try {
            table = loadOrCreate();
        } catch (Exception e) {
            log.warn("Index alias table unavailable: {}", e.getMessage());
            return List.of();
        }

        List<IndexAliasEntry> entries = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                try {
                    IndexAliasEntry entry = fromRecord(row);
                    if (sourceTable == null || sourceTable.equals(entry.getSourceTable())) {
                        entries.add(entry);
                    }
                } catch (Exception e) {
                    log.warn("Skipping unreadable alias row: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read index alias table: {}", e.getMessage());
            return List.of();
        }

        return entries.stream()
                .sorted(Comparator.comparing(IndexAliasEntry::getUpdatedAt,
                        Comparator.nullsFirst(Comparator.naturalOrder())))
                .toList();
    }

    static Record toRecord(Schema schema, IndexAliasEntry entry) {
        Record record = GenericRecord.create(schema);
        record.setField(ALIAS_NAME, entry.getAliasName());
        record.setField(SOURCE_TABLE, entry.getSourceTable());
        record.setField(INDEX_ID, entry.getIndexId());
        Instant updatedAt = entry.getUpdatedAt() == null ? Instant.now() : entry.getUpdatedAt();
        record.setField(UPDATED_AT, OffsetDateTime.ofInstant(updatedAt, ZoneOffset.UTC));
        record.setField(UPDATED_BY, entry.getUpdatedBy());
        record.setField(NOTE, entry.getNote());
        return record;
    }

    static IndexAliasEntry fromRecord(Record record) {
        Object updatedAt = record.getField(UPDATED_AT);
        Instant instant = null;
        if (updatedAt instanceof Instant value) {
            instant = value;
        } else if (updatedAt instanceof OffsetDateTime value) {
            instant = value.toInstant();
        } else if (updatedAt instanceof Long micros) {
            instant = Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L),
                    Math.floorMod(micros, 1_000_000L) * 1_000L);
        }

        return IndexAliasEntry.builder()
                .aliasName(asString(record.getField(ALIAS_NAME)))
                .sourceTable(asString(record.getField(SOURCE_TABLE)))
                .indexId(asString(record.getField(INDEX_ID)))
                .updatedAt(instant)
                .updatedBy(asString(record.getField(UPDATED_BY)))
                .note(asString(record.getField(NOTE)))
                .build();
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }
}
