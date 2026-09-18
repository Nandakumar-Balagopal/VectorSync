package io.vectorsync.worker.service;

import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import io.vectorsync.worker.service.iceberg.IcebergTableService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class DemoSeedService {

    private static final String DEMO_NAMESPACE = "default";
    private static final String DEMO_TABLE = "products";

    private final IcebergCatalogService catalogService;
    private final IcebergTableService icebergTableService;

    public DemoSeedService(IcebergCatalogService catalogService,
                           IcebergTableService icebergTableService) {
        this.catalogService = catalogService;
        this.icebergTableService = icebergTableService;
    }

    public DemoSeedResult seedProductsTable() {
        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(DEMO_NAMESPACE), DEMO_TABLE);

        boolean existed = false;
        try {
            existed = catalog.tableExists(identifier);
        } catch (Exception e) {
            log.warn("Table existence check failed: {}", e.getMessage());
        }

        if (existed) {
            try {
                catalog.dropTable(identifier, true);
                existed = false;
            } catch (Exception e) {
                log.warn("Failed to drop existing demo table: {}", e.getMessage());
            }
        }

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "description", Types.StringType.get()),
                Types.NestedField.optional(4, "category", Types.StringType.get()),
                Types.NestedField.optional(5, "price", Types.DoubleType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(identifier, schema, spec);

        List<Record> records = List.of(
                buildRecord(schema, "p-100", "Trail Runner", "Lightweight trail running shoe", "shoes", 89.0),
                buildRecord(schema, "p-200", "City Sneaker", "Everyday sneaker with cushioned sole", "shoes", 74.0),
                buildRecord(schema, "p-300", "Summit Boot", "Waterproof hiking boot", "boots", 129.0),
                buildRecord(schema, "p-400", "Canvas Classic", "Affordable canvas shoe", "shoes", 45.0)
        );

        appendRecords(table, records);

        return new DemoSeedResult(identifier.toString(), records.size(), !existed);
    }

    /**
     * Creates (replacing any existing) an Iceberg source table with the given rows.
     *
     * <p>Unlike {@link #seedProductsTable()} this does not touch the vector table, so several
     * source tables can be seeded and materialized alongside each other.
     */
    /**
     * Appends rows to an existing demo table, leaving its snapshot history intact.
     *
     * <p>Separate from {@link #seedTable} because that drops and recreates, which produces a table
     * with no ancestry. An incremental pass is defined relative to a prior snapshot, so testing it
     * needs a real append: a new snapshot whose parent is the one already materialized.
     */
    public DemoMutationResult appendToTable(String qualifiedName, List<SeedRow> rows) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("At least one row is required");
        }

        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = qualifiedName.contains(".")
                ? TableIdentifier.parse(qualifiedName)
                : TableIdentifier.of(Namespace.of(DEMO_NAMESPACE), qualifiedName);

        if (!catalog.tableExists(identifier)) {
            throw new IllegalArgumentException("Table does not exist: " + qualifiedName);
        }

        Table table = catalog.loadTable(identifier);
        List<Record> records = new ArrayList<>(rows.size());
        for (SeedRow row : rows) {
            records.add(buildRecord(table.schema(), row.id(), row.name(), row.description(),
                    row.category(), row.price()));
        }

        appendRecords(table, records);
        return new DemoMutationResult("APPEND", qualifiedName, records.size());
    }

    /**
     * Replaces the table's contents in one commit, preserving ancestry.
     *
     * <p>Distinct from {@link #seedTable}, which drops and recreates and therefore produces a table
     * with no history -- {@code assess()} sees that as a re-anchor rather than a mutation, which is
     * the wrong shape for testing anything. This is what an engine emits for {@code UPDATE} or
     * {@code DELETE} on a table with no delete files: the old data files are removed and the
     * survivors are rewritten, as a single {@code overwrite} snapshot whose parent is the snapshot
     * already materialized.
     *
     * <p>Exists for the mutation benchmark. Every measurement this project had published was taken
     * on a table loaded once and left alone, which is the easy case and not the one the
     * architecture exists for.
     */
    public DemoMutationResult replaceRows(String qualifiedName, List<SeedRow> rows) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("At least one row is required");
        }

        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = qualifiedName.contains(".")
                ? TableIdentifier.parse(qualifiedName)
                : TableIdentifier.of(Namespace.of(DEMO_NAMESPACE), qualifiedName);
        if (!catalog.tableExists(identifier)) {
            throw new IllegalArgumentException("Table does not exist: " + qualifiedName);
        }

        Table table = catalog.loadTable(identifier);
        List<Record> records = new ArrayList<>(rows.size());
        for (SeedRow row : rows) {
            records.add(buildRecord(table.schema(), row.id(), row.name(), row.description(),
                    row.category(), row.price()));
        }

        List<org.apache.iceberg.DataFile> written =
                io.vectorsync.format.io.IcebergAppender.writeFiles(table, records);
        org.apache.iceberg.OverwriteFiles overwrite = table.newOverwrite()
                .overwriteByRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue());
        written.forEach(overwrite::addFile);
        overwrite.commit();

        return new DemoMutationResult("REPLACE", qualifiedName, records.size());
    }

    /**
     * Deletes one identity partition, which is what an engine emits for a partition-scoped DELETE.
     *
     * <p>The row filter names a partition column deliberately: Iceberg deletes whole files by row
     * filter and refuses one it cannot prove covers a file entirely, so this succeeds where a
     * filter on a plain column would not. It exists to exercise the reconcile's partition scoping,
     * which an unpartitioned source cannot reach -- assess() then reports no affected partitions
     * and whole-table is the only correct answer.
     */
    public DemoMutationResult deletePartition(String qualifiedName, String column, String value) {
        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = qualifiedName.contains(".")
                ? TableIdentifier.parse(qualifiedName)
                : TableIdentifier.of(Namespace.of(DEMO_NAMESPACE), qualifiedName);
        if (!catalog.tableExists(identifier)) {
            throw new IllegalArgumentException("Table does not exist: " + qualifiedName);
        }

        Table table = catalog.loadTable(identifier);
        table.newDelete()
                .deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.equal(column, value))
                .commit();
        log.info("Deleted partition {}={} from {}", column, value, identifier);
        return new DemoMutationResult("DELETE_PARTITION", qualifiedName, 0);
    }

    public DemoSeedResult seedTable(String qualifiedName, List<SeedRow> rows) {
        return seedTable(qualifiedName, rows, null);
    }

    /**
     * @param partitionColumn identity-partition the demo table by this column, or null for none.
     *                        Exists because a reconcile can only be scoped to the partitions a
     *                        change touched, and an unpartitioned source has nothing to scope to --
     *                        so the partition-scoped path could not be exercised at all against a
     *                        table this seeder produced.
     */
    public DemoSeedResult seedTable(String qualifiedName, List<SeedRow> rows,
                                    String partitionColumn) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("At least one row is required");
        }

        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = qualifiedName.contains(".")
                ? TableIdentifier.parse(qualifiedName)
                : TableIdentifier.of(Namespace.of(DEMO_NAMESPACE), qualifiedName);

        boolean existed = false;
        try {
            existed = catalog.tableExists(identifier);
            if (existed) {
                catalog.dropTable(identifier, true);
            }
        } catch (Exception e) {
            log.warn("Could not drop existing table {}: {}", identifier, e.getMessage());
        }

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "description", Types.StringType.get()),
                Types.NestedField.optional(4, "category", Types.StringType.get()),
                Types.NestedField.optional(5, "price", Types.DoubleType.get())
        );

        PartitionSpec spec = partitionColumn == null || partitionColumn.isBlank()
                ? PartitionSpec.unpartitioned()
                : PartitionSpec.builderFor(schema).identity(partitionColumn.trim()).build();
        Table table = catalog.createTable(identifier, schema, spec);

        List<Record> records = rows.stream()
                .map(row -> buildRecord(schema, row.id(), row.name(), row.description(),
                        row.category(), row.price()))
                .toList();

        appendRecords(table, records);
        log.info("Seeded {} with {} rows", identifier, records.size());

        return new DemoSeedResult(identifier.toString(), records.size(), !existed);
    }

    public record SeedRow(String id, String name, String description, String category, Double price) {
    }

    public DemoMutationResult appendProduct(String id,
                                            String name,
                                            String description,
                                            String category,
                                            Double price) {
        Table table = loadProductsTable();
        Record record = buildRecord(table.schema(), id, name, description, category, price);
        appendRecords(table, List.of(record));
        // The real total, like the update and delete paths report. This returned a hardcoded 1,
        // which read as "one record left in the table" and looked exactly like an overwrite had
        // just destroyed the corpus.
        return new DemoMutationResult("INSERT", id, readProducts(table).size());
    }

    public DemoMutationResult updateProduct(String id,
                                            String name,
                                            String description,
                                            String category,
                                            Double price) {
        Table table = loadProductsTable();
        List<Record> records = readProducts(table);
        boolean updated = false;

        for (int i = 0; i < records.size(); i++) {
            if (id.equals(String.valueOf(records.get(i).getField("id")))) {
                records.set(i, buildRecord(table.schema(), id, name, description, category, price));
                updated = true;
                break;
            }
        }

        if (!updated) {
            throw new IllegalArgumentException("Demo product not found: " + id);
        }

        replaceRecords(table, records);
        return new DemoMutationResult("UPDATE", id, records.size());
    }

    public DemoMutationResult deleteProduct(String id) {
        Table table = loadProductsTable();
        List<Record> records = readProducts(table).stream()
                .filter(record -> !id.equals(String.valueOf(record.getField("id"))))
                .toList();

        replaceRecords(table, records);
        return new DemoMutationResult("DELETE", id, records.size());
    }

    private Record buildRecord(Schema schema,
                               String id,
                               String name,
                               String description,
                               String category,
                               Double price) {
        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("name", name);
        record.setField("description", description);
        record.setField("category", category);
        record.setField("price", price);
        return record;
    }

    /**
     * Appends records, one data file per partition.
     *
     * <p>Delegates to {@code IcebergAppender}, which groups records by their partition tuple. The
     * previous implementation wrote every record into a single file under a single PartitionKey,
     * which is silently wrong on a partitioned table: all sixty rows of a three-partition demo
     * table claimed to belong to one partition, so a partition-scoped DELETE removed nothing and
     * the reconcile could never narrow its scope. The table looked partitioned and behaved as if it
     * were not.
     */
    private void appendRecords(Table table, List<Record> records) {
        if (records.isEmpty()) {
            return;
        }
        io.vectorsync.format.io.IcebergAppender.append(table, records);
    }

    private void replaceRecords(Table table, List<Record> records) {
        OverwriteFiles overwrite = table.newOverwrite()
                .overwriteByRowFilter(Expressions.alwaysTrue());

        if (!records.isEmpty()) {
            // One file per partition, same reason as appendRecords: a single file under a single
            // PartitionKey makes a partitioned table behave as though it were not.
            io.vectorsync.format.io.IcebergAppender.writeFiles(table, records)
                    .forEach(overwrite::addFile);
        }

        overwrite.commit();
    }

    private DataFile writeDataFile(Table table, List<Record> records) {
        Schema schema = table.schema();
        PartitionSpec spec = table.spec();
        OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .format(FileFormat.PARQUET)
                .build();

        PartitionKey partitionKey = new PartitionKey(spec, schema);
        partitionKey.partition(records.get(0));

        EncryptedOutputFile encryptedOutputFile = outputFileFactory.newOutputFile(partitionKey);
        OutputFile outputFile = encryptedOutputFile.encryptingOutputFile();

        long recordCount = records.size();
        long fileSize;
        org.apache.iceberg.Metrics metrics;

        try (FileAppender<Record> appender = Parquet.write(outputFile)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build()) {
            
            for (Record record : records) {
                appender.add(record);
            }
            // Must get metrics AFTER closing the appender (done by try-with-resources)
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write demo records", e);
        }

        // Get file size and metrics from the output file after appender is closed
        try {
            fileSize = outputFile.toInputFile().getLength();
        } catch (Exception e) {
            log.warn("Could not get file size, using record count estimate", e);
            fileSize = recordCount * 100; // Rough estimate
        }

        DataFile dataFile = DataFiles.builder(spec)
                .withEncryptedOutputFile(encryptedOutputFile)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(recordCount)
                .withFormat(FileFormat.PARQUET)
                .build();

        return dataFile;
    }

    private Table loadProductsTable() {
        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(DEMO_NAMESPACE), DEMO_TABLE);
        return catalog.loadTable(identifier);
    }

    private List<Record> readProducts(Table table) {
        List<Record> records = new ArrayList<>();

        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                records.add(buildRecord(
                        table.schema(),
                        String.valueOf(row.getField("id")),
                        String.valueOf(row.getField("name")),
                        row.getField("description") == null ? null : String.valueOf(row.getField("description")),
                        row.getField("category") == null ? null : String.valueOf(row.getField("category")),
                        row.getField("price") instanceof Number ? ((Number) row.getField("price")).doubleValue() : null
                ));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read demo products", e);
        }

        return records;
    }

    public record DemoSeedResult(String tableName, int recordsWritten, boolean created) {
    }

    /**
     * @param remainingRecords rows in the table after the operation, for all three operations
     */
    public record DemoMutationResult(String operation, String id, int remainingRecords) {
    }
}
