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
        boolean vectorsReset = icebergTableService.dropVectorTable();

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

        return new DemoSeedResult(identifier.toString(), records.size(), !existed, vectorsReset);
    }

    /**
     * Creates (replacing any existing) an Iceberg source table with the given rows.
     *
     * <p>Unlike {@link #seedProductsTable()} this does not touch the vector table, so several
     * source tables can be seeded and materialized alongside each other.
     */
    public DemoSeedResult seedTable(String qualifiedName, List<SeedRow> rows) {
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

        Table table = catalog.createTable(identifier, schema, PartitionSpec.unpartitioned());

        List<Record> records = rows.stream()
                .map(row -> buildRecord(schema, row.id(), row.name(), row.description(),
                        row.category(), row.price()))
                .toList();

        appendRecords(table, records);
        log.info("Seeded {} with {} rows", identifier, records.size());

        return new DemoSeedResult(identifier.toString(), records.size(), !existed, false);
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

    private void appendRecords(Table table, List<Record> records) {
        if (records.isEmpty()) {
            return;
        }

        DataFile dataFile = writeDataFile(table, records);
        AppendFiles append = table.newAppend();
        append.appendFile(dataFile);
        append.commit();
    }

    private void replaceRecords(Table table, List<Record> records) {
        OverwriteFiles overwrite = table.newOverwrite()
                .overwriteByRowFilter(Expressions.alwaysTrue());

        if (!records.isEmpty()) {
            overwrite.addFile(writeDataFile(table, records));
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

    public record DemoSeedResult(String tableName, int recordsWritten, boolean created, boolean vectorsReset) {
    }

    /**
     * @param remainingRecords rows in the table after the operation, for all three operations
     */
    public record DemoMutationResult(String operation, String id, int remainingRecords) {
    }
}
