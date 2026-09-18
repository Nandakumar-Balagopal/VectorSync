package io.vectorsync.format.io;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Appends generic records to an Iceberg table as partitioned Parquet, in a single commit.
 *
 * <p>Shared by the vector table, the index manifest, and the alias log so that all three get the
 * same atomicity guarantee: either every record in the batch is visible or none is. Alias
 * promotion depends on that — a half-applied promotion would leave serving pointing at nothing.
 */
@Slf4j
public final class IcebergAppender {

    private IcebergAppender() {
    }

    /**
     * @throws IllegalStateException if any partition fails to write, after rolling back by simply
     *         not committing. Partial appends are never committed.
     */
    public static void append(Table table, List<Record> records) {
        List<DataFile> dataFiles = writeFiles(table, records);
        if (dataFiles.isEmpty()) {
            return;
        }

        AppendFiles append = table.newAppend();
        dataFiles.forEach(append::appendFile);
        append.commit();

        log.debug("Appended {} records across {} partitions to {}", records.size(), dataFiles.size(), table.name());
    }

    /**
     * Writes records as partitioned Parquet and returns the data files <em>without committing</em>.
     *
     * <p>Exists because an append is not always the commit a caller needs. Replacing one slice of a
     * table has to delete the old files and add the new ones in a single snapshot
     * ({@code newOverwrite}), which means the write and the commit must be separable -- and because
     * {@link #append} did not separate them, {@code ProjectionBuilder.writeBlock} reproduced this
     * logic by hand, including the two details below that are easy to get wrong the second time.
     *
     * <p>The caller owns the commit and therefore the rollback: files returned here and never
     * committed are orphans for the catalog's cleanup, not visible rows.
     */
    public static List<DataFile> writeFiles(Table table, List<Record> records) {
        if (records == null || records.isEmpty()) {
            return List.of();
        }

        Schema schema = table.schema();
        PartitionSpec spec = table.spec();
        OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .format(FileFormat.PARQUET)
                .build();

        Map<String, Batch> batches = new LinkedHashMap<>();
        for (Record record : records) {
            PartitionKey partitionKey = new PartitionKey(spec, schema);
            partitionKey.partition(record);
            batches.computeIfAbsent(partitionKey.toPath(), key -> new Batch(partitionKey))
                    .records.add(record);
        }

        List<DataFile> dataFiles = new ArrayList<>(batches.size());
        for (Batch batch : batches.values()) {
            dataFiles.add(writeBatch(schema, spec, outputFileFactory, batch));
        }
        return dataFiles;
    }

    private static DataFile writeBatch(Schema schema,
                                       PartitionSpec spec,
                                       OutputFileFactory outputFileFactory,
                                       Batch batch) {
        EncryptedOutputFile encryptedOutputFile = outputFileFactory.newOutputFile(batch.partitionKey);
        OutputFile outputFile = encryptedOutputFile.encryptingOutputFile();

        long fileSize;
        Metrics metrics;

        try {
            FileAppender<Record> appender = Parquet.write(outputFile)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build();

            try (appender) {
                for (Record record : batch.records) {
                    appender.add(record);
                }
            }

            // Both are only valid once the appender is closed.
            fileSize = appender.length();
            metrics = appender.metrics();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write Parquet batch for partition "
                    + batch.partitionKey.toPath(), e);
        }

        return DataFiles.builder(spec)
                .withEncryptedOutputFile(encryptedOutputFile)
                // Required, not optional. Iceberg serves identity-partition columns as constants
                // folded from this tuple rather than reading them from the data file, so omitting
                // it makes every partition column read back as null.
                .withPartition(batch.partitionKey)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(batch.records.size())
                .withMetrics(metrics)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private static final class Batch {
        private final PartitionKey partitionKey;
        private final List<Record> records = new ArrayList<>();

        private Batch(PartitionKey partitionKey) {
            this.partitionKey = partitionKey;
        }
    }
}
