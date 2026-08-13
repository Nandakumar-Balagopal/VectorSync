package io.vectorsync.embeddingworker.writer;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.VectorRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
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
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for writing vector embeddings to Iceberg tables.
 * Handles batching by partition and efficient Parquet file writes.
 */
@Service
@Slf4j
public class VectorWriter {

    private final Catalog catalog;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public VectorWriter(Catalog catalog) {
        this.catalog = catalog;
    }

    public void writeVectors(List<VectorRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        log.info("Writing {} vector records to Iceberg", records.size());
        Table table;
        try {
            table = loadOrCreateVectorTable();
        } catch (Exception e) {
            log.error("Vector table unavailable, skipping write: {}", e.getMessage());
            return;
        }

        Schema schema = table.schema();
        PartitionSpec spec = table.spec();
        OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .format(FileFormat.PARQUET)
                .build();

        // Group records by partition
        Map<String, PartitionBatch> batches = new HashMap<>();
        for (VectorRecord record : records) {
            Record icebergRecord = toIcebergRecord(schema, record);
            PartitionKey partitionKey = new PartitionKey(spec, schema);
            partitionKey.partition(icebergRecord);
            String partitionPath = partitionKey.toPath();

            batches.computeIfAbsent(partitionPath, key -> new PartitionBatch(partitionKey))
                    .records.add(icebergRecord);
        }

        // Write each partition batch
        AppendFiles append = table.newAppend();
        for (PartitionBatch batch : batches.values()) {
            EncryptedOutputFile encryptedOutputFile = outputFileFactory.newOutputFile(batch.partitionKey);
            OutputFile outputFile = encryptedOutputFile.encryptingOutputFile();
            long recordCount = batch.records.size();
            long fileSize;
            org.apache.iceberg.Metrics metrics;

            try (FileAppender<Record> appender = Parquet.write(outputFile)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build()) {
                
                for (Record icebergRecord : batch.records) {
                    appender.add(icebergRecord);
                }
                fileSize = appender.length();
                metrics = appender.metrics();
                
            } catch (Exception e) {
                log.error("Failed to write vector batch: {}", e.getMessage(), e);
                continue;
            }

            DataFile dataFile = DataFiles.builder(spec)
                    .withEncryptedOutputFile(encryptedOutputFile)
                    .withFileSizeInBytes(fileSize)
                    .withRecordCount(recordCount)
                    .withMetrics(metrics)
                    .withFormat(FileFormat.PARQUET)
                    .build();

            append.appendFile(dataFile);
        }

        append.commit();
        log.info("Successfully wrote {} vectors to Iceberg", records.size());
    }

    private Table loadOrCreateVectorTable() {
        Namespace namespace = Namespace.of(vectorNamespace);
        TableIdentifier identifier = TableIdentifier.of(namespace, Constants.VECTOR_TABLE_NAME);

        if (catalog.tableExists(identifier)) {
            return catalog.loadTable(identifier);
        }

        log.info("Creating vector table {}.{}", vectorNamespace, Constants.VECTOR_TABLE_NAME);
        Schema schema = new Schema(
                Types.NestedField.required(1, Constants.VECTOR_ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(2, Constants.SOURCE_TABLE_COLUMN, Types.StringType.get()),
                Types.NestedField.required(3, Constants.SOURCE_ROW_ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(4, Constants.EMBEDDING_COLUMN,
                        Types.ListType.ofRequired(5, Types.DoubleType.get())),
                Types.NestedField.required(6, Constants.TEXT_COLUMN, Types.StringType.get()),
                Types.NestedField.optional(7, Constants.METADATA_COLUMN,
                        Types.MapType.ofOptional(8, 9, Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.required(10, Constants.MODEL_NAME_COLUMN, Types.StringType.get()),
                Types.NestedField.required(11, Constants.CREATED_AT_COLUMN, Types.TimestampType.withZone())
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity(Constants.SOURCE_TABLE_COLUMN)
                .build();

        catalog.createTable(identifier, schema, spec);
        return catalog.loadTable(identifier);
    }

    private Record toIcebergRecord(Schema schema, VectorRecord record) {
        Record icebergRecord = GenericRecord.create(schema);
        icebergRecord.setField("vector_id", record.getVectorId());
        String sourceTable = record.getSourceTable();
        if (sourceTable == null && record.getMetadata() != null) {
            sourceTable = record.getMetadata().get("source_table");
        }
        icebergRecord.setField("source_table", sourceTable);
        icebergRecord.setField("source_row_id", record.getSourceRowId());
        icebergRecord.setField("embedding", record.getEmbedding());
        icebergRecord.setField("text", record.getText());
        icebergRecord.setField("metadata", record.getMetadata());
        icebergRecord.setField("model_name", record.getModelName());
        Instant createdAt = record.getCreatedAt();
        if (createdAt != null) {
            icebergRecord.setField("created_at", OffsetDateTime.ofInstant(createdAt, java.time.ZoneOffset.UTC));
        }
        return icebergRecord;
    }

    private static class PartitionBatch {
        private final PartitionKey partitionKey;
        private final List<Record> records = new ArrayList<>();

        private PartitionBatch(PartitionKey partitionKey) {
            this.partitionKey = partitionKey;
        }
    }
}

// Made with Bob
