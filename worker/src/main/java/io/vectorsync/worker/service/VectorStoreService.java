package io.vectorsync.worker.service;

import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.vector.VectorRecordCodec;
import io.vectorsync.format.vector.VectorResolution;
import io.vectorsync.worker.service.iceberg.IcebergTableService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class VectorStoreService {

    private final IcebergTableService icebergTableService;

    public VectorStoreService(IcebergTableService icebergTableService) {
        this.icebergTableService = icebergTableService;
    }

    public void writeVectors(List<VectorRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        log.info("Writing {} vector records to store", records.size());
        Table table;
        try {
            table = icebergTableService.loadOrCreateVectorTable();
        } catch (Exception e) {
            log.warn("Vector table unavailable, skipping write: {}", e.getMessage());
            return;
        }
        Schema schema = table.schema();
        PartitionSpec spec = table.spec();
        OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .format(FileFormat.PARQUET)
                .build();

        Map<String, PartitionBatch> batches = new HashMap<>();
        for (VectorRecord record : records) {
            Record icebergRecord = VectorRecordCodec.toIcebergRecord(schema, record);
            PartitionKey partitionKey = new PartitionKey(spec, schema);
            partitionKey.partition(icebergRecord);
            String partitionPath = partitionKey.toPath();

            batches.computeIfAbsent(partitionPath, key -> new PartitionBatch(partitionKey))
                    .records.add(icebergRecord);
        }

        AppendFiles append = table.newAppend();
        for (PartitionBatch batch : batches.values()) {
            EncryptedOutputFile encryptedOutputFile = outputFileFactory.newOutputFile(batch.partitionKey);
            OutputFile outputFile = encryptedOutputFile.encryptingOutputFile();
            long recordCount = batch.records.size();
            long fileSize;
            org.apache.iceberg.Metrics metrics;

            try {
                FileAppender<Record> appender = Parquet.write(outputFile)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build();

                try (appender) {
                    for (Record icebergRecord : batch.records) {
                        appender.add(icebergRecord);
                    }
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
    }

    public List<VectorRecord> getAllVectors() {
        Table table;
        try {
            table = icebergTableService.loadOrCreateVectorTable();
        } catch (Exception e) {
            log.warn("Vector table unavailable: {}", e.getMessage());
            return List.of();
        }
        List<VectorRecord> records = new ArrayList<>();

        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                try {
                    records.add(VectorRecordCodec.fromIcebergRecord(row));
                } catch (Exception e) {
                    log.warn("Skipping unreadable vector row: {}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read vectors from Iceberg: {}", e.getMessage(), e);
        }

        return VectorResolution.latestLiveVectors(records);
    }

    public List<VectorRecord> getVectorsByTable(String tableName) {
        return getAllVectors().stream()
                .filter(v -> v.getSourceTable().equals(tableName))
                .toList();
    }

    public long getVectorCount() {
        return getAllVectors().size();
    }

    private static class PartitionBatch {
        private final PartitionKey partitionKey;
        private final List<Record> records = new ArrayList<>();

        private PartitionBatch(PartitionKey partitionKey) {
            this.partitionKey = partitionKey;
        }
    }
}
