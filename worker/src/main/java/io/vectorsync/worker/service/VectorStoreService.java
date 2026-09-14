package io.vectorsync.worker.service;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.io.IcebergAppender;
import io.vectorsync.format.vector.VectorRecordCodec;
import io.vectorsync.format.vector.VectorResolution;
import io.vectorsync.worker.service.iceberg.IcebergTableService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Service
@Slf4j
public class VectorStoreService {

    private final IcebergTableService icebergTableService;

    public VectorStoreService(IcebergTableService icebergTableService) {
        this.icebergTableService = icebergTableService;
    }

    /**
     * Appends vectors in a single Iceberg commit.
     *
     * <p>Failures propagate deliberately. This previously logged and returned normally, while the
     * caller went on to advance the sync watermark — so a failed write was recorded as a
     * successful sync and the affected rows were never re-materialized.
     */
    public void writeVectors(List<VectorRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        log.info("Writing {} vector records to store", records.size());

        Table table = icebergTableService.loadOrCreateVectorTable();
        Schema schema = table.schema();

        List<Record> icebergRecords = records.stream()
                .map(record -> VectorRecordCodec.toIcebergRecord(schema, record))
                .toList();

        IcebergAppender.append(table, icebergRecords);
    }

    public List<VectorRecord> getAllVectors() {
        return VectorResolution.latestLiveVectors(readRaw());
    }

    /**
     * The live vector set as it stood at a source sequence number, which is what makes a
     * materialization reproducible rather than merely current.
     */
    public List<VectorRecord> getVectorsAsOf(long sourceSequenceNumber) {
        return VectorResolution.liveVectorsAsOf(readRaw(), sourceSequenceNumber);
    }

    /**
     * Every {@code model:version} ever materialized for a table, including versions whose rows are
     * all tombstoned.
     *
     * <p>Read from full history rather than the live set, because a delete has to tombstone every
     * version of the row and must not skip one merely because that version is already partly
     * deleted.
     */
    public List<String> materializedVersions(String sourceTable) {
        // Runs once per table per sync cycle to fan a delete across every version. Reading the
        // whole table to collect one string per row made the scheduler's cost scale with total
        // warehouse size; the predicate prunes to this table's partitions and the projection drops
        // the embedding, which is all but the entire row.
        Table table = icebergTableService.loadOrCreateVectorTable();
        return scan(table,
                Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable),
                record -> String.valueOf(record.getField(Constants.MODEL_VERSION_COLUMN)),
                Constants.MODEL_VERSION_COLUMN)
                .stream()
                .distinct()
                .sorted()
                .toList();
    }

    public List<VectorRecord> getVectorsByTable(String tableName) {
        Table table = icebergTableService.loadOrCreateVectorTable();
        return VectorResolution.latestLiveVectors(scan(table,
                Expressions.equal(Constants.SOURCE_TABLE_COLUMN, tableName),
                VectorRecordCodec::fromIcebergRecord));
    }

    public long getVectorCount() {
        return getAllVectors().size();
    }

    /** Full append-only history, before resolution. */
    private List<VectorRecord> readRaw() {
        return scan(icebergTableService.loadOrCreateVectorTable(), null,
                VectorRecordCodec::fromIcebergRecord);
    }

    private <T> List<T> scan(Table table,
                             Expression filter,
                             Function<Record, T> mapper,
                             String... columns) {
        IcebergGenerics.ScanBuilder builder = IcebergGenerics.read(table);
        if (filter != null) {
            builder = builder.where(filter);
        }
        if (columns.length > 0) {
            builder = builder.select(columns);
        }

        List<T> records = new ArrayList<>();
        try (CloseableIterable<Record> rows = builder.build()) {
            for (Record row : rows) {
                try {
                    records.add(mapper.apply(row));
                } catch (Exception e) {
                    log.warn("Skipping unreadable vector row: {}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read vectors from Iceberg", e);
        }

        return records;
    }
}
