package io.vectorsync.searchservice.service.iceberg;

import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.vector.VectorRecordCodec;
import io.vectorsync.format.vector.VectorResolution;
import io.vectorsync.format.vector.VectorTableSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class VectorSyncReader {

    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public VectorSyncReader(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public List<VectorRecord> readAllVectors() {
        return VectorResolution.latestLiveVectors(readRaw());
    }

    /**
     * The live vectors an index should cover: one source table and one model version.
     *
     * <p>Scoping an index this way removes the need to filter search results by source table,
     * which the previous implementation did after top-k and so could under-return.
     */
    public List<VectorRecord> readForIndex(String sourceTable, String modelVersion) {
        return readAllVectors().stream()
                .filter(vector -> sourceTable.equals(vector.getSourceTable()))
                .filter(vector -> modelVersion.equals(vector.modelVersion()))
                .filter(vector -> vector.getEmbedding() != null && !vector.getEmbedding().isEmpty())
                .toList();
    }

    /** Live vectors as they stood at a source sequence number, for a reproducible rebuild. */
    public List<VectorRecord> readForIndexAsOf(String sourceTable, String modelVersion, long sourceSequenceNumber) {
        return VectorResolution.liveVectorsAsOf(readRaw(), sourceSequenceNumber).stream()
                .filter(vector -> sourceTable.equals(vector.getSourceTable()))
                .filter(vector -> modelVersion.equals(vector.modelVersion()))
                .filter(vector -> vector.getEmbedding() != null && !vector.getEmbedding().isEmpty())
                .toList();
    }

    /** Highest source snapshot observed for a table, i.e. how current its embeddings are. */
    public long latestSourceSnapshot(String sourceTable) {
        return readRaw().stream()
                .filter(vector -> sourceTable.equals(vector.getSourceTable()))
                .max(java.util.Comparator.comparingLong(VectorRecord::getSourceSequenceNumber))
                .map(VectorRecord::getSourceSnapshotId)
                .orElse(0L);
    }

    /** Highest source sequence number observed for a table. Orders history; snapshot ids do not. */
    public long latestSourceSequenceNumber(String sourceTable) {
        return readRaw().stream()
                .filter(vector -> sourceTable.equals(vector.getSourceTable()))
                .mapToLong(VectorRecord::getSourceSequenceNumber)
                .max()
                .orElse(0L);
    }

    /** Distinct model versions materialized for a table. */
    public List<String> modelVersionsFor(String sourceTable) {
        return readAllVectors().stream()
                .filter(vector -> sourceTable.equals(vector.getSourceTable()))
                .map(VectorRecord::modelVersion)
                .distinct()
                .sorted()
                .toList();
    }

    /** Full append-only history, before resolution. */
    public List<VectorRecord> readRaw() {
        Table table = VectorTableSchema.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (table == null) {
            return List.of();
        }

        List<VectorRecord> records = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                try {
                    records.add(VectorRecordCodec.fromIcebergRecord(row));
                } catch (Exception e) {
                    log.warn("Skipping unreadable vector row: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read vectors from Iceberg", e);
        }

        return records;
    }
}
