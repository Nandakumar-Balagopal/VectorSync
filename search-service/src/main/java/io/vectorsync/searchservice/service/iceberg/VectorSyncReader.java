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
                    log.warn("Skipping unreadable vector row: {}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read vectors from Iceberg: {}", e.getMessage(), e);
        }

        return VectorResolution.latestLiveVectors(records);
    }
}
