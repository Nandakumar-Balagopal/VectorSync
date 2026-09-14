package io.vectorsync.searchservice.service;

import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.index.IndexAliasEntry;
import io.vectorsync.format.index.IndexManifestEntry;
import io.vectorsync.searchservice.service.iceberg.IcebergCatalogService;
import io.vectorsync.searchservice.service.iceberg.VectorSyncReader;
import io.vectorsync.searchservice.service.index.IndexRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Answers "why did this result exist?".
 *
 * <p>Walks a returned vector back through the index that served it, the model and version that
 * produced it, the stored embedding, and the exact source snapshot — then reads the source row as
 * it was at that snapshot via Iceberg time travel. This is the property a vector database cannot
 * offer, and it is the reason the vector table is worth treating as a system of record.
 */
@Service
@Slf4j
public class ProvenanceService {

    private final VectorSyncReader vectorSyncReader;
    private final IndexRegistry registry;
    private final IcebergCatalogService catalogService;

    public ProvenanceService(VectorSyncReader vectorSyncReader,
                             IndexRegistry registry,
                             IcebergCatalogService catalogService) {
        this.vectorSyncReader = vectorSyncReader;
        this.registry = registry;
        this.catalogService = catalogService;
    }

    /**
     * @param vectorId identity of a vector returned by a search
     * @return the full lineage chain, including the source row at the snapshot it was derived from
     */
    public Map<String, Object> explain(String vectorId) {
        VectorRecord vector = vectorSyncReader.readRawForVectorId(vectorId).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown vector: " + vectorId));

        Map<String, Object> chain = new LinkedHashMap<>();
        chain.put("vector", vectorSummary(vector));

        Optional<IndexAliasEntry> alias = registry.promotedAlias(vector.getSourceTable());
        Optional<IndexManifestEntry> servingIndex = alias
                .flatMap(entry -> registry.manifest().findById(entry.getIndexId()));

        chain.put("servedByAlias", alias.map(entry -> Map.of(
                "aliasName", entry.getAliasName(),
                "indexId", entry.getIndexId(),
                "promotedAt", String.valueOf(entry.getUpdatedAt()),
                "promotedBy", String.valueOf(entry.getUpdatedBy()),
                "note", String.valueOf(entry.getNote()))).orElse(null));

        chain.put("index", servingIndex.map(this::indexSummary).orElse(null));

        chain.put("embedding", Map.of(
                "model", String.valueOf(vector.getEmbeddingModel()),
                "version", String.valueOf(vector.getEmbeddingVersion()),
                "dimension", vector.getEmbeddingDim(),
                "preprocessingId", String.valueOf(vector.getPreprocessingId())));

        chain.put("source", Map.of(
                "table", String.valueOf(vector.getSourceTable()),
                "rowId", String.valueOf(vector.getSourceRowId()),
                "snapshotId", vector.getSourceSnapshotId(),
                "chunkOrdinal", vector.getChunkOrdinal()));

        chain.put("sourceRowAtSnapshot",
                readSourceRow(vector.getSourceTable(), vector.getSourceRowId(), vector.getSourceSnapshotId()));

        return chain;
    }

    /** Every stored version of one source row, which is what makes a change history auditable. */
    public List<Map<String, Object>> history(String sourceTable, String sourceRowId) {
        return vectorSyncReader.readRawForTable(sourceTable).stream()
                .filter(record -> sourceRowId.equals(record.getSourceRowId()))
                // By sequence number, not snapshot id. Iceberg snapshot ids are random longs, so
                // ordering a change history by them shuffles it.
                .sorted(Comparator.comparingLong(VectorRecord::getSourceSequenceNumber)
                        .thenComparingLong(VectorRecord::getSourceCommittedAtMillis))
                .map(this::vectorSummary)
                .toList();
    }

    private Map<String, Object> vectorSummary(VectorRecord vector) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("vectorId", vector.getVectorId());
        summary.put("modelVersion", vector.modelVersion());
        summary.put("sourceSnapshotId", vector.getSourceSnapshotId());
        summary.put("deleted", vector.isDeleted());
        summary.put("text", vector.getText());
        summary.put("createdAt", String.valueOf(vector.getCreatedAt()));
        summary.put("metadata", vector.getMetadata() == null ? Map.of() : vector.getMetadata());
        return summary;
    }

    private Map<String, Object> indexSummary(IndexManifestEntry entry) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("indexId", entry.getIndexId());
        summary.put("algorithm", entry.getIndexAlgorithm());
        summary.put("params", entry.getIndexParams());
        summary.put("metric", entry.getSimilarityMetric());
        summary.put("dimension", entry.getDimension());
        summary.put("builtFromSnapshotId", entry.getSourceSnapshotId());
        summary.put("vectorCount", entry.getVectorCount());
        summary.put("status", String.valueOf(entry.getStatus()));
        summary.put("evalMetrics", entry.getEvalMetrics());
        summary.put("artifactUri", entry.getIndexUri());
        return summary;
    }

    /**
     * Reads the source row via Iceberg time travel at the snapshot the embedding was derived from,
     * so the answer reflects the data as it actually was rather than as it is now.
     */
    private Object readSourceRow(String sourceTable, String sourceRowId, long snapshotId) {
        if (sourceTable == null || sourceRowId == null || snapshotId <= 0) {
            return null;
        }

        try {
            Table table = catalogService.getCatalog().loadTable(toIdentifier(sourceTable));

            try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                    .useSnapshot(snapshotId)
                    .build()) {
                for (Record row : rows) {
                    Object id = row.getField("id");
                    if (id != null && sourceRowId.equals(id.toString())) {
                        Map<String, Object> values = new HashMap<>();
                        for (int i = 0; i < row.size(); i++) {
                            values.put(table.schema().columns().get(i).name(), String.valueOf(row.get(i)));
                        }
                        return values;
                    }
                }
            }

            return Map.of("note", "row not present at snapshot " + snapshotId);
        } catch (Exception e) {
            log.warn("Could not read source row {} of {} at snapshot {}: {}",
                    sourceRowId, sourceTable, snapshotId, e.getMessage());
            return Map.of("error", String.valueOf(e.getMessage()));
        }
    }

    private TableIdentifier toIdentifier(String tableName) {
        if (tableName.contains(".")) {
            return TableIdentifier.parse(tableName);
        }
        return TableIdentifier.of(Namespace.of("default"), tableName);
    }
}
