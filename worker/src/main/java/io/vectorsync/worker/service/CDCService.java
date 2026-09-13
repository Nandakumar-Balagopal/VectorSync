package io.vectorsync.worker.service;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.vector.VectorIds;
import io.vectorsync.worker.service.embedding.EmbeddingException;
import io.vectorsync.worker.service.embedding.EmbeddingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class CDCService {

    private final EmbeddingService embeddingService;

    public CDCService(EmbeddingService embeddingService) {
        this.embeddingService = embeddingService;
    }

    public List<VectorRecord> processChangeEvents(TableConfig tableConfig,
                                                    List<ChangeEvent> changeEvents) {
        List<VectorRecord> vectorRecords = new ArrayList<>();

        for (ChangeEvent event : changeEvents) {
            try {
                VectorRecord record = processChangeEvent(tableConfig, event);
                if (record != null) {
                    vectorRecords.add(record);
                }
            } catch (Exception e) {
                log.error("Error processing change event for table {}: {}", tableConfig.getTableId(), e.getMessage(), e);
            }
        }

        return vectorRecords;
    }

    private VectorRecord processChangeEvent(TableConfig tableConfig, ChangeEvent event)
            throws EmbeddingException {
        log.debug("Processing change event for table: {}", tableConfig.getTableName());

        String sourceRowId = extractRowId(event);
        String preprocessingId = preprocessingId(tableConfig);
        boolean delete = ChangeEvent.OPERATION_DELETE.equals(event.getOperation());

        String textContent = null;
        List<Double> embedding = List.of();

        if (!delete) {
            textContent = extractTextContent(tableConfig, event);
            if (textContent == null || textContent.isBlank()) {
                log.warn("No text content found for event in table {}", tableConfig.getTableName());
                return null;
            }
            embedding = embeddingService.generateEmbedding(textContent);
        }

        VectorRecord record = VectorRecord.builder()
                .sourceTable(tableConfig.getTableName())
                .sourceRowId(sourceRowId)
                .sourceSnapshotId(event.getSnapshotId())
                .chunkOrdinal(0)
                .embeddingModel(tableConfig.getModelName())
                .embeddingVersion(tableConfig.embeddingVersionOrDefault())
                .embeddingDim(embedding.size())
                .preprocessingId(preprocessingId)
                .embedding(embedding)
                .text(textContent)
                .deleted(delete)
                .metadata(extractMetadata(event))
                .createdAt(Instant.now())
                .build();

        // Identity is derived from lineage, so re-materializing a snapshot is idempotent.
        record.setVectorId(VectorIds.vectorId(record));
        return record;
    }

    private String preprocessingId(TableConfig tableConfig) {
        return VectorIds.preprocessingId(tableConfig.getEmbeddingColumns(), TableConfig.TEXT_JOIN_SEPARATOR);
    }

    private String extractTextContent(TableConfig tableConfig, ChangeEvent event) {
        List<String> embeddingColumns = tableConfig.getEmbeddingColumns();
        StringBuilder sb = new StringBuilder();

        for (String column : embeddingColumns) {
            Object value = event.getRowData().get(column);
            if (value != null) {
                if (sb.length() > 0) {
                    sb.append(TableConfig.TEXT_JOIN_SEPARATOR);
                }
                sb.append(value);
            }
        }

        return sb.toString().isBlank() ? null : sb.toString();
    }

    private String extractRowId(ChangeEvent event) {
        Object rowId = event.getRowData().get("id");
        if (rowId == null) {
            rowId = event.getRowData().get("ID");
        }
        if (rowId == null) {
            throw new IllegalArgumentException(
                    "Source rows must contain an id column; cannot derive a stable vector identity without one");
        }
        return rowId.toString();
    }

    /**
     * Only non-lineage extras belong here. Snapshot, model, version, and row identity are typed
     * columns in format v2 so they can be partitioned and range-pruned.
     */
    private Map<String, String> extractMetadata(ChangeEvent event) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("operation", event.getOperation());
        if (event.getDetectedAt() != null) {
            metadata.put("detected_at", event.getDetectedAt().toString());
        }
        return metadata;
    }
}
