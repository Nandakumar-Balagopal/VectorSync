package io.vectorsync.worker.service;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.worker.service.embedding.EmbeddingException;
import io.vectorsync.worker.service.embedding.EmbeddingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.*;

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
        Map<String, String> metadata = extractMetadata(event);
        metadata.put("source_table", tableConfig.getTableName());
        metadata.put("source_row_id", sourceRowId);
        metadata.put("id", sourceRowId);
        metadata.put("operation", event.getOperation());

        if (ChangeEvent.OPERATION_DELETE.equals(event.getOperation())) {
            return VectorRecord.builder()
                    .vectorId(UUID.randomUUID().toString())
                    .sourceTable(tableConfig.getTableName())
                    .sourceRowId(sourceRowId)
                    .embedding(Collections.emptyList())
                    .text("")
                    .metadata(metadata)
                    .modelName(tableConfig.getModelName())
                    .createdAt(Instant.now())
                    .deleted(true)
                    .build();
        }

        String textContent = extractTextContent(tableConfig, event);
        if (textContent == null || textContent.isBlank()) {
            log.warn("No text content found for event in table {}", tableConfig.getTableName());
            return null;
        }

        List<Double> embedding = embeddingService.generateEmbedding(textContent);

        return VectorRecord.builder()
                .vectorId(UUID.randomUUID().toString())
                .sourceTable(tableConfig.getTableName())
                .sourceRowId(sourceRowId)
                .embedding(embedding)
                .text(textContent)
                .metadata(metadata)
                .modelName(tableConfig.getModelName())
                .createdAt(Instant.now())
                .deleted(false)
                .build();
    }

    private String extractTextContent(TableConfig tableConfig, ChangeEvent event) {
        List<String> embeddingColumns = tableConfig.getEmbeddingColumns();
        StringBuilder sb = new StringBuilder();

        for (String column : embeddingColumns) {
            Object value = event.getRowData().get(column);
            if (value != null) {
                if (sb.length() > 0) {
                    sb.append(" | ");
                }
                sb.append(value.toString());
            }
        }

        return sb.toString().isBlank() ? null : sb.toString();
    }

    private String extractRowId(ChangeEvent event) {
        Object rowId = event.getRowData().get("id");
        if (rowId != null) {
            return rowId.toString();
        }
        return UUID.randomUUID().toString();
    }

    private Map<String, String> extractMetadata(ChangeEvent event) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("snapshot_id", String.valueOf(event.getSnapshotId()));
        metadata.put("previous_snapshot_id", String.valueOf(event.getPreviousSnapshotId()));
        metadata.put("detected_at", event.getDetectedAt().toString());
        return metadata;
    }
}
