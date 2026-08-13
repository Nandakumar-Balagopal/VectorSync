package io.vectorsync.embeddingworker.consumer;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.embeddingworker.provider.EmbeddingException;
import io.vectorsync.embeddingworker.provider.EmbeddingProvider;
import io.vectorsync.embeddingworker.writer.VectorWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Processes CDC events by generating embeddings and writing vectors.
 * 
 * TODO: Replace in-memory processing with Kafka consumer for event-driven architecture.
 * This will enable:
 * - Horizontal scaling of embedding workers
 * - Better fault tolerance and retry mechanisms
 * - Decoupling from CDC workers
 */
@Service
@Slf4j
public class EmbeddingProcessor {

    private final EmbeddingProvider embeddingProvider;
    private final VectorWriter vectorWriter;

    public EmbeddingProcessor(EmbeddingProvider embeddingProvider, VectorWriter vectorWriter) {
        this.embeddingProvider = embeddingProvider;
        this.vectorWriter = vectorWriter;
    }

    /**
     * Process a batch of CDC events and generate embeddings.
     * 
     * @param tableConfig Configuration for the source table
     * @param changeEvents List of change events to process
     * @return List of generated vector records
     */
    public List<VectorRecord> processChangeEvents(TableConfig tableConfig, List<ChangeEvent> changeEvents) {
        List<VectorRecord> vectorRecords = new ArrayList<>();

        for (ChangeEvent event : changeEvents) {
            try {
                VectorRecord record = processChangeEvent(tableConfig, event);
                if (record != null) {
                    vectorRecords.add(record);
                }
            } catch (Exception e) {
                log.error("Error processing change event for table {}: {}", 
                         tableConfig.getTableId(), e.getMessage(), e);
            }
        }

        // Write vectors in batch
        if (!vectorRecords.isEmpty()) {
            try {
                vectorWriter.writeVectors(vectorRecords);
                log.info("Successfully processed {} events for table {}", 
                        vectorRecords.size(), tableConfig.getTableName());
            } catch (Exception e) {
                log.error("Failed to write vectors for table {}: {}", 
                         tableConfig.getTableName(), e.getMessage(), e);
            }
        }

        return vectorRecords;
    }

    private VectorRecord processChangeEvent(TableConfig tableConfig, ChangeEvent event)
            throws EmbeddingException {
        log.debug("Processing change event for table: {}", tableConfig.getTableName());

        String textContent = extractTextContent(tableConfig, event);
        if (textContent == null || textContent.isBlank()) {
            log.warn("No text content found for event in table {}", tableConfig.getTableName());
            return null;
        }

        List<Double> embedding = embeddingProvider.generateEmbedding(textContent);

        String sourceRowId = extractRowId(event);

        Map<String, String> metadata = extractMetadata(event);
        metadata.put("source_table", tableConfig.getTableName());

        return VectorRecord.builder()
                .vectorId(UUID.randomUUID().toString())
                .sourceTable(tableConfig.getTableName())
                .sourceRowId(sourceRowId)
                .embedding(embedding)
                .text(textContent)
                .metadata(metadata)
                .modelName(tableConfig.getModelName())
                .createdAt(Instant.now())
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
        metadata.put("detected_at", event.getDetectedAt().toString());
        return metadata;
    }
}

// Made with Bob
