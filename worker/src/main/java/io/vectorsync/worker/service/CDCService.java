package io.vectorsync.worker.service;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.vector.VectorIds;
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

    /**
     * Result of materializing a batch of change events.
     *
     * <p>The failure count matters: the caller must not advance the sync watermark past a snapshot
     * whose events did not all materialize, or those changes are lost permanently. An empty record
     * list with zero failures is a legitimate outcome (for example, every changed row had blank
     * text) and is safe to advance past.
     */
    public record MaterializationResult(List<VectorRecord> records, int failed) {
        public boolean complete() {
            return failed == 0;
        }
    }

    /** A change event resolved to the point where it only needs an embedding. */
    private record PendingRecord(VectorRecord record, String text) {
    }

    /**
     * Materializes change events, embedding all of them in one batch.
     *
     * <p>This previously made one HTTP round trip per changed row while the embedding service's
     * batch endpoint went unused, which dominated sync time for anything but a trivial table.
     */
    public MaterializationResult processChangeEvents(TableConfig tableConfig,
                                                     List<ChangeEvent> changeEvents) {
        List<VectorRecord> vectorRecords = new ArrayList<>();
        List<PendingRecord> pending = new ArrayList<>();
        int failed = 0;

        for (ChangeEvent event : changeEvents) {
            try {
                PendingRecord prepared = prepare(tableConfig, event);
                if (prepared == null) {
                    continue;
                }
                if (prepared.text() == null) {
                    // A tombstone needs no embedding and is final already.
                    vectorRecords.add(finalise(prepared.record(), List.of()));
                } else {
                    pending.add(prepared);
                }
            } catch (Exception e) {
                failed++;
                log.error("Error preparing change event for table {}: {}",
                        tableConfig.getTableId(), e.getMessage(), e);
            }
        }

        if (!pending.isEmpty()) {
            try {
                List<String> texts = pending.stream().map(PendingRecord::text).toList();
                List<List<Double>> embeddings = embeddingService.generateEmbeddings(texts);

                if (embeddings.size() != pending.size()) {
                    throw new IllegalStateException("Embedding provider returned " + embeddings.size()
                            + " embeddings for " + pending.size() + " texts");
                }

                for (int i = 0; i < pending.size(); i++) {
                    vectorRecords.add(finalise(pending.get(i).record(), embeddings.get(i)));
                }
            } catch (Exception e) {
                // The batch is all-or-nothing, so every pending row counts as failed and the
                // watermark holds rather than skipping past changes that were never embedded.
                failed += pending.size();
                log.error("Batch embedding failed for {} rows in table {}: {}",
                        pending.size(), tableConfig.getTableName(), e.getMessage(), e);
            }
        }

        return new MaterializationResult(vectorRecords, failed);
    }

    /**
     * Resolves an event into a record awaiting its embedding.
     *
     * @return null when the event yields nothing to store; a {@link PendingRecord} whose text is
     *         null when it is a tombstone
     */
    private PendingRecord prepare(TableConfig tableConfig, ChangeEvent event) {
        boolean delete = ChangeEvent.OPERATION_DELETE.equals(event.getOperation());
        String textContent = null;

        if (!delete) {
            textContent = extractTextContent(tableConfig, event);
            if (textContent == null || textContent.isBlank()) {
                log.warn("No text content found for event in table {}", tableConfig.getTableName());
                return null;
            }
        }

        VectorRecord record = VectorRecord.builder()
                .sourceTable(tableConfig.getTableName())
                .sourceRowId(extractRowId(event))
                .sourceSnapshotId(event.getSnapshotId())
                .chunkOrdinal(0)
                .embeddingModel(tableConfig.getModelName())
                .embeddingVersion(tableConfig.embeddingVersionOrDefault())
                .preprocessingId(VectorIds.preprocessingId(
                        tableConfig.getEmbeddingColumns(), TableConfig.TEXT_JOIN_SEPARATOR))
                .text(textContent)
                .deleted(delete)
                .metadata(extractMetadata(event))
                .createdAt(Instant.now())
                .build();

        return new PendingRecord(record, textContent);
    }

    /** Attaches the embedding and derives the record's identity from its completed lineage. */
    private VectorRecord finalise(VectorRecord record, List<Double> embedding) {
        record.setEmbedding(embedding);
        record.setEmbeddingDim(embedding.size());
        record.setVectorId(VectorIds.vectorId(record));
        return record;
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
