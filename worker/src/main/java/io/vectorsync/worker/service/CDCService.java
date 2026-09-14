package io.vectorsync.worker.service;

import io.vectorsync.common.dto.ChangeEvent;
import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.vector.VectorIds;
import io.vectorsync.worker.service.embedding.EmbeddingRequest;
import io.vectorsync.worker.service.embedding.EmbeddingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Slf4j
public class CDCService {

    private final EmbeddingService embeddingService;
    private final VectorStoreService vectorStoreService;

    public CDCService(EmbeddingService embeddingService, VectorStoreService vectorStoreService) {
        this.embeddingService = embeddingService;
        this.vectorStoreService = vectorStoreService;
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

        List<String> knownVersions = null;

        for (ChangeEvent event : changeEvents) {
            try {
                if (ChangeEvent.OPERATION_DELETE.equals(event.getOperation())) {
                    if (knownVersions == null) {
                        // Fetched once per batch, and only when something was actually deleted.
                        knownVersions = vectorStoreService.materializedVersions(tableConfig.getTableName());
                    }
                    vectorRecords.addAll(tombstones(tableConfig, event, knownVersions));
                    continue;
                }

                PendingRecord prepared = prepare(tableConfig, event);
                if (prepared == null) {
                    continue;
                }
                pending.add(prepared);
            } catch (Exception e) {
                failed++;
                log.error("Error preparing change event for table {}: {}",
                        tableConfig.getTableId(), e.getMessage(), e);
            }
        }

        if (!pending.isEmpty()) {
            try {
                List<EmbeddingRequest> requests = pending.stream()
                        .map(entry -> new EmbeddingRequest(
                                entry.record().getVectorId(),
                                entry.record().getSourceTable(),
                                entry.record().getSourceRowId(),
                                entry.record().getEmbeddingModel(),
                                entry.text()))
                        .toList();

                Map<String, List<Double>> embeddings = embeddingService.generateEmbeddings(requests);

                // Staged so a row missing from the response leaves no partial records behind;
                // the batch is all-or-nothing.
                List<VectorRecord> embedded = new ArrayList<>(pending.size());
                for (PendingRecord entry : pending) {
                    List<Double> embedding = embeddings.get(entry.record().getVectorId());
                    if (embedding == null || embedding.isEmpty()) {
                        throw new IllegalStateException("Embedding provider returned nothing for "
                                + entry.record().getVectorId());
                    }
                    embedded.add(finalise(entry.record(), embedding));
                }
                vectorRecords.addAll(embedded);
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
     * Resolves an insert or update into a record awaiting its embedding.
     *
     * @return null when the event yields nothing to store
     */
    private PendingRecord prepare(TableConfig tableConfig, ChangeEvent event) {
        String textContent = extractTextContent(tableConfig, event);
        if (textContent == null || textContent.isBlank()) {
            log.warn("No text content found for event in table {}", tableConfig.getTableName());
            return null;
        }

        VectorRecord record = VectorRecord.builder()
                .sourceTable(tableConfig.getTableName())
                .sourceRowId(extractRowId(event))
                .sourceSnapshotId(event.getSnapshotId())
                .sourceSequenceNumber(event.getSequenceNumber())
                .sourceCommittedAtMillis(event.getCommittedAtMillis())
                .chunkOrdinal(0)
                .embeddingModel(tableConfig.getModelName())
                .embeddingVersion(tableConfig.embeddingVersionOrDefault())
                .preprocessingId(VectorIds.preprocessingId(
                        tableConfig.getEmbeddingColumns(), TableConfig.TEXT_JOIN_SEPARATOR))
                .text(textContent)
                .deleted(false)
                .metadata(extractMetadata(event))
                .createdAt(Instant.now())
                .build();

        // Identity depends only on source lineage, not on the embedding, so it can be derived now
        // and used to correlate the batch response.
        record.setVectorId(VectorIds.vectorId(record));

        return new PendingRecord(record, textContent);
    }

    /**
     * A deleted source row is tombstoned under **every** version materialized for the table, not
     * just the currently configured one.
     *
     * <p>Resolution is keyed by model version, so tombstoning only the current version would leave
     * the row live -- and therefore discoverable -- under every older version. For a table claiming
     * to be an auditable system of record, a deleted row must disappear from all of them.
     */
    private List<VectorRecord> tombstones(TableConfig tableConfig,
                                          ChangeEvent event,
                                          List<String> knownVersions) {
        String sourceRowId = extractRowId(event);

        Set<String> versions = new LinkedHashSet<>(knownVersions);
        // Always include the configured version, so a delete arriving before that version has
        // materialized anything is still recorded.
        versions.add(tableConfig.getModelName() + ":" + tableConfig.embeddingVersionOrDefault());

        List<VectorRecord> result = new ArrayList<>(versions.size());
        for (String modelVersion : versions) {
            int separator = modelVersion.lastIndexOf(':');
            if (separator <= 0) {
                log.warn("Skipping malformed model version '{}' while tombstoning {}",
                        modelVersion, sourceRowId);
                continue;
            }

            VectorRecord tombstone = VectorRecord.builder()
                    .sourceTable(tableConfig.getTableName())
                    .sourceRowId(sourceRowId)
                    .sourceSnapshotId(event.getSnapshotId())
                    .sourceSequenceNumber(event.getSequenceNumber())
                    .sourceCommittedAtMillis(event.getCommittedAtMillis())
                    .chunkOrdinal(0)
                    .embeddingModel(modelVersion.substring(0, separator))
                    .embeddingVersion(modelVersion.substring(separator + 1))
                    .preprocessingId(VectorIds.preprocessingId(
                            tableConfig.getEmbeddingColumns(), TableConfig.TEXT_JOIN_SEPARATOR))
                    .embedding(List.of())
                    .embeddingDim(0)
                    .text(null)
                    .deleted(true)
                    .metadata(extractMetadata(event))
                    .createdAt(Instant.now())
                    .build();
            tombstone.setVectorId(VectorIds.vectorId(tombstone));
            result.add(tombstone);
        }

        return result;
    }

    /** Attaches the embedding. Identity was already derived in {@link #prepare}. */
    private VectorRecord finalise(VectorRecord record, List<Double> embedding) {
        record.setEmbedding(embedding);
        record.setEmbeddingDim(embedding.size());
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
