package io.vectorsync.format.vector;

import io.vectorsync.common.dto.VectorRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The rule that decides which stored vector is live for a given source row.
 *
 * <p>This is the single most important definition in the format: the vector table is append-only,
 * so every reader must agree on how to collapse its history into a current view. Previously this
 * logic was copy-pasted into both {@code VectorStoreService} and {@code VectorSyncReader}, which
 * meant two components could disagree about what the format means.
 *
 * <p>TODO(format-v2): resolution currently orders by wall-clock {@code created_at}, which is
 * metadata about the pipeline run rather than about the data version. It is non-deterministic
 * across machines and cannot answer "what was live at source snapshot N?". Replace with explicit
 * ordering on {@code (source_snapshot_id, embedding_version)} once those become typed columns.
 */
@Slf4j
public final class VectorResolution {

    private static final String METADATA_SOURCE_TABLE = "source_table";
    private static final String METADATA_SOURCE_ROW_ID = "source_row_id";
    private static final String METADATA_ID = "id";

    private VectorResolution() {
    }

    /**
     * Collapses append-only history to the newest non-deleted vector per source row and model.
     */
    public static List<VectorRecord> latestLiveVectors(List<VectorRecord> records) {
        Map<String, VectorRecord> latestBySourceRow = new LinkedHashMap<>();

        records.stream()
                .filter(VectorResolution::hasSourceKey)
                .sorted(Comparator.comparing(
                        VectorRecord::getCreatedAt,
                        Comparator.nullsFirst(Comparator.naturalOrder())
                ))
                .forEach(record -> latestBySourceRow.put(sourceKey(record), record));

        return latestBySourceRow.values().stream()
                .filter(record -> !record.isDeleted())
                .toList();
    }

    private static boolean hasSourceKey(VectorRecord record) {
        if (sourceKey(record) != null) {
            return true;
        }

        log.warn("Dropping vector row without source key. vectorId={}, sourceTable={}, sourceRowId={}, modelName={}, metadataKeys={}",
                record.getVectorId(),
                record.getSourceTable(),
                record.getSourceRowId(),
                record.getModelName(),
                record.getMetadata() == null ? List.of() : record.getMetadata().keySet());
        return false;
    }

    /**
     * Identity of the source row a vector represents, scoped by model so that embeddings from
     * different models coexist rather than superseding one another.
     */
    public static String sourceKey(VectorRecord record) {
        String sourceTable = record.getSourceTable();
        String sourceRowId = record.getSourceRowId();

        if (record.getMetadata() != null) {
            if (sourceTable == null || sourceTable.isBlank()) {
                sourceTable = record.getMetadata().get(METADATA_SOURCE_TABLE);
            }
            sourceRowId = firstNonBlank(sourceRowId, record.getMetadata().get(METADATA_SOURCE_ROW_ID));
            sourceRowId = firstNonBlank(sourceRowId, record.getMetadata().get(METADATA_ID));
        }

        if (sourceTable == null || sourceTable.isBlank() || sourceRowId == null || sourceRowId.isBlank()) {
            return null;
        }

        return sourceTable + "::" + sourceRowId + "::" + record.getModelName();
    }

    private static String firstNonBlank(String primary, String fallback) {
        return primary == null || primary.isBlank() ? fallback : primary;
    }
}
