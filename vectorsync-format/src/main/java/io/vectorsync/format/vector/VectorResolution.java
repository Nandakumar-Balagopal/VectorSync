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
 * <p>This is the most important definition in the format: the vector table is append-only, so
 * every reader must agree on how to collapse its history into a current view.
 *
 * <p>Format v2 resolves by {@code source_snapshot_id} rather than by wall-clock {@code created_at}.
 * Wall clock is metadata about the pipeline run, not about the data version: it is non-deterministic
 * across machines and cannot answer "what was live at source snapshot N?". Snapshot ordering is
 * deterministic, reproducible, and time-travel queryable. {@code created_at} survives only as a
 * tiebreak for two materializations of the same snapshot.
 */
@Slf4j
public final class VectorResolution {

    private VectorResolution() {
    }

    /**
     * Orders oldest-first so that a later record overwrites an earlier one for the same key.
     * Snapshot id is primary; created_at breaks ties within a snapshot.
     */
    private static final Comparator<VectorRecord> OLDEST_FIRST =
            Comparator.comparingLong(VectorRecord::getSourceSnapshotId)
                    .thenComparing(VectorRecord::getCreatedAt,
                            Comparator.nullsFirst(Comparator.naturalOrder()));

    /** Collapses append-only history to the current live set. */
    public static List<VectorRecord> latestLiveVectors(List<VectorRecord> records) {
        return liveVectorsAsOf(records, Long.MAX_VALUE);
    }

    /**
     * Collapses history as it stood at a source snapshot, ignoring anything derived from a later
     * one. This is what makes a stored embedding set reproducible: the same inputs and the same
     * target snapshot always yield the same answer.
     */
    public static List<VectorRecord> liveVectorsAsOf(List<VectorRecord> records, long asOfSourceSnapshotId) {
        Map<String, VectorRecord> latestByKey = new LinkedHashMap<>();

        records.stream()
                .filter(VectorResolution::hasSourceKey)
                .filter(record -> record.getSourceSnapshotId() <= asOfSourceSnapshotId)
                .sorted(OLDEST_FIRST)
                .forEach(record -> latestByKey.put(sourceKey(record), record));

        return latestByKey.values().stream()
                .filter(record -> !record.isDeleted())
                .toList();
    }

    /**
     * Identity of the logical thing a vector represents: a chunk of a source row, scoped by model
     * version so that embeddings from different models coexist rather than superseding each other.
     */
    public static String sourceKey(VectorRecord record) {
        if (isBlank(record.getSourceTable()) || isBlank(record.getSourceRowId())) {
            return null;
        }

        return String.join("::",
                record.getSourceTable(),
                record.getSourceRowId(),
                Integer.toString(record.getChunkOrdinal()),
                record.modelVersion());
    }

    private static boolean hasSourceKey(VectorRecord record) {
        if (sourceKey(record) != null) {
            return true;
        }

        log.warn("Dropping vector row without source key. vectorId={}, sourceTable={}, sourceRowId={}, modelVersion={}",
                record.getVectorId(),
                record.getSourceTable(),
                record.getSourceRowId(),
                record.modelVersion());
        return false;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
