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
 * <p>Resolution orders by Iceberg's snapshot <em>sequence number</em>, which the spec guarantees
 * increases monotonically per table. It deliberately does not order by {@code source_snapshot_id}:
 * Iceberg snapshot ids are random longs, so comparing them numerically silently reorders history
 * and a later tombstone can lose to the row it was meant to delete. Nor does it order by
 * wall-clock {@code created_at}, which is metadata about the pipeline run rather than the data
 * version, is non-deterministic across machines, and cannot answer "what was live at source
 * version N?".
 *
 * <p>Snapshot commit time and then {@code created_at} break ties, which matter only for two
 * materializations of the same source version.
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
            Comparator.comparingLong(VectorRecord::getSourceSequenceNumber)
                    .thenComparingLong(VectorRecord::getSourceCommittedAtMillis)
                    .thenComparing(VectorRecord::getCreatedAt,
                            Comparator.nullsFirst(Comparator.naturalOrder()));

    /** Collapses append-only history to the current live set. */
    public static List<VectorRecord> latestLiveVectors(List<VectorRecord> records) {
        return liveVectorsAsOf(records, Long.MAX_VALUE);
    }

    /**
     * Collapses history as it stood at a source sequence number, ignoring anything derived from a
     * later one. This is what makes a stored embedding set reproducible: the same inputs and the
     * same target version always yield the same answer.
     *
     * <p>Takes a sequence number, not a snapshot id, because only sequence numbers are ordered.
     */
    public static List<VectorRecord> liveVectorsAsOf(List<VectorRecord> records,
                                                     long asOfSourceSequenceNumber) {
        Map<String, VectorRecord> latestByKey = new LinkedHashMap<>();

        records.stream()
                .filter(VectorResolution::hasSourceKey)
                .filter(record -> record.getSourceSequenceNumber() <= asOfSourceSequenceNumber)
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
