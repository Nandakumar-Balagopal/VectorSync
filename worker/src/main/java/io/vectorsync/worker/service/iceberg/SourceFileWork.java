package io.vectorsync.worker.service.iceberg;

import lombok.Builder;

/**
 * One data file of a source table, to be read and embedded as a single unit of work.
 *
 * <p>A data file is the smallest thing Iceberg will hand out without opening any data, and it
 * arrives already carrying its own provenance. That makes it the natural unit here: work can be
 * distributed, retried, or resumed one file at a time, and a failure costs one file rather than a
 * whole table scan. {@link #recordCount()} and {@link #fileSizeInBytes()} come from table metadata,
 * so a scheduler can size batches before a single byte of Parquet is read.
 *
 * <p>{@link #snapshotId()}, {@link #sequenceNumber()} and {@link #committedAtMillis()} are the
 * source version that the rows in this file belong to, and they are stamped onto every content map
 * entry derived from it. Ordering of those entries must use {@link #sequenceNumber()}: Iceberg
 * snapshot ids are random longs, so comparing them numerically shuffles history. The snapshot id is
 * kept for identity and for pinning a re-read of this exact file, never for ordering.
 *
 * <p>{@link #recordCount()} is an upper bound rather than an exact count when delete files apply to
 * the file: the metadata counts rows written, and position or equality deletes are only resolved
 * when the file is actually read.
 *
 * <p>{@code fromBackfillAnchor} distinguishes a file enumerated from a pinned backfill anchor from
 * one produced by an incremental append scan. The distinction is not cosmetic: a backfill pass saw
 * the state of the table at its anchor and is therefore entitled to treat an absent chunk as a
 * deletion, while an incremental pass saw only what was appended and is not.
 *
 * <p>That entitlement is bounded by what the pass actually enumerated. A reconcile narrowed to a
 * set of partitions produces backfill-anchored work too, yet it is complete only inside those
 * partitions, and the work item does not carry the set. So a sweep that tombstones chunks absent
 * from a pass must be scoped by the caller to the same partitions it asked for; run against the
 * whole table it would tombstone every row the reconcile never looked at.
 */
@Builder
public record SourceFileWork(
        String sourceTable,
        String dataFilePath,
        long recordCount,
        long fileSizeInBytes,
        long snapshotId,
        long sequenceNumber,
        long committedAtMillis,
        boolean fromBackfillAnchor) {

    public SourceFileWork {
        if (sourceTable == null || sourceTable.isBlank()) {
            throw new IllegalArgumentException("Source file work without a source table has no scope");
        }
        if (dataFilePath == null || dataFilePath.isBlank()) {
            throw new IllegalArgumentException(
                    "Source file work for " + sourceTable + " has no data file path");
        }
    }

    /** True when this file came from an incremental append scan rather than a backfill anchor. */
    public boolean fromIncrementalAppend() {
        return !fromBackfillAnchor;
    }
}
