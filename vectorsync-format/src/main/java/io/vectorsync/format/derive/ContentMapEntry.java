package io.vectorsync.format.derive;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * One assertion about a source row: at source version {@link #sourceSequenceNumber}, this chunk of
 * this row held this content.
 *
 * <p>This is the other half of the content-keyed split. The vector lives in the embedding store
 * keyed by content; this table says which rows point at which content, and when that pointer
 * changed. Because the pointer is the only thing that moves when a row is edited, an update whose
 * embedded columns did not actually change writes one small row here and performs no inference at
 * all.
 *
 * <p>Append-only. An edit appends a new entry at a higher source sequence number and a delete
 * appends one with {@link #deleted} set; nothing is ever rewritten, so the table can answer "what
 * was live at source version N?" and not merely "what is live now".
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ContentMapEntry {

    private String sourceTable;

    /** Row identity derived from the spec's key columns. */
    private String sourceRowId;

    private int chunkOrdinal;

    /**
     * Content this chunk resolved to, or null on a tombstone -- a deleted row has no content. This
     * is the foreign key into the embedding store, under {@link #modelVersion} and
     * {@link #configId}.
     */
    private String contentHash;

    /** {@link MaterializationSpec#configId()}. Also the partition value. */
    private String configId;

    /** {@code model:version}. Redundant with {@link #configId}, which hashes it, but kept so a
     * reader can route to the right embedding-store partition without resolving the spec. */
    private String modelVersion;

    /**
     * Source snapshot this assertion was derived from. Identity only -- Iceberg snapshot ids are
     * random longs and must never be compared to order two entries.
     */
    private long sourceSnapshotId;

    /**
     * Iceberg's monotonic sequence number for {@link #sourceSnapshotId}. This is what orders
     * history, and the only field that may be used to decide which of two entries for the same
     * chunk is newer.
     */
    private long sourceSequenceNumber;

    /** Commit time of the source snapshot. Breaks ties between two reads of the same version. */
    private long sourceCommittedAtMillis;

    /** True when the row or chunk no longer exists in the source. */
    private boolean deleted;

    /** When this entry was written. Last-resort tiebreak; never the primary ordering. */
    private Instant createdAt;

    /**
     * Identity of the logical thing this entry asserts about.
     *
     * <p>Scoped by configuration rather than by model version, because {@code configId} already
     * hashes the model -- adding the model version would make the key wider without making it
     * finer, and two entries under the same config always carry the same model version.
     */
    public String chunkKey() {
        return String.join("::",
                sourceTable == null ? "" : sourceTable,
                sourceRowId == null ? "" : sourceRowId,
                Integer.toString(chunkOrdinal),
                configId == null ? "" : configId);
    }

    /** A live entry must point at content; a tombstone deliberately does not. */
    public boolean isLive() {
        return !deleted && contentHash != null && !contentHash.isBlank();
    }
}
