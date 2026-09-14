package io.vectorsync.controlplane.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * One durable, leasable unit of materialization work: a single source data file that has to be
 * read, chunked, hashed and embedded.
 *
 * <p>The unit is a data file rather than a table or a snapshot because that is the granularity at
 * which progress can survive a crash. With one {@code lastSnapshotId} per table there is no state
 * between "not started" and "finished", so a worker that dies 90% of the way through a large
 * backfill repeats all of it, and a single unreadable file blocks that table forever. Here each
 * file carries its own state and attempt count, so a crash loses at most the in-flight leases and
 * a permanently bad file goes terminal {@link State#FAILED} while its siblings drain.
 *
 * <h2>Lifecycle</h2>
 * <pre>
 *   PENDING --lease--&gt; LEASED --complete--&gt; DONE
 *                        |  fail / lease expiry, attempts &lt; maxAttempts --&gt; PENDING
 *                        |  fail / lease expiry, attempts &gt;= maxAttempts --&gt; FAILED
 * </pre>
 * A lease is a timestamp, not a lock: nothing has to be released. A worker that is killed,
 * partitioned or GC-paused past {@code leaseExpiresAt} simply loses the item to the reclaimer,
 * which is what makes the queue resumable rather than merely durable.
 *
 * <h2>Schema</h2>
 * This project has no Flyway or Liquibase; the schema is whatever Hibernate derives from these
 * annotations under {@code spring.jpa.hibernate.ddl-auto=update} (see
 * {@code control-plane/src/main/resources/application.yml}). Two consequences:
 * <ul>
 *   <li>The indexes and the unique constraint declared below <em>are</em> the schema definition.
 *       Removing one here removes it from every new deployment.</li>
 *   <li>{@code update} only adds; it never drops or retypes a column. Renaming a field or
 *       narrowing a type therefore needs a hand-written migration, and leaves the old column
 *       behind until someone runs it.</li>
 * </ul>
 * A partial index ({@code WHERE state = 'PENDING'}) would serve the dispatch scan with a smaller
 * tree, but JPA cannot express one; add it by hand if the DONE tail ever dwarfs the live queue.
 */
@Entity
@Table(
        name = "work_items",
        indexes = {
                // Exactly the shape of the dispatch scan in WorkQueueService.lease: equality on
                // state, then the ORDER BY. Both order-by columns are ASC in the same order as the
                // query, so Postgres gets the rows pre-sorted instead of sorting the whole backlog.
                @Index(name = "idx_work_items_dispatch", columnList = "state, priority, created_at"),
                @Index(name = "idx_work_items_progress", columnList = "materialization_id, state")
        },
        // Named so ON CONFLICT column inference in WorkItemRepository.insertIfAbsent has an index
        // to resolve against; without a unique index that statement fails at runtime.
        uniqueConstraints = @UniqueConstraint(name = "uq_work_items_dedup", columnNames = "dedup_key")
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WorkItemEntity {

    /** Retry budget for an item whose enqueuer did not choose one. */
    public static final int DEFAULT_MAX_ATTEMPTS = 3;

    @Id
    @Column(name = "id", length = 36)
    private String id;

    /**
     * SHA-256 of {@code (materializationId, dataFilePath, snapshotId)}, the identity under which
     * enqueue is idempotent. Stored as its own column because the natural triple contains a file
     * path of unbounded length, and a unique btree index over a 1 KiB text column is both large
     * and, past ~2700 bytes, outright rejected by Postgres.
     */
    @Column(name = "dedup_key", nullable = false, length = 64)
    private String dedupKey;

    /**
     * The (table, model, config) combination this file feeds. Distinct from the source table: a
     * model migration materializes the same table twice under two materialization ids, and their
     * queues must drain and report progress independently.
     */
    @Column(name = "materialization_id", nullable = false, length = 128)
    private String materializationId;

    @Column(name = "source_table", nullable = false, length = 512)
    private String sourceTable;

    @Column(name = "data_file_path", nullable = false, length = 1024)
    private String dataFilePath;

    /** Rows in the file, from Iceberg metadata. Used to weight progress, never to drive reads. */
    @Column(name = "record_count", nullable = false)
    private long recordCount;

    /**
     * Snapshot the file was collected from. Part of the dedup identity so that re-planning the
     * same snapshot after a crash is a no-op while the next snapshot enqueues freely.
     *
     * <p>Identity only: Iceberg snapshot ids are random longs, so this is never compared or
     * ordered. Ordering is {@link #sequenceNumber}, then {@link #committedAtMillis}, then
     * {@link #createdAt} -- the same rule the vector tables use, and the same mistake that has
     * been fixed in this repo more than once.
     */
    @Column(name = "snapshot_id", nullable = false)
    private long snapshotId;

    /** Iceberg sequence number of the producing snapshot. Monotonic, hence orderable. 0 = unknown. */
    @Column(name = "sequence_number", nullable = false)
    private long sequenceNumber;

    /** Commit wall clock of the producing snapshot, epoch millis. 0 = unknown. */
    @Column(name = "committed_at_millis", nullable = false)
    private long committedAtMillis;

    @Column(name = "kind", nullable = false, length = 16)
    @Enumerated(EnumType.STRING)
    private Kind kind;

    @Column(name = "state", nullable = false, length = 16)
    @Enumerated(EnumType.STRING)
    private State state;

    /**
     * Dispatch order, <em>lower first</em>, ties broken by {@link #createdAt}.
     *
     * <p>Ascending rather than the more familiar "bigger is more urgent" so that the dispatch
     * ORDER BY is {@code priority ASC, created_at ASC} and matches {@code idx_work_items_dispatch}
     * exactly. A descending leading column would force a sort of every pending row on each poll.
     * The practical use is keeping a million-file backfill behind live incremental work.
     */
    @Column(name = "priority", nullable = false)
    private int priority;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    /** Attempts after which the item goes terminal instead of back to PENDING. */
    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts;

    /** Worker that holds the lease. Null whenever state is not LEASED. */
    @Column(name = "lease_owner", length = 128)
    private String leaseOwner;

    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /** True while this item is owned by a worker whose lease has not yet elapsed. */
    public boolean isLeaseActive(Instant now) {
        return state == State.LEASED && leaseExpiresAt != null && leaseExpiresAt.isAfter(now);
    }

    /** True once the item can no longer be retried, whether it succeeded or gave up. */
    public boolean isTerminal() {
        return state == State.DONE || state == State.FAILED;
    }

    public enum Kind {
        /** Historical data the materialization has never seen. Bulk, low urgency, resumable. */
        BACKFILL,
        /** A file from a new source snapshot. Small and latency sensitive. */
        INCREMENTAL
    }

    public enum State {
        PENDING,
        LEASED,
        DONE,
        FAILED
    }
}
