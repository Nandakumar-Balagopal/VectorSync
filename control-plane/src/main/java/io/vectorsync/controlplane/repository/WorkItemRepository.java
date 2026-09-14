package io.vectorsync.controlplane.repository;

import io.vectorsync.controlplane.model.WorkItemEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/**
 * Persistence for the leased work queue.
 *
 * <p>Three operations here are deliberately native SQL rather than derived methods, because each
 * one depends on behavior JPQL cannot express:
 * <ul>
 *   <li>{@link #insertIfAbsent} needs {@code ON CONFLICT DO NOTHING}. Checking for existence and
 *       then inserting would race two concurrent planners into a constraint violation, and in
 *       Postgres that violation aborts the whole transaction -- so an enqueue of 10,000 files
 *       would lose all of them because one was already present.</li>
 *   <li>{@link #selectLeasableIds} needs {@code FOR UPDATE SKIP LOCKED}, the only formulation in
 *       which N workers can poll the same queue concurrently without serializing on each other or
 *       handing the same item to two of them.</li>
 *   <li>The reclaim statements need set-relative arithmetic ({@code attempts = attempts + 1})
 *       applied to an unbounded row set in one round trip.</li>
 * </ul>
 * All parameters bound by the native statements are non-null (nullable columns are written as
 * literal {@code NULL} in the SQL), because Hibernate has no declared type to hand Postgres for a
 * null bind and the driver rejects it with "could not determine data type of parameter".
 */
@Repository
public interface WorkItemRepository extends JpaRepository<WorkItemEntity, String> {

    /**
     * Inserts one PENDING item unless its dedup key is already present, returning 1 if it was
     * inserted and 0 if it was already queued, running or finished.
     *
     * <p>The conflict target is the unique index behind {@code uq_work_items_dedup}. DO NOTHING
     * rather than DO UPDATE: an item already in the queue may currently be LEASED by a worker, and
     * overwriting its state or attempt count from a replanner would resurrect work that is in
     * flight.
     */
    @Modifying
    @Query(value = """
            INSERT INTO work_items (
                id, dedup_key, materialization_id, source_table, data_file_path, record_count,
                snapshot_id, sequence_number, committed_at_millis, kind, state, priority,
                attempts, max_attempts, lease_owner, lease_expires_at, last_error,
                created_at, updated_at)
            VALUES (
                :id, :dedupKey, :materializationId, :sourceTable, :dataFilePath, :recordCount,
                :snapshotId, :sequenceNumber, :committedAtMillis, :kind, 'PENDING', :priority,
                0, :maxAttempts, NULL, NULL, NULL, :now, :now)
            ON CONFLICT (dedup_key) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("id") String id,
                       @Param("dedupKey") String dedupKey,
                       @Param("materializationId") String materializationId,
                       @Param("sourceTable") String sourceTable,
                       @Param("dataFilePath") String dataFilePath,
                       @Param("recordCount") long recordCount,
                       @Param("snapshotId") long snapshotId,
                       @Param("sequenceNumber") long sequenceNumber,
                       @Param("committedAtMillis") long committedAtMillis,
                       @Param("kind") String kind,
                       @Param("priority") int priority,
                       @Param("maxAttempts") int maxAttempts,
                       @Param("now") Instant now);

    /**
     * Claims up to {@code limit} dispatchable item ids in priority order, locking them for the
     * duration of the calling transaction and skipping anything another worker is already
     * claiming.
     *
     * <p>SKIP LOCKED is the whole point: plain {@code FOR UPDATE} would make the second poller
     * block until the first committed, turning a pool of workers into a queue of one. The rows
     * stay locked until the caller's transaction commits, so the lease stamp written after this
     * call cannot be lost to a competing claim -- which is also why the caller must be
     * transactional. Run outside a transaction, each statement autocommits, the locks evaporate
     * immediately and two workers happily lease the same file.
     *
     * <p>Fewer than {@code limit} rows coming back does not mean the queue is empty; it may mean
     * the visible head was locked by peers. Callers poll again rather than concluding "drained".
     *
     * <p>{@code attempts < max_attempts} is a belt-and-braces guard. {@code fail} already sends an
     * exhausted item to FAILED, but a row left behind by an older build must not be redispatched
     * forever.
     */
    @Query(value = """
            SELECT id
            FROM work_items
            WHERE state = 'PENDING'
              AND attempts < max_attempts
            ORDER BY priority ASC, created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<String> selectLeasableIds(@Param("limit") int limit);

    /**
     * As {@link #selectLeasableIds} but restricted to one materialization.
     *
     * <p>Needed because a runner iterating materializations one at a time can only derive work for
     * the spec it currently holds. Leasing from the global head handed it items belonging to other
     * materializations, and reporting those back as failures spent their retry budget: with three
     * or more materializations registered, the queue head's items reached terminal FAILED before any
     * worker ever opened the file.
     */
    @Query(value = """
            SELECT id
            FROM work_items
            WHERE state = 'PENDING'
              AND attempts < max_attempts
              AND materialization_id = :materializationId
            ORDER BY priority ASC, created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<String> selectLeasableIdsFor(@Param("materializationId") String materializationId,
                                      @Param("limit") int limit);

    /**
     * Sends expired leases whose next attempt would exceed the budget straight to FAILED.
     *
     * <p>Must run before {@link #requeueExpiredLeases}, which would otherwise reset these rows to
     * PENDING first and leave nothing for this statement to match.
     *
     * <p>A NULL {@code lease_expires_at} on a LEASED row should be impossible, but it is included
     * as expired anyway: the alternative is a row that no reclaimer can ever see and no worker
     * will ever finish.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE work_items
            SET state = 'FAILED',
                attempts = attempts + 1,
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_error = :message,
                updated_at = :now
            WHERE state = 'LEASED'
              AND (lease_expires_at IS NULL OR lease_expires_at < :now)
              AND attempts + 1 >= max_attempts
            """, nativeQuery = true)
    int failExhaustedExpiredLeases(@Param("now") Instant now, @Param("message") String message);

    /**
     * Returns every remaining expired lease to PENDING, spending one attempt.
     *
     * <p>The attempt is charged deliberately. A worker that segfaults on a specific file would
     * otherwise be handed that file again after every lease timeout, forever, which is the exact
     * "one bad table stalls the pool" failure the retry budget exists to bound.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE work_items
            SET state = 'PENDING',
                attempts = attempts + 1,
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = :now
            WHERE state = 'LEASED'
              AND (lease_expires_at IS NULL OR lease_expires_at < :now)
            """, nativeQuery = true)
    int requeueExpiredLeases(@Param("now") Instant now);

    /**
     * Row counts per state for one materialization, as {@code [State, Long]} pairs. Grouped in the
     * database so progress for a million-file backfill costs one index scan instead of a million
     * rows crossing the wire.
     */
    @Query("SELECT w.state, COUNT(w) FROM WorkItemEntity w "
            + "WHERE w.materializationId = :materializationId GROUP BY w.state")
    List<Object[]> countByStateForMaterialization(@Param("materializationId") String materializationId);

    /** Row counts per state across every materialization, as {@code [State, Long]} pairs. */
    @Query("SELECT w.state, COUNT(w) FROM WorkItemEntity w GROUP BY w.state")
    List<Object[]> countByState();
}
