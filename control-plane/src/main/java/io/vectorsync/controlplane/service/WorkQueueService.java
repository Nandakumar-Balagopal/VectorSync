package io.vectorsync.controlplane.service;

import io.vectorsync.controlplane.model.WorkItemEntity;
import io.vectorsync.controlplane.model.WorkItemEntity.Kind;
import io.vectorsync.controlplane.model.WorkItemEntity.State;
import io.vectorsync.controlplane.repository.WorkItemRepository;
import io.vectorsync.controlplane.repository.EmbeddedContentRepository;
import io.vectorsync.format.derive.ContentHash;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Durable, leased work queue for materialization.
 *
 * <p>A backfill is resumable only if progress is recorded at a finer grain than "table finished".
 * This service is that record: the planner enqueues one item per source data file, workers lease a
 * few at a time, and every completion is a commit. Kill every worker mid-backfill and the surviving
 * queue describes exactly what is left; a source file that cannot be embedded burns its retry budget
 * and goes terminal while its siblings keep draining, so one poisonous table cannot stall the pool.
 *
 * <p>Concurrency rests on two database properties rather than on any in-process coordination, so it
 * holds across worker restarts and multiple control-plane replicas:
 * <ul>
 *   <li>Claiming is {@code SELECT ... FOR UPDATE SKIP LOCKED} inside a transaction, so concurrent
 *       pollers take disjoint items without blocking each other.</li>
 *   <li>Enqueue is {@code INSERT ... ON CONFLICT DO NOTHING} on a hash of
 *       {@code (materializationId, dataFilePath, snapshotId)}, so replanning after a crash is a
 *       no-op instead of a second copy of the same backfill.</li>
 * </ul>
 *
 * <p>Success is accepted without proof of ownership: {@link #complete(String)} takes a report from a
 * worker that stalled past its lease and had the item reclaimed, because that worker is telling the
 * truth about work that really happened, and the downstream writes are keyed by content hash and
 * therefore idempotent. The cost of a late success is at worst one redundant re-embedding, which is
 * cheaper than refusing a genuine completion and re-running the file for certain. Failure is not
 * symmetric -- see {@link #fail(String, String, String)} -- because a stale failure spends a budget
 * and a queue slot that now belong to somebody else.
 */
@Service
@Slf4j
public class WorkQueueService {

    /** Live CDC work. Lower number drains first, so this outranks backfill. */
    public static final int PRIORITY_INCREMENTAL = 0;

    /**
     * Historical work. Ranked behind incremental so that a long backfill cannot delay freshness on
     * a table that is actively changing.
     */
    public static final int PRIORITY_BACKFILL = 100;

    public static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(5);

    /**
     * Ceiling on one lease call. Every claimed row stays locked until the claiming transaction
     * commits, so an unbounded limit would let one worker hold the head of the queue -- and,
     * worse, own more work than it can finish inside a lease.
     */
    private static final int MAX_LEASE_BATCH = 500;

    /**
     * Stack traces arrive here verbatim. Truncated so a retry storm cannot turn the queue table
     * into a log sink; the full trace is already in the worker's own logs.
     */
    private static final int MAX_ERROR_LENGTH = 4000;

    private final WorkItemRepository workItemRepository;

    private final EmbeddedContentRepository embeddedContentRepository;

    public WorkQueueService(WorkItemRepository workItemRepository,
                            EmbeddedContentRepository embeddedContentRepository) {
        this.workItemRepository = workItemRepository;
        this.embeddedContentRepository = embeddedContentRepository;
    }

    /** One embedding a worker durably committed, reported alongside the completion that wrote it. */
    public record EmbeddedContent(String modelVersion, String configId, String contentHash, int embeddingDim) {
    }

    /**
     * Enqueues work for a materialization, skipping anything already queued or finished, and
     * returns the number of genuinely new items.
     *
     * <p>Idempotent on {@code (materializationId, dataFilePath, snapshotId)}. That triple is the
     * identity of "this file, as seen in this snapshot, for this model and config", so the planner
     * can be re-run after a crash, a leader election or a duplicate webhook without duplicating
     * work. Deduplication happens in the database, not by a read-then-write in Java: two planners
     * racing would both see "absent" and both insert.
     *
     * @param materializationId (table, model, config) identity the work belongs to
     * @param descriptors       one per source data file; duplicates within the batch are collapsed
     * @return count of newly inserted items, so a caller can log "enqueued 12 of 4000 files"
     */
    @Transactional
    public int enqueue(String materializationId, List<WorkDescriptor> descriptors) {
        if (materializationId == null || materializationId.isBlank()) {
            throw new IllegalArgumentException("materializationId is required");
        }
        if (descriptors == null || descriptors.isEmpty()) {
            return 0;
        }

        Instant now = Instant.now();

        // Keyed and sorted before the first insert, for two reasons. Collapsing repeats here rather
        // than leaving them to ON CONFLICT saves a round trip for a planner that lists the same
        // manifest twice. Inserting in dedup-key order is the more important one: ON CONFLICT DO
        // NOTHING has to wait on a conflicting row that a concurrent transaction has inserted but
        // not yet committed, so two planners covering overlapping files in different orders deadlock
        // and Postgres aborts one of them -- losing a whole batch to the failure mode ON CONFLICT was
        // chosen to prevent. One agreed lock order makes the cycle impossible. Nothing downstream
        // depends on insertion order: every row in a batch shares one created_at, so the dispatch
        // tiebreak among them was never this order anyway.
        Map<String, WorkDescriptor> byDedupKey = new TreeMap<>();
        for (WorkDescriptor descriptor : descriptors) {
            if (descriptor == null) {
                continue;
            }
            String dataFilePath = requireText(descriptor.getDataFilePath(), "dataFilePath");
            requireText(descriptor.getSourceTable(), "sourceTable");
            byDedupKey.putIfAbsent(
                    dedupKey(materializationId, dataFilePath, descriptor.getSnapshotId()), descriptor);
        }

        int inserted = 0;
        for (Map.Entry<String, WorkDescriptor> entry : byDedupKey.entrySet()) {
            WorkDescriptor descriptor = entry.getValue();
            Kind kind = descriptor.getKind() == null ? Kind.INCREMENTAL : descriptor.getKind();

            inserted += workItemRepository.insertIfAbsent(
                    UUID.randomUUID().toString(),
                    entry.getKey(),
                    materializationId,
                    descriptor.getSourceTable(),
                    descriptor.getDataFilePath(),
                    Math.max(0L, descriptor.getRecordCount()),
                    descriptor.getSnapshotId(),
                    descriptor.getSequenceNumber(),
                    descriptor.getCommittedAtMillis(),
                    kind.name(),
                    descriptor.getPriority() == null ? defaultPriority(kind) : descriptor.getPriority(),
                    resolveMaxAttempts(descriptor.getMaxAttempts()),
                    now);
        }

        log.info("Enqueued {} new work items for materialization {} ({} descriptors offered)",
                inserted, materializationId, descriptors.size());
        return inserted;
    }

    /**
     * Claims up to {@code limit} items for {@code owner} and stamps a lease that expires after
     * {@code leaseDuration}.
     *
     * <p>Transactional and not read-only on purpose. The row locks taken by the claim query are
     * released at commit, and the lease stamp is written inside the same transaction, so no other
     * worker can observe a claimed-but-unstamped row. Split the claim and the stamp across two
     * transactions and the queue hands the same file to two workers.
     *
     * <p>Returned in dispatch order, and possibly shorter than {@code limit} because peers may
     * hold the head of the queue. An empty list means "nothing available right now", never
     * "the queue is drained" -- ask {@link #stats(String)} for that.
     */
    @Transactional
    public List<WorkItemEntity> lease(String owner, int limit, Duration leaseDuration) {
        return lease(owner, limit, leaseDuration, null);
    }

    /**
     * Claims items, optionally restricted to one materialization.
     *
     * <p>A caller that can only process one materialization must pass it. Filtering after an
     * unscoped lease is not equivalent: the items it cannot use are already leased to it, and
     * handing them back as failures consumes their retry budget.
     */
    @Transactional
    public List<WorkItemEntity> lease(String owner,
                                      int limit,
                                      Duration leaseDuration,
                                      String materializationId) {
        if (owner == null || owner.isBlank()) {
            throw new IllegalArgumentException("owner is required");
        }
        if (limit <= 0) {
            return List.of();
        }

        int effectiveLimit = Math.min(limit, MAX_LEASE_BATCH);
        Duration effectiveLease = leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()
                ? DEFAULT_LEASE_DURATION
                : leaseDuration;

        List<String> ids = materializationId == null || materializationId.isBlank()
                ? workItemRepository.selectLeasableIds(effectiveLimit)
                : workItemRepository.selectLeasableIdsFor(materializationId, effectiveLimit);
        if (ids.isEmpty()) {
            return List.of();
        }

        Instant now = Instant.now();
        Instant expiresAt = now.plus(effectiveLease);

        // findAllById loses the priority ordering the claim query established, so reindex by id and
        // walk the id list. Workers process in the order they receive, and that order is the point.
        Map<String, WorkItemEntity> byId = new HashMap<>(ids.size() * 2);
        for (WorkItemEntity item : workItemRepository.findAllById(ids)) {
            item.setState(State.LEASED);
            item.setLeaseOwner(owner);
            item.setLeaseExpiresAt(expiresAt);
            item.setUpdatedAt(now);
            byId.put(item.getId(), item);
        }

        List<WorkItemEntity> leased = new ArrayList<>(byId.size());
        for (String id : ids) {
            WorkItemEntity item = byId.get(id);
            if (item != null) {
                leased.add(item);
            }
        }

        workItemRepository.saveAll(leased);
        log.debug("Leased {} work items to {} until {}", leased.size(), owner, expiresAt);
        return leased;
    }

    /**
     * Marks an item finished. Terminal and idempotent: a repeated report is a no-op.
     *
     * <p>Accepts a completion for an item that is no longer LEASED (its lease expired and the
     * reclaimer pushed it back to PENDING). The work provably happened, so recording it is strictly
     * better than letting a peer redo it; the only risk is that a peer has already re-leased the
     * item and will finish a duplicate, which content-hash keying makes harmless.
     */
    @Transactional
    public void complete(String id) {
        complete(id, List.of());
    }

    /**
     * Marks an item done and records the embeddings it committed, in one transaction.
     *
     * <p>The single transaction is the correctness property, not an optimisation. "This content has
     * a durable vector" and "the work that wrote it finished" must not be separately observable: if
     * the record were written first and completion failed, a retry would trust a cache hit for a
     * vector whose write may have been rolled back; if completion were written first and the record
     * failed, the content would be silently re-embedded forever with no way to notice.
     *
     * <p>The worker commits to Iceberg <em>before</em> calling this, which makes the remaining error
     * one-directional. A completion that never lands leaves the content unrecorded, so a retry pays
     * for a byte-identical embedding again -- wasteful and harmless. The reverse, a record without a
     * vector, is the one that cannot happen.
     */
    @Transactional
    public void complete(String id, List<EmbeddedContent> embedded) {
        WorkItemEntity item = mustFind(id);
        if (item.getState() == State.DONE) {
            return;
        }
        if (item.getState() != State.LEASED) {
            log.warn("Completing work item {} from state {}; its lease had already been reclaimed",
                    id, item.getState());
        }

        item.setState(State.DONE);
        item.setLeaseOwner(null);
        item.setLeaseExpiresAt(null);
        item.setLastError(null);
        item.setUpdatedAt(Instant.now());
        workItemRepository.save(item);

        Instant now = Instant.now();
        int recorded = 0;
        for (EmbeddedContent content : embedded == null ? List.<EmbeddedContent>of() : embedded) {
            if (content == null || blank(content.contentHash())
                    || blank(content.modelVersion()) || blank(content.configId())) {
                continue;
            }
            recorded += embeddedContentRepository.recordIfAbsent(
                    content.modelVersion(), content.configId(), content.contentHash(),
                    content.embeddingDim(), now);
        }

        log.debug("Work item {} done ({} rows from {}), {} of {} embeddings newly recorded",
                id, item.getRecordCount(), item.getDataFilePath(), recorded,
                embedded == null ? 0 : embedded.size());
    }

    /**
     * Which of {@code contentHashes} already have a durable vector.
     *
     * <p>Read-only and safe to call from any worker: the answer is a property of committed state
     * rather than of the caller's process.
     */
    @Transactional(readOnly = true)
    public Set<String> findExistingHashes(String modelVersion,
                                          String configId,
                                          Collection<String> contentHashes) {
        if (contentHashes == null || contentHashes.isEmpty()) {
            return Set.of();
        }
        return new HashSet<>(embeddedContentRepository.findExistingHashes(
                modelVersion, configId, contentHashes));
    }

    @Transactional(readOnly = true)
    public long countEmbedded(String modelVersion, String configId) {
        return embeddedContentRepository.countForScope(modelVersion, configId);
    }

    /** Records a failed attempt without claiming which lease it belongs to. */
    @Transactional
    public void fail(String id, String error) {
        fail(id, null, error);
    }

    /**
     * Records a failed attempt. Returns the item to PENDING while it has attempts left, otherwise
     * parks it in terminal FAILED.
     *
     * <p>Only a LEASED item can consume an attempt. A duplicate failure report -- a worker that
     * retries the callback after a timeout, say -- must not spend a second attempt on the same
     * try, or a transient control-plane blip would exhaust the budget of every in-flight item.
     * The error text is still recorded so the most recent diagnosis is not lost.
     *
     * <p>{@code owner} fences the report against the lease generation it was issued for, and it is
     * what makes the rule above hold under a slow worker. A worker that overruns its lease, has the
     * item reclaimed and re-leased to a peer, and only then reports failure is describing a lease
     * that no longer exists: charging that attempt spends the peer's budget (two attempts per real
     * failure, so items retire FAILED early and their rows silently never reach the index), and
     * resetting the state to PENDING would put the item back on the dispatch queue while the peer is
     * still working it. A null owner keeps the unfenced behavior for a caller that cannot supply one.
     */
    @Transactional
    public void fail(String id, String owner, String error) {
        WorkItemEntity item = mustFind(id);
        String message = truncate(error);
        Instant now = Instant.now();

        if (item.getState() != State.LEASED) {
            log.warn("Ignoring failure report for work item {} in state {}: {}",
                    id, item.getState(), message);
            if (!item.isTerminal()) {
                item.setLastError(message);
                item.setUpdatedAt(now);
                workItemRepository.save(item);
            }
            return;
        }
        if (owner != null && item.getLeaseOwner() != null && !owner.equals(item.getLeaseOwner())) {
            log.warn("Ignoring failure report for work item {} from {}: lease now held by {}: {}",
                    id, owner, item.getLeaseOwner(), message);
            item.setLastError(message);
            item.setUpdatedAt(now);
            workItemRepository.save(item);
            return;
        }

        int attempts = item.getAttempts() + 1;
        item.setAttempts(attempts);
        item.setLastError(message);
        item.setLeaseOwner(null);
        item.setLeaseExpiresAt(null);
        item.setUpdatedAt(now);

        if (attempts >= item.getMaxAttempts()) {
            item.setState(State.FAILED);
            log.error("Work item {} ({}) failed permanently after {} attempts: {}",
                    id, item.getDataFilePath(), attempts, message);
        } else {
            item.setState(State.PENDING);
            log.warn("Work item {} ({}) failed on attempt {} of {}, requeued: {}",
                    id, item.getDataFilePath(), attempts, item.getMaxAttempts(), message);
        }
        workItemRepository.save(item);
    }

    /**
     * Returns items whose lease has elapsed to the queue, and returns how many moved.
     *
     * <p>This is the difference between a durable queue and a resumable one. Without it, a worker
     * that is OOM-killed holding ten leases leaves those ten items LEASED forever: the rows survive
     * the crash but nothing will ever dispatch them again. Intended to be called on a short
     * schedule (well under the lease duration) by whatever scheduler the lead wires up.
     *
     * <p>Exhausted items are failed before the rest are requeued; doing it in the other order would
     * reset their state and leave nothing to retire, so a reliably fatal file would cycle forever.
     */
    @Transactional
    public int reclaimExpiredLeases() {
        Instant now = Instant.now();
        int exhausted = workItemRepository.failExhaustedExpiredLeases(
                now, "Lease expired and retry budget exhausted");
        int requeued = workItemRepository.requeueExpiredLeases(now);

        int reclaimed = exhausted + requeued;
        if (reclaimed > 0) {
            log.warn("Reclaimed {} expired leases: {} requeued, {} exhausted and failed",
                    reclaimed, requeued, exhausted);
        }
        return reclaimed;
    }

    /**
     * Progress for one materialization, or across all of them when {@code materializationId} is
     * null or blank.
     *
     * <p>Aggregated by the database. A backfill of a large table is millions of rows, and counting
     * them in Java would mean shipping the entire queue to answer a progress bar.
     */
    @Transactional(readOnly = true)
    public QueueStats stats(String materializationId) {
        boolean scoped = materializationId != null && !materializationId.isBlank();
        List<Object[]> rows = scoped
                ? workItemRepository.countByStateForMaterialization(materializationId)
                : workItemRepository.countByState();

        Map<State, Long> counts = new HashMap<>();
        for (Object[] row : rows) {
            counts.put((State) row[0], ((Number) row[1]).longValue());
        }

        long pending = counts.getOrDefault(State.PENDING, 0L);
        long leased = counts.getOrDefault(State.LEASED, 0L);
        long done = counts.getOrDefault(State.DONE, 0L);
        long failed = counts.getOrDefault(State.FAILED, 0L);

        return QueueStats.builder()
                .materializationId(scoped ? materializationId : null)
                .pending(pending)
                .leased(leased)
                .done(done)
                .failed(failed)
                .total(pending + leased + done + failed)
                .build();
    }

    /** Single item lookup, for a worker that wants to re-read state it may have lost. */
    @Transactional(readOnly = true)
    public Optional<WorkItemEntity> find(String id) {
        return id == null || id.isBlank() ? Optional.empty() : workItemRepository.findById(id);
    }

    /**
     * Hashes the enqueue identity into the fixed-width column the unique index covers.
     *
     * <p>Reuses {@link ContentHash} for the SHA-256 and, more importantly, for its unit-separator
     * join: with an ordinary delimiter a materialization id or file path containing that delimiter
     * could produce the same joined string as a different triple and silently swallow real work.
     */
    private static String dedupKey(String materializationId, String dataFilePath, long snapshotId) {
        return ContentHash.of(
                List.of(materializationId, dataFilePath, Long.toString(snapshotId)),
                ContentHash.SEPARATOR);
    }

    private static int defaultPriority(Kind kind) {
        return kind == Kind.BACKFILL ? PRIORITY_BACKFILL : PRIORITY_INCREMENTAL;
    }

    private static int resolveMaxAttempts(Integer requested) {
        // Zero or negative would make the item undispatchable the moment it is inserted, since
        // dispatch requires attempts < max_attempts.
        return requested == null || requested < 1 ? WorkItemEntity.DEFAULT_MAX_ATTEMPTS : requested;
    }

    private WorkItemEntity mustFind(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("work item id is required");
        }
        return workItemRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Unknown work item: " + id));
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private static String truncate(String error) {
        if (error == null || error.isBlank()) {
            return "unspecified failure";
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }

    /**
     * One unit of work as the planner sees it, before the queue gives it an id and a lifecycle.
     *
     * <p>{@code snapshotId} is part of the enqueue identity but is never ordered on: Iceberg
     * snapshot ids are random longs. Ordering uses {@code sequenceNumber}, then
     * {@code committedAtMillis}. Longs rather than boxed types so that "unknown" is 0 and the
     * native insert never has to bind a null, which Postgres rejects for want of a declared type.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class WorkDescriptor {
        private String sourceTable;
        private String dataFilePath;
        private long recordCount;
        private long snapshotId;
        private long sequenceNumber;
        private long committedAtMillis;
        private Kind kind;
        /** Null takes the default for the kind: backfill behind incremental. */
        private Integer priority;
        /** Null takes {@link WorkItemEntity#DEFAULT_MAX_ATTEMPTS}. */
        private Integer maxAttempts;
    }

    /** Queue depth by state. {@code materializationId} is null when the counts are global. */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class QueueStats {
        private String materializationId;
        private long pending;
        private long leased;
        private long done;
        private long failed;
        private long total;

        /**
         * True once nothing is queued or in flight. Says nothing about success: a queue with
         * retired FAILED items is drained. Use {@link #isComplete()} before advancing anything
         * that means "this source version is materialized".
         */
        public boolean isDrained() {
            return pending == 0 && leased == 0;
        }

        /**
         * True once every item finished successfully.
         *
         * <p>The distinction from {@link #isDrained()} is the one that matters to a caller holding a
         * watermark. A file that exhausted its retries is terminal, so the queue will never dispatch
         * it again; treating "drained" as "done" would advance the materialized version past rows
         * that were never embedded, and nothing downstream would ever notice they are missing.
         */
        public boolean isComplete() {
            return isDrained() && failed == 0;
        }
    }
}
