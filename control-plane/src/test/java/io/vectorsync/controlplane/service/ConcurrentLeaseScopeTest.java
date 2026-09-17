package io.vectorsync.controlplane.service;

import io.vectorsync.controlplane.model.WorkItemEntity;
import io.vectorsync.controlplane.repository.WorkItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The claim that leasing is safe under concurrency, which until now the repository only asserted in
 * prose.
 *
 * <p>The queue hands work out with {@code FOR UPDATE SKIP LOCKED} and scopes each lease to one
 * materialization. Both halves have been described as correct in comments and commit messages, and
 * neither was tested: the existing suite covers dedup-record semantics, not contention. An earlier
 * bug in this area caused data loss at three or more materializations -- the lease was global, so a
 * worker that could only process materialization A leased items belonging to B and handed them back
 * as failures, consuming B's retry budget until its items were exhausted and permanently dropped.
 * The scoping fix is what made it safe, and this pins both properties.
 *
 * <p>Two things are being tested, and they fail in different ways. Mutual exclusion: no item may be
 * leased to two owners at once, or the same file is derived twice and the same inference is paid for
 * twice. Scope isolation: a scoped lease must return nothing belonging to another materialization,
 * however many workers are competing.
 *
 * <p>Real Postgres, and necessarily: {@code SKIP LOCKED} is the mechanism under test and no
 * in-memory database implements it. Concurrency is driven from real threads against real
 * transactions rather than simulated, because the property is about what the database does when two
 * transactions reach the same row.
 */
@SpringBootTest
@Testcontainers
class ConcurrentLeaseScopeTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    WorkQueueService workQueue;
    @Autowired
    WorkItemRepository workItemRepository;

    /** A materialization id prefix per test, so tests sharing one database cannot see each other. */
    private String scope;

    @BeforeEach
    void scopePerTest(TestInfo info) {
        scope = Integer.toHexString(
                info.getTestMethod().map(java.lang.reflect.Method::getName).orElse("x").hashCode()
                        & 0x7fffffff);
    }

    private String materialization(String suffix) {
        return "m-" + scope + "-" + suffix;
    }

    private void enqueue(String materializationId, int files) {
        List<WorkQueueService.WorkDescriptor> descriptors = new ArrayList<>(files);
        for (int file = 0; file < files; file++) {
            descriptors.add(WorkQueueService.WorkDescriptor.builder()
                    .sourceTable("default." + materializationId)
                    .dataFilePath("s3://bucket/" + materializationId + "/file-" + file + ".parquet")
                    .recordCount(100)
                    .snapshotId(1L)
                    .sequenceNumber(1L)
                    .committedAtMillis(1L)
                    .kind(WorkItemEntity.Kind.BACKFILL)
                    .build());
        }
        assertEquals(files, workQueue.enqueue(materializationId, descriptors));
    }

    private <T> List<T> inParallel(List<Callable<T>> tasks) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(tasks.size());
        try {
            List<T> results = new ArrayList<>(tasks.size());
            for (Future<T> future : pool.invokeAll(tasks)) {
                results.add(future.get());
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("concurrent workers never lease the same item twice")
    void leasesAreMutuallyExclusive() throws Exception {
        String materializationId = materialization("a");
        enqueue(materializationId, 40);

        // Eight workers each try to take more than their share. Without SKIP LOCKED and the row
        // lock, two would return overlapping sets and the same data file would be derived twice --
        // paying for the same inference twice and, before the dedup record was durable, writing the
        // same content to Tier 1 twice.
        List<Callable<List<String>>> tasks = new ArrayList<>();
        for (int worker = 0; worker < 8; worker++) {
            String owner = "worker-" + worker;
            tasks.add(() -> workQueue.lease(owner, 10, Duration.ofMinutes(5), materializationId)
                    .stream().map(WorkItemEntity::getId).toList());
        }

        List<List<String>> leasedPerWorker = inParallel(tasks);

        Set<String> seen = ConcurrentHashMap.newKeySet();
        int total = 0;
        for (List<String> leased : leasedPerWorker) {
            total += leased.size();
            for (String id : leased) {
                assertTrue(seen.add(id),
                        "item " + id + " was leased to two workers at once, so the same data file "
                                + "would be derived twice");
            }
        }

        assertEquals(40, total,
                "every enqueued item should have been handed out exactly once across the workers");
        assertEquals(40, seen.size());
    }

    @Test
    @DisplayName("a scoped lease returns nothing belonging to another materialization")
    void scopedLeasesDoNotCross() throws Exception {
        String first = materialization("a");
        String second = materialization("b");
        String third = materialization("c");
        enqueue(first, 12);
        enqueue(second, 12);
        enqueue(third, 12);

        // The shape of the historical bug: several materializations, several workers, each worker
        // able to process only its own. A worker that receives a foreign item cannot use it, and
        // reporting it as failed burns a retry budget that does not belong to it.
        List<Callable<List<WorkItemEntity>>> tasks = new ArrayList<>();
        for (String target : List.of(first, second, third, first, second, third)) {
            tasks.add(() -> workQueue.lease("worker-for-" + target, 6, Duration.ofMinutes(5), target));
        }

        List<List<WorkItemEntity>> results = inParallel(tasks);

        for (int task = 0; task < results.size(); task++) {
            String expected = List.of(first, second, third, first, second, third).get(task);
            for (WorkItemEntity item : results.get(task)) {
                assertEquals(expected, item.getMaterializationId(),
                        "a lease scoped to " + expected + " returned an item belonging to "
                                + item.getMaterializationId() + ", which is the unscoped-lease bug "
                                + "that consumed another materialization's retry budget");
            }
        }
    }

    @Test
    @DisplayName("a scoped lease leaves other materializations' items pending, not hidden")
    void scopingDoesNotStarvePeers() {
        String mine = materialization("a");
        String theirs = materialization("b");
        enqueue(mine, 5);
        enqueue(theirs, 5);

        // Draining one scope entirely must leave the other scope exactly as it was. A lease that
        // touched peer rows -- even only to skip them -- could leave them locked or attempted.
        List<WorkItemEntity> leased =
                workQueue.lease("worker", 10, Duration.ofMinutes(5), mine);
        assertEquals(5, leased.size(), "the whole of my own scope should be leasable");

        List<WorkItemEntity> peers = workItemRepository.findAll().stream()
                .filter(item -> theirs.equals(item.getMaterializationId()))
                .toList();
        assertEquals(5, peers.size());
        for (WorkItemEntity peer : peers) {
            assertEquals(WorkItemEntity.State.PENDING, peer.getState(),
                    "a peer item changed state while another scope was being leased");
            assertEquals(0, peer.getAttempts(),
                    "a peer item was charged an attempt by a lease it was never handed to");
        }
    }

    @Test
    @DisplayName("an owner that leases twice does not accumulate the same item twice")
    void repeatedLeaseByOneOwnerIsNotDoubleCounted() {
        String materializationId = materialization("a");
        enqueue(materializationId, 6);

        List<WorkItemEntity> firstPass =
                workQueue.lease("worker", 4, Duration.ofMinutes(5), materializationId);
        List<WorkItemEntity> secondPass =
                workQueue.lease("worker", 4, Duration.ofMinutes(5), materializationId);

        // The runner leases repeatedly within one cycle until the queue drains, so the same owner
        // asking again must advance rather than re-serve what it already holds.
        Set<String> ids = new java.util.HashSet<>();
        firstPass.forEach(item -> ids.add(item.getId()));
        secondPass.forEach(item -> assertTrue(ids.add(item.getId()),
                "a second lease by the same owner returned an item it already held"));
        assertEquals(6, ids.size(), "the two passes together should cover the whole scope");
    }
}
