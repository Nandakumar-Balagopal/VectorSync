package io.vectorsync.controlplane.service;

import io.vectorsync.controlplane.model.WorkItemEntity;
import io.vectorsync.controlplane.repository.EmbeddedContentRepository;
import io.vectorsync.controlplane.repository.WorkItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dedup set must be falsifiable, and it must be shared.
 *
 * <p>The design this replaces kept an exact set in one JVM's heap and treated a hit as permanently
 * true because the embedding store is append-only. That reasoning omits the case where a commit the
 * process believed it made did not survive: the set then reported a hit forever, inference was
 * skipped, and the content map committed a pointer to a vector that does not exist, which caps
 * projection coverage and stalls the serving watermark with no runtime repair. It also meant the
 * measured deduplication property held for exactly one worker.
 *
 * <p>These tests pin the two properties that make the replacement safe: a hash is recorded only by
 * the transaction that completes the work which wrote it, and every worker sees the same answer.
 *
 * <p>Runs against real Postgres via Testcontainers, and has to. Both the enqueue path and the
 * dedup record rely on {@code INSERT ... ON CONFLICT DO NOTHING}, which H2 cannot parse even in
 * PostgreSQL compatibility mode -- so an in-memory substitute would either fail outright or force
 * the production SQL to be weakened to suit the test. Flyway runs here too, which means the
 * migrations themselves are exercised rather than assumed.
 */
@SpringBootTest
@Testcontainers
class DurableDedupTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private static final String MODEL = "all-MiniLM-L6-v2:v1";

    /**
     * A configuration id per test method.
     *
     * <p>Tests share one database, and {@code embedded_content} is keyed by scope, so a shared
     * config id makes any count assertion depend on execution order -- which is how the batch test
     * first read 503 instead of 500. A distinct scope per test is also the more faithful shape: two
     * different derivation configurations genuinely do not share entries.
     */
    private String config;

    @BeforeEach
    void scopePerTest(TestInfo info) {
        config = "cfg-" + info.getTestMethod().map(java.lang.reflect.Method::getName).orElse("unknown");
    }

    @Autowired
    WorkQueueService workQueue;
    @Autowired
    WorkItemRepository workItemRepository;
    @Autowired
    EmbeddedContentRepository embeddedContentRepository;

    private String leaseOneItem(String materializationId, String file) {
        workQueue.enqueue(materializationId, List.of(WorkQueueService.WorkDescriptor.builder()
                .sourceTable("default.products")
                .dataFilePath(file)
                .recordCount(100)
                .snapshotId(1L)
                .sequenceNumber(1L)
                .committedAtMillis(1L)
                .kind(WorkItemEntity.Kind.BACKFILL)
                .build()));

        List<WorkItemEntity> leased = workQueue.lease(
                "test-owner", 1, Duration.ofMinutes(5), materializationId);
        assertEquals(1, leased.size(), "the item just enqueued should be leasable");
        return leased.get(0).getId();
    }

    @Test
    @DisplayName("a hash is not embedded-known until the completion that wrote it commits")
    void unrecordedUntilCompletion() {
        String itemId = leaseOneItem("mat-1", "s3://bucket/a.parquet");

        // Before completion the vectors may be durable in Iceberg, but nothing has attested to it.
        // Reporting a hit here is exactly the failure being prevented.
        assertTrue(workQueue.findExistingHashes(MODEL, config, List.of("hash-a")).isEmpty(),
                "no hash may be reported present before the work that wrote it completed");

        workQueue.complete(itemId, List.of(
                new WorkQueueService.EmbeddedContent(MODEL, config, "hash-a", 384)));

        assertEquals(Set.of("hash-a"), workQueue.findExistingHashes(MODEL, config, List.of("hash-a")));
    }

    @Test
    @DisplayName("an abandoned item records nothing, so its content is re-embedded rather than assumed")
    void failedWorkRecordsNothing() {
        String itemId = leaseOneItem("mat-2", "s3://bucket/b.parquet");

        workQueue.fail(itemId, "test-owner", "embedding provider unavailable");

        assertTrue(workQueue.findExistingHashes(MODEL, config, List.of("hash-b")).isEmpty(),
                "a failed pass must leave the content looking novel; the error must be a redundant "
                        + "embedding, never a missing vector");
    }

    @Test
    @DisplayName("the record is shared, so a second worker sees what the first wrote")
    void visibleAcrossWorkers() {
        String first = leaseOneItem("mat-3", "s3://bucket/c.parquet");
        workQueue.complete(first, List.of(
                new WorkQueueService.EmbeddedContent(MODEL, config, "shared-hash", 384)));

        // A different owner leasing different work asks the same question and gets the same answer.
        // In the replaced design this returned empty, because the answer lived in the first
        // worker's heap -- which is why the measured dedup property held only at one worker.
        String second = leaseOneItem("mat-4", "s3://bucket/d.parquet");
        assertEquals(Set.of("shared-hash"),
                workQueue.findExistingHashes(MODEL, config, List.of("shared-hash")));
        workQueue.complete(second, List.of());
    }

    @Test
    @DisplayName("the same content under a different model or config is a different vector")
    void scopeIsPartOfIdentity() {
        String itemId = leaseOneItem("mat-5", "s3://bucket/e.parquet");
        workQueue.complete(itemId, List.of(
                new WorkQueueService.EmbeddedContent(MODEL, config, "scoped-hash", 384)));

        assertTrue(workQueue.findExistingHashes("all-mpnet-base-v2:v1", config,
                List.of("scoped-hash")).isEmpty(), "a different model is a different vector");
        assertTrue(workQueue.findExistingHashes(MODEL, "other-config-id",
                List.of("scoped-hash")).isEmpty(), "a different configuration is a different vector");
    }

    @Test
    @DisplayName("recording the same hash twice is not an error")
    void recordingIsIdempotent() {
        // Two workers can derive the same novel content concurrently and both legitimately succeed,
        // and a retried completion repeats its report. A conflict is the expected outcome of a
        // healthy race, so it must not fail the transaction that carries the completion.
        String first = leaseOneItem("mat-6", "s3://bucket/f.parquet");
        workQueue.complete(first, List.of(
                new WorkQueueService.EmbeddedContent(MODEL, config, "dup-hash", 384)));

        String second = leaseOneItem("mat-7", "s3://bucket/g.parquet");
        workQueue.complete(second, List.of(
                new WorkQueueService.EmbeddedContent(MODEL, config, "dup-hash", 384)));

        assertEquals(1L, embeddedContentRepository.countForScope(MODEL, config));
    }

    @Test
    @DisplayName("a probe answers a large batch and reports only what it was asked about")
    void probeIsScopedToTheBatch() {
        String itemId = leaseOneItem("mat-8", "s3://bucket/h.parquet");

        List<WorkQueueService.EmbeddedContent> embedded = new java.util.ArrayList<>();
        for (int i = 0; i < 500; i++) {
            embedded.add(new WorkQueueService.EmbeddedContent(MODEL, config, "batch-" + i, 384));
        }
        workQueue.complete(itemId, embedded);

        Set<String> found = workQueue.findExistingHashes(MODEL, config,
                List.of("batch-1", "batch-499", "never-embedded"));

        assertEquals(Set.of("batch-1", "batch-499"), found);
        assertFalse(found.contains("never-embedded"));
        assertEquals(500L, embeddedContentRepository.countForScope(MODEL, config));
    }

    @Test
    @Transactional
    @DisplayName("an empty probe does no work")
    void emptyProbe() {
        assertTrue(workQueue.findExistingHashes(MODEL, config, List.of()).isEmpty());
        assertTrue(workQueue.findExistingHashes(MODEL, config, null).isEmpty());
    }
}
