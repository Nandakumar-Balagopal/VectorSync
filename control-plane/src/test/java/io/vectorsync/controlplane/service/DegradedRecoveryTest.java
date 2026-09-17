package io.vectorsync.controlplane.service;

import io.vectorsync.controlplane.model.MaterializationEntity;
import io.vectorsync.controlplane.model.MaterializationEntity.State;
import io.vectorsync.controlplane.repository.MaterializationRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DEGRADED was a one-way door, and that was arguably worse than the gap it guards.
 *
 * <p>A materialization is parked in DEGRADED when {@code assess()} finds commits an incremental
 * append scan cannot describe -- a delete, an overwrite. Refusing there is correct: advancing the
 * watermark past changes that were never materialized would serve deleted rows indefinitely. But
 * nothing could leave the state again. {@code resume} accepted only PAUSED, and the worker's
 * {@code runnable()} asks for VALIDATED, BACKFILLING and LIVE, so a degraded entry simply stopped
 * and no operator action could restart it.
 *
 * <p>Clearing the state would not have been enough either, which is the property these tests are
 * really about. {@code anchor_snapshot_id} was written once at admission and never advanced, so the
 * next cycle would assess the same range, find the same destructive commit, and degrade again.
 * Recovery has to move the anchor, and {@link #resumeAdvancesTheAnchor} is what pins that -- an
 * implementation that reset the state without re-anchoring passes a naive "is it LIVE again" check
 * and still flickers forever in production.
 *
 * <p>Needs real Postgres for the same reason the dedup tests do, and a real Iceberg catalog because
 * re-anchoring reads the source table's current snapshot rather than trusting a caller to supply one.
 */
@SpringBootTest
@Testcontainers
class DegradedRecoveryTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private static Path warehouse;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws IOException {
        warehouse = Files.createTempDirectory("vectorsync-degraded-");
        registry.add("iceberg.catalog.warehouse", () -> "file://" + warehouse);
    }

    @Autowired
    AdmissionService admission;
    @Autowired
    MaterializationRepository materializationRepository;

    private String sourceTable;
    private String id;

    @BeforeEach
    void scopePerTest(TestInfo info) {
        String name = info.getTestMethod().map(java.lang.reflect.Method::getName).orElse("unknown");
        // Short and unique. config_id is varchar(32) in the schema, so deriving it from the full
        // method name overflows the column; the scope only has to differ between tests.
        String slug = Integer.toHexString(name.hashCode() & 0x7fffffff);
        sourceTable = "default.src_" + slug;
        id = "m-" + slug;
    }

    /**
     * Creates the source table and commits {@code commits} snapshots to it, returning the newest
     * snapshot id. Each append is its own commit, so the table has a real history to re-anchor
     * across rather than a single snapshot that would make the assertion vacuous.
     */
    private long createSource(int commits) throws Exception {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "description", Types.StringType.get()));

        try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(), "file://" + warehouse)) {
            TableIdentifier identifier = TableIdentifier.of(
                    Namespace.of("default"), sourceTable.substring(sourceTable.indexOf('.') + 1));
            if (!catalog.tableExists(identifier)) {
                if (!catalog.namespaceExists(Namespace.of("default"))) {
                    catalog.createNamespace(Namespace.of("default"));
                }
                catalog.createTable(identifier, schema, PartitionSpec.unpartitioned());
            }
            Table table = catalog.loadTable(identifier);
            for (int commit = 0; commit < commits; commit++) {
                // An empty append still produces a snapshot, which is all this test needs: the
                // anchor is a version pointer and re-anchoring is about which version it names.
                table.newAppend().commit();
            }
            table.refresh();
            return table.currentSnapshot().snapshotId();
        }
    }

    /** A LIVE materialization anchored at {@code anchor}, written straight to the repository. */
    private MaterializationEntity liveAt(long anchor, long anchorSequence, long watermark) {
        MaterializationEntity entity = MaterializationEntity.builder()
                .id(id)
                .sourceTable(sourceTable)
                .configId("cfg-" + id)
                .keyColumns("id")
                .embeddingColumns("description")
                .modelName("all-MiniLM-L6-v2")
                .embeddingVersion("v1")
                .chunker("whole")
                .chunkSize(0)
                .chunkOverlap(0)
                .normalize(false)
                .state(State.LIVE)
                .anchorSnapshotId(anchor)
                .anchorSequenceNumber(anchorSequence)
                .incrementalWatermark(watermark)
                .freshnessSlaSeconds(3600)
                .purgeEligible(false)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        return materializationRepository.saveAndFlush(entity);
    }

    @Test
    @DisplayName("a degraded materialization can be resumed at all")
    void degradedCanBeResumed() throws Exception {
        long anchor = createSource(1);
        liveAt(anchor, 1L, 5L);
        admission.markDegraded(id, "incremental scan unsafe: RECONCILE_REQUIRED");

        assertEquals(State.DEGRADED, materializationRepository.findById(id).orElseThrow().getState());

        admission.resume(id);

        MaterializationEntity resumed = materializationRepository.findById(id).orElseThrow();
        assertNotEquals(State.DEGRADED, resumed.getState(),
                "resume left the materialization degraded, so it is still a one-way door");
        assertTrue(resumed.getState().isSchedulable(),
                "resume produced a state the scheduler will not act on: " + resumed.getState());
    }

    @Test
    @DisplayName("resuming a degraded materialization advances the anchor, or it re-degrades forever")
    void resumeAdvancesTheAnchor() throws Exception {
        long oldAnchor = createSource(1);
        liveAt(oldAnchor, 1L, 5L);
        admission.markDegraded(id, "deletes between snapshots require a reconcile");

        // The source moves on. In production this is the destructive commit that degraded it plus
        // whatever followed; here it is enough that the current snapshot is no longer the anchor.
        long newAnchor = createSource(2);
        assertNotEquals(oldAnchor, newAnchor, "the fixture did not actually move the source table");

        admission.resume(id);

        MaterializationEntity resumed = materializationRepository.findById(id).orElseThrow();
        assertEquals(newAnchor, resumed.getAnchorSnapshotId(),
                "the anchor did not move, so the next cycle will assess the same range, find the "
                        + "same destructive commit, and degrade again");
        assertEquals(0L, resumed.getIncrementalWatermark(),
                "the watermark was left in place, so plan() will try to continue an incremental "
                        + "range from an anchor that no longer starts there");
        assertEquals(State.BACKFILLING, resumed.getState());
    }

    @Test
    @DisplayName("the reason for the original refusal survives the resume")
    void resumePreservesWhyItDegraded() throws Exception {
        long anchor = createSource(1);
        liveAt(anchor, 1L, 5L);
        admission.markDegraded(id, "deletes or overwrites between snapshots require a reconcile");
        createSource(1);

        admission.resume(id);

        // Deliberately not cleared. Re-anchoring skips the range that degraded it without
        // tombstoning anything in it, so rows deleted in that window keep serving stale vectors.
        // An operator needs that fact to survive the recovery, not be tidied away by it.
        String error = materializationRepository.findById(id).orElseThrow().getLastError();
        assertTrue(error != null && error.contains("re-anchored"),
                "the resume did not record that a range was skipped: " + error);
        assertTrue(error.contains("reconcile"),
                "the original refusal reason was discarded: " + error);
    }

    @Test
    @DisplayName("resuming a live materialization is still refused")
    void resumeStillRefusesNonRecoverableStates() throws Exception {
        long anchor = createSource(1);
        liveAt(anchor, 1L, 5L);

        // Widening resume to DEGRADED must not turn it into a general-purpose state setter.
        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> admission.resume(id));
        assertTrue(thrown.getMessage().contains("LIVE"), thrown.getMessage());
    }

    @Test
    @DisplayName("re-anchoring refuses when the source table has no snapshot to anchor to")
    void reanchorRefusesWithoutASourceSnapshot() {
        // No source table created at all: the catalog cannot resolve it. Failing loudly beats
        // anchoring to zero, which would be a pointer to nothing that the planner would then treat
        // as a legitimate version.
        liveAt(123L, 1L, 5L);
        admission.markDegraded(id, "degraded");

        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> admission.resume(id));
        assertTrue(thrown.getMessage().contains("re-anchor"), thrown.getMessage());

        assertEquals(State.DEGRADED, materializationRepository.findById(id).orElseThrow().getState(),
                "a failed re-anchor moved the state anyway, so the entry is now in a state whose "
                        + "anchor was never updated");
    }
}
