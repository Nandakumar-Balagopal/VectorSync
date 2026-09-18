package io.vectorsync.worker.service.derive;

import io.vectorsync.common.dto.TableConfig;
import io.vectorsync.format.catalog.IcebergCatalogConfig;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.worker.client.DerivationControlClient;
import io.vectorsync.worker.client.DerivationControlClient.Materialization;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import io.vectorsync.worker.service.iceberg.IncrementalChangeDetector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * The derivation cycle's concurrency, and the two ways it can be wrong.
 *
 * <p>Deliberately a plain Mockito test rather than a {@code @SpringBootTest}: one of the properties
 * under test is that the context <em>refuses to start</em> in a particular configuration, which a
 * running context cannot assert, and the collaborators here are all cheap to stub.
 *
 * <p>The first hazard is configuration. Concurrency turns a single-writer deployment into a
 * multi-writer one, and the shipped catalog type is {@code hadoop}, which has no atomic commit: two
 * concurrent commits can both succeed with one silently lost -- data loss with no exception and no
 * way to detect it afterwards. So raising parallelism on Hadoop has to fail loudly at startup, and
 * the default has to keep starting.
 *
 * <p>The second is the grouping key. {@code MaterializationSpec.configId} deliberately excludes the
 * source table, so several materializations routinely share one, and two that share one address the
 * same Tier-1 content space. Deriving them at once means both probe the dedup record before either
 * has written, both miss, and both pay for the same inference -- breaking the one-row-per-content
 * invariant and falsifying the equal-inference claim {@link ReproducibilityTest} makes. Grouping by
 * materialization id instead of by configId would look correct and quietly double the bill.
 */
class ConcurrentCycleTest {

    private DerivationControlClient control;
    private IncrementalChangeDetector detector;
    private DeriveService deriveService;
    private IcebergCatalogService catalogService;
    private IcebergCatalogConfig catalogConfig;
    private ContentHashIndex hashIndex;
    private DeriveMetricsRegistry metrics;
    private ReconcileService reconcileService;

    @BeforeEach
    void stubCollaborators() {
        control = Mockito.mock(DerivationControlClient.class);
        detector = Mockito.mock(IncrementalChangeDetector.class);
        deriveService = Mockito.mock(DeriveService.class);
        catalogService = Mockito.mock(IcebergCatalogService.class);
        catalogConfig = Mockito.mock(IcebergCatalogConfig.class);
        hashIndex = Mockito.mock(ContentHashIndex.class);
        metrics = Mockito.mock(DeriveMetricsRegistry.class);
        reconcileService = Mockito.mock(ReconcileService.class);
        when(catalogService.config()).thenReturn(catalogConfig);
    }

    private MaterializationRunner runner(int parallelism, String catalogType) {
        when(catalogConfig.type()).thenReturn(catalogType);
        MaterializationRunner runner = new MaterializationRunner(
                control, detector, deriveService, catalogService, hashIndex, metrics,
                reconcileService);
        ReflectionTestUtils.setField(runner, "parallelism", parallelism);
        ReflectionTestUtils.setField(runner, "configuredOwner", "test-owner");
        ReflectionTestUtils.setField(runner, "leaseBatch", 8);
        ReflectionTestUtils.setField(runner, "leaseSeconds", 900L);
        ReflectionTestUtils.setField(runner, "maxBatchesPerCycle", 32);
        ReflectionTestUtils.setField(runner, "vectorNamespace", "vector");
        ReflectionTestUtils.setField(runner, "publishProjection", false);
        ReflectionTestUtils.setField(runner, "reconcileEnabled", false);
        return runner;
    }

    private static MaterializationSpec spec(String sourceTable, String embeddingColumn) {
        return MaterializationSpec.builder()
                .sourceTable(sourceTable)
                .keyColumns(List.of("id"))
                .embeddingColumns(List.of(embeddingColumn))
                .joinSeparator(" ")
                .chunker("whole")
                .modelName("all-MiniLM-L6-v2")
                .modelRevision("concurrency")
                .embeddingVersion("v1")
                .build();
    }

    /** LIVE with a watermark, so plan() takes the incremental branch and drain() leases nothing. */
    private static Materialization live(String id, MaterializationSpec spec) {
        return Materialization.builder()
                .id(id)
                .state("LIVE")
                .sourceTable(spec.getSourceTable())
                .configId(spec.configId())
                .anchorSnapshotId(100L)
                .anchorSequenceNumber(1L)
                .incrementalWatermark(1L)
                .spec(spec)
                .build();
    }

    @Test
    @DisplayName("the shipped default starts on a hadoop catalog")
    void defaultParallelismStartsOnHadoop() {
        // The regression guard that matters most: parallelism defaults to 1 and the shipped catalog
        // type is hadoop, so if the refusal were keyed on the catalog alone rather than on the
        // combination, every default deployment would fail to boot.
        MaterializationRunner runner = runner(1, "hadoop");
        runner.startUp();
        assertEquals("test-owner", ReflectionTestUtils.getField(runner, "owner"));
    }

    @Test
    @DisplayName("parallelism above one refuses to start on a hadoop catalog")
    void refusesConcurrencyOnHadoop() {
        MaterializationRunner runner = runner(2, "hadoop");

        IllegalStateException thrown = assertThrows(IllegalStateException.class, runner::startUp);
        assertTrue(thrown.getMessage().contains("hadoop"),
                "the refusal must name the catalog type: " + thrown.getMessage());
        assertTrue(thrown.getMessage().contains("parallelism"),
                "the refusal must name the property to change: " + thrown.getMessage());
    }

    @Test
    @DisplayName("parallelism above one is allowed on a catalog with atomic commits")
    void allowsConcurrencyOnJdbc() {
        MaterializationRunner runner = runner(4, "jdbc");
        runner.startUp();
        assertEquals("test-owner", ReflectionTestUtils.getField(runner, "owner"));
    }

    @Test
    @DisplayName("materializations sharing a config id are never derived concurrently")
    void oneConfigIdAtATime() throws Exception {
        // Three materializations over three different source tables, two of which share an
        // embedding configuration. configId excludes the source table, so those two share a
        // configId -- which is exactly the case that must serialize.
        MaterializationSpec sharedA = spec("default.products", "description");
        MaterializationSpec sharedB = spec("default.tickets", "description");
        MaterializationSpec separate = spec("default.articles", "body");
        assertEquals(sharedA.configId(), sharedB.configId(),
                "the fixture is wrong: these two must share a config id");
        assertTrue(!separate.configId().equals(sharedA.configId()),
                "the fixture is wrong: this one must have its own config id");

        when(control.runnable()).thenReturn(List.of(
                live("m-a", sharedA), live("m-b", sharedB), live("m-c", separate)));

        // Records how many threads are inside plan() for a given configId at once, by making the
        // first thing plan() calls slow enough for an overlap to be observable.
        Map<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();
        Map<String, Integer> peak = new ConcurrentHashMap<>();
        Set<String> threads = ConcurrentHashMap.newKeySet();

        when(detector.currentVersion(any(TableConfig.class))).thenAnswer(invocation -> {
            TableConfig config = invocation.getArgument(0);
            String key = configKeyFor(config.getTableName(), sharedA, sharedB, separate);
            threads.add(Thread.currentThread().getName());
            int now = inFlight.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            peak.merge(key, now, Math::max);
            try {
                Thread.sleep(150);
            } finally {
                inFlight.get(key).decrementAndGet();
            }
            // Empty version, so plan() returns 0 and drain() leases nothing: the cycle does no real
            // work and the only thing measured is the shape of the concurrency.
            return new IncrementalChangeDetector.SourceVersion(0L, 0L);
        });

        MaterializationRunner runner = runner(4, "jdbc");
        runner.startUp();
        runner.runCycle();
        runner.shutDown();

        assertEquals(1, peak.getOrDefault(sharedA.configId(), 0),
                "two materializations sharing a config id overlapped, so each would probe the dedup "
                        + "record before the other had written and both would pay for the same "
                        + "inference. The grouping key is wrong -- it must be configId, not the "
                        + "materialization id.");
        assertTrue(threads.size() > 1,
                "everything ran on one thread, so the parallelism knob is not wired: " + threads);
    }

    private static String configKeyFor(String tableName, MaterializationSpec... specs) {
        for (MaterializationSpec spec : specs) {
            if (spec.getSourceTable().equals(tableName)) {
                return spec.configId();
            }
        }
        return "unknown";
    }

    @Test
    @DisplayName("a materialization that throws does not stop its peers or void the cycle report")
    void oneFailureDoesNotStarveTheRest() {
        MaterializationSpec first = spec("default.broken", "description");
        MaterializationSpec second = spec("default.fine", "body");

        when(control.runnable()).thenReturn(List.of(live("m-1", first), live("m-2", second)));
        when(detector.currentVersion(any(TableConfig.class))).thenAnswer(invocation -> {
            TableConfig config = invocation.getArgument(0);
            if ("default.broken".equals(config.getTableName())) {
                throw new IllegalStateException("planned failure");
            }
            return new IncrementalChangeDetector.SourceVersion(0L, 0L);
        });

        MaterializationRunner runner = runner(2, "jdbc");
        runner.startUp();
        MaterializationRunner.CycleReport report = runner.runCycle();
        runner.shutDown();

        // Both are still counted as seen: the report is built from the runnable list, so a thrown
        // materialization cannot make the cycle look smaller than it was.
        assertEquals(2, report.materializationsSeen(),
                "a failing materialization was dropped from the cycle report");
    }

    @Test
    @DisplayName("a reconcile reports its processed work, so the watermark can advance")
    void reconcileReportsItsProcessedWork() {
        // The bug this guards was silent and observable only in production logs: reconcile() drains
        // its own queue inside plan(), so the drain() that follows found nothing, publish()
        // early-returned on processedThisCycle == 0 for a LIVE materialization, the watermark never
        // moved, and the next cycle re-assessed the identical range. Observed as the same
        // "206 of 495 rows retired" on consecutive passes, forever.
        //
        // The cycle report is the observable proxy: a reconcile that processed files must surface a
        // non-zero filesProcessed, because that is the value publish() keys on.
        MaterializationSpec spec = spec("default.reconciled", "description");
        when(control.runnable()).thenReturn(List.of(live("m-rec", spec)));

        // A source that moved, with an assessment demanding a reconcile over one known partition.
        when(detector.currentVersion(any(TableConfig.class)))
                .thenReturn(new IncrementalChangeDetector.SourceVersion(999L, 9L));
        when(detector.assess(any(TableConfig.class), anyLong(), anyLong()))
                .thenReturn(new IncrementalChangeDetector.ScanAssessment(
                        IncrementalChangeDetector.ScanVerdict.RECONCILE_REQUIRED,
                        "deletes between snapshots", List.of(999L), Set.of("day=2026-09-18")));
        when(detector.backfillWork(any(TableConfig.class), anyLong(), any()))
                .thenReturn(List.of());
        when(control.enqueue(anyString(), any(), anyString())).thenReturn(1);
        when(control.lease(anyString(), anyInt(), anyLong(), anyString()))
                .thenReturn(List.of());
        when(reconcileService.sweep(any(), any(), anyLong(), anyLong(), anyLong()))
                .thenReturn(new ReconcileService.SweepResult(10, 12, 2, true, "2 rows tombstoned"));

        MaterializationRunner runner = runner(1, "jdbc");
        ReflectionTestUtils.setField(runner, "reconcileEnabled", true);
        runner.startUp();
        MaterializationRunner.CycleReport report = runner.runCycle();
        runner.shutDown();

        assertTrue(report.filesProcessed() > 0,
                "the reconcile's work was not reported, so publish() sees zero processed and the "
                        + "watermark will never advance -- the reconcile then repeats every cycle "
                        + "against the same range");
    }

    @Test
    @DisplayName("a reconcile scopes its re-derive to the affected partitions")
    void reconcileScopesToAffectedPartitions() {
        MaterializationSpec spec = spec("default.scoped", "description");
        when(control.runnable()).thenReturn(List.of(live("m-scope", spec)));
        when(detector.currentVersion(any(TableConfig.class)))
                .thenReturn(new IncrementalChangeDetector.SourceVersion(1000L, 10L));
        when(detector.assess(any(TableConfig.class), anyLong(), anyLong()))
                .thenReturn(new IncrementalChangeDetector.ScanAssessment(
                        IncrementalChangeDetector.ScanVerdict.RECONCILE_REQUIRED,
                        "overwrite", List.of(1000L), Set.of("day=2026-09-18", "day=2026-09-17")));
        when(detector.backfillWork(any(TableConfig.class), anyLong(), any()))
                .thenReturn(List.of());
        when(control.enqueue(anyString(), any(), anyString())).thenReturn(0);
        when(control.lease(anyString(), anyInt(), anyLong(), anyString())).thenReturn(List.of());
        when(reconcileService.sweep(any(), any(), anyLong(), anyLong(), anyLong()))
                .thenReturn(new ReconcileService.SweepResult(5, 5, 0, true, "nothing vanished"));

        MaterializationRunner runner = runner(1, "jdbc");
        ReflectionTestUtils.setField(runner, "reconcileEnabled", true);
        runner.startUp();
        runner.runCycle();
        runner.shutDown();

        // The scoped overload, not the whole-table one. A reconcile that re-derives the entire
        // source on every mutation is correct and proportional to the table rather than the change,
        // which is what makes a rapidly-changing table unaffordable.
        Mockito.verify(detector).backfillWork(any(TableConfig.class), anyLong(), any());
        Mockito.verify(detector, Mockito.never()).backfillWork(any(TableConfig.class), anyLong());
    }
}
