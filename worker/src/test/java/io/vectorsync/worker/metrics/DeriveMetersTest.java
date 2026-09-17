package io.vectorsync.worker.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.vectorsync.worker.service.derive.DeriveMetricsRegistry;
import io.vectorsync.worker.service.derive.DeriveResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two decisions about the derivation meters that are easy to reverse by accident, and costly to.
 *
 * <p>The first is that the headline number is published as two counters rather than as one ratio
 * gauge. A gauge would be the obvious choice -- the interesting figure is the fraction of chunks
 * that avoided inference -- and it would be wrong twice over: averaging two workers' ratios is not
 * the fleet ratio, so it does not aggregate, and it freezes at its last value when a worker goes
 * idle, which reads as a healthy rate for a materialization that has actually stopped. Counters let
 * the consumer divide over whatever window it chooses.
 *
 * <p>The second is that tag cardinality is capped. {@code source_table} and {@code config_id} are
 * both user-supplied and unbounded, and {@code config_id} is a hash, so every chunker or
 * model-revision change mints a value that will never recur. A counter is never unregistered, so
 * without a cap one benchmark sweep leaves a permanent series per table. That failure lands on the
 * operator's metrics backend, which is precisely why it has to be bounded here.
 */
@SpringBootTest(properties = {
        "embedding.provider=mock",
        "iceberg.vector.namespace=vector",
        "vectorsync.runner.enabled=false",
        "vectorsync.legacy-sync.enabled=false",
        // Small enough to cross inside a test without registering thousands of meters.
        "vectorsync.metrics.max-tag-values=3",
})
class DeriveMetersTest {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws IOException {
        Path warehouse = Files.createTempDirectory("vectorsync-meters-");
        registry.add("iceberg.catalog.warehouse", () -> "file://" + warehouse);
    }

    @Autowired
    DeriveMetricsRegistry metrics;
    @Autowired
    MeterRegistry meters;

    /** Not exercised here; mocked so the context starts without a control plane. */
    @MockitoBean
    io.vectorsync.worker.client.DerivationControlClient control;

    private static DeriveResult pass(int chunks, int inference, int storeHits) {
        return new DeriveResult(chunks, chunks, chunks, storeHits, inference, 0, List.of());
    }

    private double counter(String name, String table, String config) {
        io.micrometer.core.instrument.Counter found = meters.find(name)
                .tag("source_table", table)
                .tag("config_id", config)
                .counter();
        assertNotNull(found, "no counter registered for " + name);
        return found.count();
    }

    @Test
    @DisplayName("inference saving is published as counters, never as a ratio gauge")
    void savingIsCountersNotARatio() {
        metrics.record("default.meters_a", "cfg-a", pass(100, 40, 25));

        assertEquals(100.0, counter("vectorsync.inference.chunks", "default.meters_a", "cfg-a"));
        assertEquals(40.0, counter("vectorsync.inference.calls", "default.meters_a", "cfg-a"));
        // chunks - calls: everything that needed no model call, whether it hit the store or was
        // collapsed within the batch before the probe.
        assertEquals(60.0, counter("vectorsync.inference.avoided", "default.meters_a", "cfg-a"));
        // Kept separate on purpose. Collapsing the two sources of saving is how the earlier
        // dedupRate came to understate a freshly duplicated table to nearly zero.
        assertEquals(25.0, counter("vectorsync.inference.store.hits", "default.meters_a", "cfg-a"));

        assertNull(meters.find("vectorsync.inference.avoided.rate").meter(),
                "a ratio gauge was registered; it does not aggregate across workers and freezes "
                        + "at its last value when a worker goes idle");
        assertNull(meters.find("vectorsync.inference.rate").meter(), "a ratio gauge was registered");
    }

    @Test
    @DisplayName("counters accumulate across passes rather than reporting the last one")
    void countersAccumulate() {
        metrics.record("default.meters_b", "cfg-b", pass(10, 10, 0));
        metrics.record("default.meters_b", "cfg-b", pass(10, 0, 10));

        assertEquals(20.0, counter("vectorsync.inference.chunks", "default.meters_b", "cfg-b"));
        assertEquals(10.0, counter("vectorsync.inference.calls", "default.meters_b", "cfg-b"));
        assertEquals(10.0, counter("vectorsync.inference.avoided", "default.meters_b", "cfg-b"));
        assertEquals(2.0, counter("vectorsync.derive.passes", "default.meters_b", "cfg-b"));
    }

    @Test
    @DisplayName("tag cardinality is capped, so a benchmark sweep cannot mint unbounded series")
    void cardinalityIsCapped() {
        // 50 distinct source tables, which is what a bench sweep looks like. Without the cap this
        // leaves 50 permanent series on every vectorsync meter, because a counter is never
        // unregistered once created.
        for (int table = 0; table < 50; table++) {
            metrics.record("default.sweep_" + table, "cfg-sweep-" + table, pass(5, 5, 0));
        }

        // Counted across the whole meter name and not filtered to this test's tags, deliberately.
        // MeterFilter.maximumAllowableTags bounds distinct tag values per meter name for the
        // lifetime of the registry, and tests share one Spring context -- so an assertion scoped to
        // one test's own tag values depends on which tests ran first. An earlier version of this
        // test asserted "at least one of my series exists" and failed for exactly that reason: the
        // three tables used by the tests above had already consumed a budget of 3.
        long series = meters.find("vectorsync.inference.chunks").counters().size();

        assertTrue(series <= 3,
                "the cardinality cap did not hold: " + series + " series on one meter name, so an "
                        + "unbounded tag would accumulate a permanent series per table and the "
                        + "failure would land on the operator's metrics backend");
    }

    @Test
    @DisplayName("the existing HTTP snapshot keeps working, because bench scripts read it")
    void httpSnapshotIsUnchanged() {
        metrics.record("default.meters_c", "cfg-c", pass(50, 20, 10));

        // bench/demo.py reads these keys. Bridging to Micrometer must add a channel, not replace
        // one, or the benchmarks that produced every number in the docs stop working.
        var snapshot = metrics.snapshot("default.meters_c", "cfg-c");
        assertTrue(snapshot.containsKey("inferenceCalls"), snapshot.keySet().toString());
        assertTrue(snapshot.containsKey("inferenceAvoidedRate"), snapshot.keySet().toString());
        assertEquals(20L, ((Number) snapshot.get("inferenceCalls")).longValue());
    }
}
