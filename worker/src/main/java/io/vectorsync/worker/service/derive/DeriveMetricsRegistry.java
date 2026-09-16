package io.vectorsync.worker.service.derive;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Derivation counters per {@code (source_table, config_id)}, readable over HTTP.
 *
 * <p>These numbers existed only inside log lines, which meant the one property this system claims --
 * that inference tracks distinct content rather than rows -- could be asserted but not queried. A
 * reviewer had to trust a log grep, and nothing could alert on a dedup rate that collapsed because
 * of a silent model-version mismatch.
 *
 * <p>Cumulative per scope rather than per pass, because a table is derived over many passes (one per
 * data file, plus later incremental passes) and the interesting ratio is over all of them. Kept
 * in memory on purpose: these are observations about work this process did, not state anything
 * depends on, so losing them on restart costs visibility and nothing else. Anything that must
 * survive a restart lives in the control plane.
 */
@Service
@Slf4j
public class DeriveMetricsRegistry {

    private final Map<String, Counters> byScope = new ConcurrentHashMap<>();

    private static final class Counters {
        private final AtomicLong rows = new AtomicLong();
        private final AtomicLong chunks = new AtomicLong();
        private final AtomicLong distinctHashes = new AtomicLong();
        private final AtomicLong cacheHits = new AtomicLong();
        private final AtomicLong inferenceCalls = new AtomicLong();
        private final AtomicLong failed = new AtomicLong();
        private final AtomicLong passes = new AtomicLong();
    }

    public void record(String sourceTable, String configId, DeriveResult result) {
        if (sourceTable == null || configId == null || result == null) {
            return;
        }
        Counters counters = byScope.computeIfAbsent(key(sourceTable, configId), k -> new Counters());
        counters.rows.addAndGet(result.rowsProcessed());
        counters.chunks.addAndGet(result.chunksProcessed());
        counters.distinctHashes.addAndGet(result.distinctHashes());
        counters.cacheHits.addAndGet(result.cacheHits());
        counters.inferenceCalls.addAndGet(result.inferenceCalls());
        counters.failed.addAndGet(result.failed());
        counters.passes.incrementAndGet();
    }

    /** @return counters for one scope; zeros when nothing has been derived for it in this process */
    public Map<String, Object> snapshot(String sourceTable, String configId) {
        Counters counters = byScope.get(key(sourceTable, configId));

        Map<String, Object> view = new LinkedHashMap<>();
        view.put("sourceTable", sourceTable);
        view.put("configId", configId);

        long chunks = counters == null ? 0L : counters.chunks.get();
        long inference = counters == null ? 0L : counters.inferenceCalls.get();

        view.put("passes", counters == null ? 0L : counters.passes.get());
        view.put("rowsProcessed", counters == null ? 0L : counters.rows.get());
        view.put("chunksProcessed", chunks);
        view.put("distinctHashes", counters == null ? 0L : counters.distinctHashes.get());
        view.put("cacheHits", counters == null ? 0L : counters.cacheHits.get());
        view.put("inferenceCalls", inference);
        view.put("failed", counters == null ? 0L : counters.failed.get());

        // The honest headline. Saving comes from two places -- content already in the store, and
        // duplicate content within a single batch that collapses before the store is consulted --
        // and only this ratio captures both. Reporting cache hits alone understates a fresh
        // highly-duplicated table to nearly nothing, which is exactly the corpus the design is for.
        view.put("inferenceAvoidedRate", chunks == 0 ? 0.0 : 1.0 - ((double) inference / chunks));
        return view;
    }

    /** Every scope this process has derived, for an overview panel. */
    public Map<String, Map<String, Object>> all() {
        Map<String, Map<String, Object>> out = new LinkedHashMap<>();
        byScope.keySet().forEach(scope -> {
            String[] parts = scope.split("", 2);
            if (parts.length == 2) {
                out.put(scope, snapshot(parts[0], parts[1]));
            }
        });
        return out;
    }

    /** Unit separator, so a table name containing the delimiter cannot forge another scope's key. */
    private static String key(String sourceTable, String configId) {
        return sourceTable + "" + configId;
    }
}
