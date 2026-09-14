package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.derive.EmbeddingStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A lookup index over the content hashes already present in the embedding store.
 *
 * <p>This exists because Iceberg is the wrong tool for the one access pattern the dedup path needs:
 * a point lookup of many keys, once per batch. The store is partitioned by {@code hash_prefix}
 * precisely to prune that lookup, and measurement showed the pruning does not work at realistic
 * batch sizes. Content hashes are uniformly distributed, so a batch of B hashes over P prefix
 * buckets touches {@code P * (1 - (1 - 1/P)^B)} of them -- for B=500 and P=256 that is essentially
 * all 256. Every probe therefore degraded to a full scan of the store, and because the store grows
 * with every batch, total backfill cost became quadratic. Measured on a 100-table run: 9.3s of
 * Iceberg time per table at 5k stored vectors, rising to 28.2s per table at 20k, with inference
 * flat at ~2s throughout. A 500-row file whose content was entirely cached still cost 28.5 seconds.
 *
 * <p>An exact in-memory set fixes it because the question is asymmetric. A hash the index has seen
 * is definitely present -- the store is append-only and immutable, so a positive can never go
 * stale. A hash the index has not seen is definitely absent, but only once the index has been
 * seeded from the store, which is why seeding is a precondition rather than an optimization. With
 * both directions exact, the steady state issues no Iceberg reads at all.
 *
 * <p>The honest limit is memory: a 64-character hex hash costs roughly 120 bytes as a String, so
 * 10 million vectors is around 1.2GB and this stops being viable well before the billions the
 * architecture claims. Past {@link #maxEntries} the index refuses to grow and the caller falls back
 * to probing Iceberg directly -- slow but correct. A production deployment needs a shared key-value
 * store, or a Bloom filter in front of one; this is the single-process form of the same idea, and
 * the ceiling is enforced rather than assumed.
 */
@Service
@Slf4j
public class ContentHashIndex {

    /**
     * Cap on tracked hashes. Beyond this the index degrades to pass-through rather than consuming
     * the heap the index build needs.
     */
    @Value("${vectorsync.dedup.index.max-entries:5000000}")
    private int maxEntries;

    /** One set per (model_version, config_id): a hash is only "present" under a given derivation. */
    private final Map<String, Set<String>> byScope = new ConcurrentHashMap<>();
    private final Set<String> saturated = ConcurrentHashMap.newKeySet();

    /**
     * Partitions {@code candidates} into hashes already embedded and hashes that must be embedded.
     *
     * @return the subset of {@code candidates} already present in the store
     */
    public Set<String> findExisting(Table store,
                                    Collection<String> candidates,
                                    String modelVersion,
                                    String configId) {
        if (store == null || candidates == null || candidates.isEmpty()) {
            return Set.of();
        }

        String scope = scope(modelVersion, configId);
        if (saturated.contains(scope)) {
            return EmbeddingStore.findExistingHashes(store, candidates, modelVersion, configId);
        }

        Set<String> known = byScope.computeIfAbsent(scope, key -> seed(store, modelVersion, configId));
        if (known == null) {
            // Seeding failed or overflowed; correctness requires falling back to the store.
            saturated.add(scope);
            byScope.remove(scope);
            return EmbeddingStore.findExistingHashes(store, candidates, modelVersion, configId);
        }

        Set<String> present = new HashSet<>();
        for (String candidate : candidates) {
            if (candidate != null && known.contains(candidate)) {
                present.add(candidate);
            }
        }
        return present;
    }

    /**
     * Records hashes that have just been written to the store.
     *
     * <p>Must be called after a successful append and never before. Recording a hash that failed to
     * commit would make the index claim a vector exists when it does not, and every future row
     * carrying that content would map to nothing -- a silent, permanent hole in the projection.
     */
    public void recordWritten(Collection<String> contentHashes, String modelVersion, String configId) {
        if (contentHashes == null || contentHashes.isEmpty()) {
            return;
        }
        String scope = scope(modelVersion, configId);
        if (saturated.contains(scope)) {
            return;
        }

        Set<String> known = byScope.get(scope);
        if (known == null) {
            return;
        }

        known.addAll(contentHashes);
        if (known.size() > maxEntries) {
            log.warn("Dedup index for {} exceeded {} entries; falling back to store probes",
                    scope, maxEntries);
            saturated.add(scope);
            byScope.remove(scope);
        }
    }

    /** Drops cached state. For tests and for an operator who has rebuilt the store underneath us. */
    public void invalidate() {
        byScope.clear();
        saturated.clear();
    }

    /**
     * Loads every content hash for one scope, projecting the single column.
     *
     * <p>One full scan of one partition set, once per process per scope, replacing one scan per
     * batch. Returns null when the scope is too large to track, which the caller treats as a
     * permanent fallback rather than retrying the scan on every batch.
     */
    private Set<String> seed(Table store, String modelVersion, String configId) {
        long startedAt = System.currentTimeMillis();
        Set<String> hashes = ConcurrentHashMap.newKeySet();

        try (CloseableIterable<Record> rows = IcebergGenerics.read(store)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .select(Constants.CONTENT_HASH_COLUMN, Constants.CONFIG_ID_COLUMN)
                .build()) {

            for (Record row : rows) {
                Object hash = row.getField(Constants.CONTENT_HASH_COLUMN);
                if (hash != null) {
                    hashes.add(String.valueOf(hash));
                }
                if (hashes.size() > maxEntries) {
                    log.warn("Embedding store scope {} holds more than {} hashes; dedup index "
                            + "disabled for it", scope(modelVersion, configId), maxEntries);
                    return null;
                }
            }
        } catch (Exception e) {
            // Loud and non-caching: silently treating a failed seed as "store is empty" would
            // re-embed everything, which is the exact cost this class exists to remove.
            log.error("Could not seed dedup index for {} / {}: {}", modelVersion, configId,
                    e.getMessage());
            return null;
        }

        log.info("Seeded dedup index for {} / {} with {} hashes in {}ms",
                modelVersion, configId, hashes.size(), System.currentTimeMillis() - startedAt);
        return hashes;
    }

    private static String scope(String modelVersion, String configId) {
        return modelVersion + "" + configId;
    }
}
