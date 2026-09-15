package io.vectorsync.worker.service.derive;

import io.vectorsync.worker.client.DerivationControlClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Answers "does this content already have a durable embedding?" against shared state, with a local
 * cache in front of it.
 *
 * <p>The authority is the control plane's {@code embedded_content} table, not this process. That is
 * the correctness change. This class previously held an exact in-memory set and treated its
 * positives as permanently true on the argument that the embedding store is append-only, so a
 * positive could never go stale. The argument has a hole: it assumes every commit the process
 * believed it made actually survived. When one did not -- a rolled-back append, a dropped table, a
 * warehouse restored from backup -- the set kept reporting a cache hit, inference was skipped, and
 * the content map committed a pointer to a vector that does not exist. Projection coverage is then
 * capped at that unresolved sequence number, so the serving watermark stalls permanently, and
 * nothing in the system could detect or repair it. The set also lived in one heap, so the measured
 * deduplication property held only for a single worker: a hash written by worker A was simply absent
 * from worker B's set.
 *
 * <p>The local cache is now a cache in the strict sense -- it may only be consulted for entries the
 * shared store has confirmed, and it is never the reason a hash is treated as present. Its purpose
 * is to spare a round trip for content this process has already probed, which during a backfill of
 * repetitive data is most of the batch.
 *
 * <p>Negative results are deliberately not cached. A hash absent now can be written by a peer a
 * moment later, and caching that absence would make this process re-embed content that already
 * exists for as long as the entry lived -- wasteful, and invisible.
 */
@Service
@Slf4j
public class ContentHashIndex {

    /**
     * Cap on cached positives per scope. Bounded because this is a convenience, not the authority:
     * exceeding it costs round trips, never correctness.
     */
    @Value("${vectorsync.dedup.cache.max-entries-per-scope:1000000}")
    private int maxEntriesPerScope;

    @Value("${vectorsync.dedup.cache.enabled:true}")
    private boolean cacheEnabled;

    private final DerivationControlClient control;

    /** Confirmed-present hashes per (model_version, config_id). Positives only. */
    private final Map<String, Set<String>> confirmed = new ConcurrentHashMap<>();

    public ContentHashIndex(DerivationControlClient control) {
        this.control = control;
    }

    /**
     * Partitions {@code candidates} into those that already have a durable embedding and those that
     * do not.
     *
     * @param store retained for signature compatibility with the embedding store; unused, because
     *              the store is no longer the thing asked. Probing it per batch was a full scan of
     *              the matching partitions at realistic batch sizes.
     * @return the subset of {@code candidates} known to be embedded
     */
    public Set<String> findExisting(Table store,
                                    Collection<String> candidates,
                                    String modelVersion,
                                    String configId) {
        if (candidates == null || candidates.isEmpty()) {
            return Set.of();
        }

        String scope = scope(modelVersion, configId);
        Set<String> cached = cacheEnabled
                ? confirmed.computeIfAbsent(scope, key -> ConcurrentHashMap.newKeySet())
                : null;

        Set<String> present = new HashSet<>();
        Set<String> unknown = new LinkedHashSet<>();
        for (String candidate : candidates) {
            if (candidate == null || candidate.isBlank()) {
                continue;
            }
            if (cached != null && cached.contains(candidate)) {
                present.add(candidate);
            } else {
                unknown.add(candidate);
            }
        }

        if (unknown.isEmpty()) {
            return present;
        }

        Set<String> fromStore = control.probeEmbedded(modelVersion, configId, unknown);
        if (fromStore == null) {
            // The probe failed. Reporting "nothing is embedded" would re-embed the whole batch and
            // silently multiply cost; reporting the cached subset as if it were the whole answer
            // would be worse. Propagating lets the caller hold the batch and retry.
            throw new IllegalStateException(String.format(
                    "Dedup probe failed for %s / %s; holding the batch rather than re-embedding it",
                    modelVersion, configId));
        }

        present.addAll(fromStore);
        if (cached != null) {
            remember(cached, fromStore, scope);
        }
        return present;
    }

    /**
     * Caches hashes the shared store has just confirmed as present.
     *
     * <p>Only called with a confirmed set. Nothing in this class adds an entry on the strength of a
     * local write, which is the distinction that makes the cache safe: a write this process made and
     * then lost cannot become a permanent false positive.
     */
    public void cacheConfirmed(Collection<String> contentHashes, String modelVersion, String configId) {
        if (!cacheEnabled || contentHashes == null || contentHashes.isEmpty()) {
            return;
        }
        String scope = scope(modelVersion, configId);
        remember(confirmed.computeIfAbsent(scope, key -> ConcurrentHashMap.newKeySet()),
                contentHashes, scope);
    }

    private void remember(Set<String> cached, Collection<String> hashes, String scope) {
        if (cached.size() >= maxEntriesPerScope) {
            return;
        }
        cached.addAll(hashes);
        if (cached.size() > maxEntriesPerScope) {
            log.debug("Dedup cache for {} reached {} entries; further positives are not cached",
                    scope, cached.size());
        }
    }

    /** Drops cached positives. Safe at any time: the authority is elsewhere. */
    public void invalidate() {
        confirmed.clear();
    }

    private static String scope(String modelVersion, String configId) {
        return modelVersion + "" + configId;
    }
}
