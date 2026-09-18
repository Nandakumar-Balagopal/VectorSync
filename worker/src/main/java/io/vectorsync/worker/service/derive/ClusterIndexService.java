package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.derive.ClusteredIndex;
import io.vectorsync.format.derive.ContentMap;
import io.vectorsync.format.derive.ContentMapEntry;
import io.vectorsync.format.derive.ContentHash;
import io.vectorsync.format.derive.EmbeddingStore;
import io.vectorsync.format.derive.VectorClustering;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds and probes a cluster-partitioned serving table.
 *
 * <p>Exists to test one claim: that a query engine's partition pruning can stand in for an ANN index
 * while no engine can read an Iceberg-native one. Building writes every canonical vector into a
 * table partitioned by its nearest centroid; probing ranks centroids, turns the closest few into a
 * partition predicate, and scores exactly within what that reads.
 *
 * <p>Deliberately reads canonical vectors rather than the row projection. Clustering the same
 * content once per row would multiply the index by the duplication factor and make each probe read
 * mostly repeats of the same vector, which is the opposite of the reduction being attempted.
 */
@Service
@Slf4j
public class ClusterIndexService {

    /** Iterations {@link VectorClustering#fit} is allowed before it stops refining. */
    private static final int MAX_FIT_ROUNDS = 25;

    /**
     * Distinct content hashes {@link #countCanonical} will hold before giving up on an exact answer.
     *
     * <p>A freshness check must not be able to exhaust the heap of a running worker to answer an
     * operator's status call. At 64-character hashes this bounds the set to roughly 200 MB, past
     * which the honest report is {@link Freshness#UNKNOWN} rather than a number.
     */
    private static final int MAX_COUNTED_HASHES = 2_000_000;

    /**
     * How much a scope may grow, as a fraction of what it already holds, before a refit is preferred
     * to appending against the existing centroids.
     *
     * <p>The one judgement call in the incremental path. Each append assigns against centroids
     * fitted over older content, so the assignment drifts from what a fresh fit would give; that
     * costs recall rather than correctness, because whatever is read is still scored exactly, but it
     * compounds. 0.25 keeps a scope within one refit of its data while making ordinary appends free.
     */
    @Value("${vectorsync.cluster.max-incremental-fraction:0.25}")
    private double maxIncrementalFraction;

    private final IcebergCatalogService catalogService;

    /**
     * Centroids and scope size per scope, cached for the life of the index.
     *
     * <p>Both were read from Iceberg on every query. Centroids are a table scan, and neither value
     * changes until a rebuild -- so the query path was paying catalog load and scan planning twice
     * over for constants. Cleared by {@link #build}, which is the only thing that can invalidate them.
     */
    private final java.util.Map<String, List<float[]>> centroidCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<String, Long> scopeSizeCache = new java.util.concurrent.ConcurrentHashMap<>();

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public ClusterIndexService(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    /**
     * @param reused true when the recorded coverage digest already matched, so no clustering ran and
     *               nothing was committed. This is the measurable output of content-addressed index
     *               identity: after a compaction or any other layout-only rewrite it is true, and
     *               the rebuild cost is zero.
     * @param coverageDigest the digest this scope's index covers, whether just written or matched
     * @param incremental true when only the new content was appended, assigned against the centroids
     *                    already on disk, with nothing relabelled and no refit
     */
    public record BuildReport(String sourceTable,
                              String modelVersion,
                              String configId,
                              int clusters,
                              int iterations,
                              long vectors,
                              long canonicalRowsRead,
                              List<Integer> clusterSizes,
                              boolean reused,
                              String coverageDigest,
                              boolean incremental) {
    }

    /** How the clustered index stands against the canonical vectors it was built from. */
    public enum Freshness {
        /** The index holds exactly the scope's canonical content. */
        FRESH,
        /** Tier 1 has content the index does not. A probe cannot return it. */
        BEHIND,
        /** The comparison could not be made exactly; see {@link ScopeStatus#note()}. */
        UNKNOWN
    }

    /**
     * @param indexedVectors   rows the clustered table holds for this scope
     * @param canonicalVectors vectors Tier 1 holds for this scope, when that can be counted exactly
     * @param centroids        centroid rows on disk for this scope; {@code k}, or the index is not
     *                         built. More than {@code k} would mean two fitted generations coexist
     */
    public record ScopeStatus(String sourceTable,
                              String modelVersion,
                              String configId,
                              long indexedVectors,
                              long canonicalVectors,
                              int centroids,
                              Freshness freshness,
                              String note) {
    }

    /**
     * Fits centroids over a scope's canonical vectors and replaces the clustered table's slice for
     * that scope.
     *
     * <p>Three outcomes, cheapest first, and which one applies is decided from the content rather
     * than from the caller's intent:
     *
     * <ol>
     *   <li><b>Reuse.</b> The recorded coverage digest matches, so the content this scope holds is
     *       unchanged and the index on disk is provably still correct. Nothing is fitted and nothing
     *       is committed. This is the case a compaction, a data-file rewrite or a sort
     *       reorganisation produces -- all of them make a new Iceberg snapshot and change no
     *       content.
     *   <li><b>Incremental append.</b> Content was added and none removed, within a bounded fraction
     *       of the scope. The new content is assigned against the centroids already on disk and
     *       appended; nothing is relabelled. See {@link #tryIncrement} for why appending is sound
     *       here and not for a refit.
     *   <li><b>Refit.</b> Everything else -- a removal, a scope with no centroids, or growth past
     *       the incremental fraction. Refitting relabels every vector, so the scope is replaced
     *       rather than appended to.
     * </ol>
     *
     * <p>The refit path is the honest cost of this layer and is not hidden: an incremental append
     * assigns against centroids fitted over older content, so its assignment drifts from what a
     * fresh fit would give. That costs recall and not correctness, because whatever the probe reads
     * is still scored exactly.
     *
     * <p>Two commits land, the data and then the centroids, and there is no atomicity across them:
     * Iceberg commits one table at a time. Data first is the better of the two orders, because it
     * means the published centroids always describe an assignment that is already on disk. A failure
     * between them leaves the previous centroids ranking a new assignment, which costs recall -- the
     * candidates that are read are still scored exactly -- rather than pointing the probe at
     * partitions that hold no files.
     */
    /**
     * Clusters to fit for a corpus of {@code contentCount} distinct vectors.
     *
     * <p>The standard IVF sizing heuristic, {@code sqrt(n)}, because a fixed count cannot be right
     * across corpus sizes and being wrong is expensive rather than merely suboptimal: NFCorpus at 16
     * clusters needed 53% of the table for the same retrieval quality it reached at 3.8% with 64.
     * An index that reads half the table prunes nothing while still costing a refit to maintain, so
     * a scheduler using one fixed number would maintain an index worth less than no index.
     *
     * <p>Encouragingly the heuristic agrees with the measurement -- sqrt(3593) is 60, against the 64
     * that measured well -- though one corpus agreeing is not evidence that it generalises, and the
     * cluster-count sensitivity recorded in docs/DEMO.md still applies.
     *
     * <p>Floored at 2 because one cluster is a full scan with extra steps, and capped so a very
     * large corpus cannot produce more partitions than a catalog wants to track.
     */
    /**
     * Distinct content Tier 1 holds for a scope, or 0 when there is none.
     *
     * <p>Exposed for the scheduler, which has to size the cluster count from the corpus and has to
     * know whether there is a corpus at all -- building over nothing would commit an empty scope and
     * record coverage for it, which the next tick would read as up to date.
     */
    public long canonicalCount(String modelVersion, String configId) {
        Table store = EmbeddingStore.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (store == null) {
            return 0;
        }
        return countCanonical(store, modelVersion, configId).count();
    }

    public static int suggestedClusters(long contentCount) {
        if (contentCount <= 4) {
            return 1;
        }
        return (int) Math.max(2, Math.min(4096, Math.round(Math.sqrt((double) contentCount))));
    }

    public BuildReport build(String sourceTable, String modelVersion, String configId, int clusters) {
        Table store = EmbeddingStore.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (store == null) {
            throw new IllegalStateException("No embedding store; nothing has been derived yet");
        }

        Canonical canonical = readCanonical(store, sourceTable, modelVersion, configId);
        if (canonical.vectors().isEmpty()) {
            // Refusing beats committing: replaceScope with no entries would delete a scope that is
            // serving, and an empty read here means Tier 1 has not caught up or the scope is
            // misspelled, not that the content disappeared.
            throw new IllegalStateException(String.format(
                    "Refusing to empty the clustered index for %s / %s: Tier 1 returned no vectors "
                            + "for that scope. Run the embedding pass for it before rebuilding.",
                    modelVersion, configId));
        }

        // A stable order makes the strided initialisation reproducible, so rebuilding over unchanged
        // data produces the same assignment and the recall figures stay comparable.
        List<String> hashes = new ArrayList<>(canonical.vectors().keySet());
        hashes.sort(Comparator.naturalOrder());

        // Identity by coverage, not by source version. The digest is over the content this scope
        // holds, so a source snapshot that only moved bytes -- a compaction, a data-file rewrite, a
        // sort reorganisation, a partition rewrite -- yields the same digest and the index already
        // on disk is provably still correct for it. That is the case this skip exists for: those
        // operations are routine scheduled maintenance in a lakehouse, they change no embedding, and
        // an index keyed by snapshot id would rebuild in full for every one of them.
        //
        // Note what is NOT claimed here: this proves the covered content set is unchanged, not that
        // the assignment is optimal. Centroids fitted over the same content are unchanged too, since
        // fit() is deterministic over a sorted input, so skipping is exact rather than approximate.
        String coverageDigest = ContentHash.coverageDigest(hashes);
        Table coverageTable = ClusteredIndex.loadCoverageOrCreate(
                catalogService.getCatalog(), vectorNamespace);
        Optional<ClusteredIndex.Coverage> recorded =
                ClusteredIndex.readCoverage(coverageTable, modelVersion, configId);

        if (recorded.isPresent() && recorded.get().digest().equals(coverageDigest)
                && indexHasRows(sourceTable, modelVersion, configId)) {
            // The rows check is not redundant. Coverage is committed after the data it describes, so
            // a coverage row without rows should be impossible -- but a purge, a manual drop or a
            // failed migration can produce one, and treating that as "up to date" would serve an
            // empty index forever with no error.
            log.info("Index for {} / {} already covers this content ({}, {} contents); skipping rebuild",
                    modelVersion, configId, coverageDigest, recorded.get().contentCount());
            return new BuildReport(sourceTable, modelVersion, configId, 0, 0,
                    (int) recorded.get().contentCount(), canonical.rowsRead(), List.of(), true,
                    coverageDigest, false);
        }

        List<float[]> ordered = new ArrayList<>(hashes.size());
        for (String hash : hashes) {
            ordered.add(canonical.vectors().get(hash));
        }

        Table clusteredTable = ClusteredIndex.loadOrCreate(
                catalogService.getCatalog(), vectorNamespace, sourceTable);

        BuildReport incremental = tryIncrement(clusteredTable, coverageTable, sourceTable,
                modelVersion, configId, canonical, hashes, coverageDigest,
                recorded.flatMap(ClusteredIndex.Coverage::refitContentCount));
        if (incremental != null) {
            return incremental;
        }

        VectorClustering.Model model = VectorClustering.fit(ordered, clusters, MAX_FIT_ROUNDS);

        int[] sizes = new int[model.clusterCount()];
        List<ClusteredIndex.Entry> entries = new ArrayList<>(hashes.size());
        for (int position = 0; position < hashes.size(); position++) {
            String hash = hashes.get(position);
            float[] embedding = ordered.get(position);
            int cluster = model.assign(embedding);
            sizes[cluster]++;
            entries.add(new ClusteredIndex.Entry(hash, cluster, modelVersion, configId,
                    canonical.dimension(), embedding, canonical.texts().get(hash)));
        }

        ClusteredIndex.replaceScope(clusteredTable, modelVersion, configId, entries);

        Table centroidTable = ClusteredIndex.loadCentroidsOrCreate(
                catalogService.getCatalog(), vectorNamespace);
        ClusteredIndex.replaceCentroids(centroidTable, modelVersion, configId, model.centroids());

        // Last, deliberately. A coverage row that ran ahead of the rows and centroids it describes
        // would make the next build skip against an index that was never finished -- the same
        // ordering rule the derive path follows when it appends vectors before the pointers to them.
        // A refit re-establishes the baseline: the centroids now describe exactly this content.
        ClusteredIndex.replaceCoverage(coverageTable, modelVersion, configId, coverageDigest,
                hashes.size(), hashes.size());

        List<Integer> clusterSizes = new ArrayList<>(sizes.length);
        for (int size : sizes) {
            clusterSizes.add(size);
        }

        String scope = scopeKey(modelVersion, configId);
        centroidCache.remove(scope);
        scopeSizeCache.remove(scope);

        log.info("Clustered {} distinct vectors ({} canonical rows read) for {} / {} into {} clusters in {} iterations",
                entries.size(), canonical.rowsRead(), modelVersion, configId,
                model.clusterCount(), model.iterations());
        return new BuildReport(sourceTable, modelVersion, configId, model.clusterCount(),
                model.iterations(), entries.size(), canonical.rowsRead(), clusterSizes, false,
                coverageDigest, false);
    }

    /**
     * Appends only the content the index does not yet hold, assigning it to the existing centroids.
     *
     * @return a report when the delta was applied incrementally, or {@code null} when the caller
     *         must refit the scope
     *
     * <p>This is the step that makes content-addressed index identity useful for real change rather
     * than only for layout churn. The coverage digest already makes a compaction free; without this,
     * one genuinely new row still costs a full refit of the scope, because refitting is the only
     * write path there is.
     *
     * <p>Appending is sound here for the specific reason that {@code ClusteredIndex.append}'s
     * javadoc rules it out for a rebuild: a rebuild refits, and refitting relabels every existing
     * vector, so appending a second generation would leave each vector present under two cluster
     * ids. Assigning against the <em>existing</em> centroids relabels nothing, so append is exactly
     * the right primitive and no vector can appear twice.
     *
     * <p>Three conditions, each of which is a correctness or quality bound rather than a tuning
     * preference:
     *
     * <ul>
     *   <li><b>Nothing may have been removed.</b> Deleting a subset of a scope's rows would need a
     *       predicate on {@code content_hash}, which is not a partition field of the clustered
     *       table, and Iceberg refuses a row filter it cannot prove covers whole files. Row-level
     *       deletes would do it, but equality deletes are being retired from the spec, so a removal
     *       falls back to a refit -- which is correct, just not cheap.
     *   <li><b>Centroids must already exist</b> for this scope. With none there is nothing to assign
     *       against.
     *   <li><b>The delta must be small relative to the index.</b> Every increment assigns against
     *       centroids fitted over older content, so the assignment drifts from what a fresh fit
     *       would produce. That costs recall, not correctness -- candidates read are still scored
     *       exactly -- but the drift is unbounded over many increments, so past a fraction of the
     *       scope a refit is the better trade. This is the one number here that is a judgement
     *       rather than a constraint.
     * </ul>
     */
    private BuildReport tryIncrement(Table clusteredTable,
                                     Table coverageTable,
                                     String sourceTable,
                                     String modelVersion,
                                     String configId,
                                     Canonical canonical,
                                     List<String> hashes,
                                     String coverageDigest,
                                     Optional<Long> recordedRefitCount) {
        List<float[]> centroids = ClusteredIndex.readCentroids(clusteredTable == null
                ? null : ClusteredIndex.loadCentroidsOrCreate(
                        catalogService.getCatalog(), vectorNamespace), modelVersion, configId);
        if (centroids.isEmpty()) {
            return null;
        }

        Set<String> indexed =
                ClusteredIndex.scopeContentHashes(clusteredTable, modelVersion, configId);
        if (indexed.isEmpty()) {
            return null;
        }

        Set<String> current = new LinkedHashSet<>(hashes);
        List<String> added = new ArrayList<>();
        for (String hash : hashes) {
            if (!indexed.contains(hash)) {
                added.add(hash);
            }
        }
        boolean removals = false;
        for (String hash : indexed) {
            if (!current.contains(hash)) {
                removals = true;
                break;
            }
        }

        if (removals) {
            log.info("Scope {} / {} has removed content; refitting rather than appending",
                    modelVersion, configId);
            return null;
        }
        if (added.isEmpty()) {
            // The digest differed but the content set does not, which means the digest was recorded
            // against something else -- a different clustering, or a coverage row from an older
            // build. Refit rather than silently accept it.
            return null;
        }
        // Measured against the size at the last full refit, not against the current size. The
        // latter is self-defeating under steady growth and shipped that way: a table growing a few
        // percent per pass never exceeds the fraction, so every pass appends, the centroids fitted
        // over the original corpus are never refit, and the assignment drifts without bound. The
        // guard written to prevent drift was exactly what permitted it.
        //
        // An absent baseline means the coverage row predates this column. Treated as "unknown" and
        // therefore permissive, because the alternative -- reading it as zero -- forces a full refit
        // of every scope in the warehouse on upgrade.
        long baseline = recordedRefitCount.orElse((long) indexed.size());
        long ceiling = baseline + Math.max(1, (long) (baseline * maxIncrementalFraction));
        if (hashes.size() > ceiling) {
            log.info("Scope {} / {} holds {} contents against {} at its last refit, beyond the "
                            + "incremental fraction {}; refitting rather than appending",
                    modelVersion, configId, hashes.size(), baseline, maxIncrementalFraction);
            return null;
        }

        VectorClustering.Model existing = new VectorClustering.Model(centroids, 0);
        int[] sizes = new int[centroids.size()];
        List<ClusteredIndex.Entry> newEntries = new ArrayList<>(added.size());
        for (String hash : added) {
            float[] embedding = canonical.vectors().get(hash);
            int cluster = existing.assign(embedding);
            sizes[cluster]++;
            newEntries.add(new ClusteredIndex.Entry(hash, cluster, modelVersion, configId,
                    canonical.dimension(), embedding, canonical.texts().get(hash)));
        }

        ClusteredIndex.append(clusteredTable, newEntries);
        // Centroids are deliberately left alone: they are what the appended rows were assigned
        // against, and rewriting them here would describe an assignment that is not on disk.
        // Baseline carried forward unchanged: this append did not refit, so the centroids still
        // describe the corpus as it stood at the recorded count.
        ClusteredIndex.replaceCoverage(coverageTable, modelVersion, configId, coverageDigest,
                hashes.size(), baseline);

        String scope = scopeKey(modelVersion, configId);
        centroidCache.remove(scope);
        scopeSizeCache.remove(scope);

        List<Integer> clusterSizes = new ArrayList<>(sizes.length);
        for (int size : sizes) {
            clusterSizes.add(size);
        }

        log.info("Appended {} new vectors to {} already indexed for {} / {} against existing centroids",
                added.size(), indexed.size(), modelVersion, configId);
        return new BuildReport(sourceTable, modelVersion, configId, centroids.size(), 0,
                hashes.size(), canonical.rowsRead(), clusterSizes, false, coverageDigest, true);
    }

    /**
     * Whether a scope's clustered table actually holds rows, from manifest metadata only.
     *
     * <p>Guards the coverage skip against a coverage row that outlived its data -- a purge, a manual
     * drop, or a half-applied migration. Without it, "the digest matches" would be enough to serve
     * an empty index indefinitely.
     */
    private boolean indexHasRows(String sourceTable, String modelVersion, String configId) {
        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = ClusteredIndex.identifier(vectorNamespace, sourceTable);
        if (!catalog.tableExists(identifier)) {
            return false;
        }
        return countScope(catalog.loadTable(identifier), modelVersion, configId) > 0;
    }

    /**
     * A scope's canonical vectors, deduplicated by content hash.
     *
     * @param rowsRead rows Tier 1 returned, which exceeds the map size exactly when Tier 1 holds
     *                 the same key more than once
     */
    private record Canonical(Map<String, float[]> vectors,
                             Map<String, String> texts,
                             int dimension,
                             long rowsRead) {
    }

    /**
     * Reads a scope's canonical vectors, one per content hash.
     *
     * <p>The deduplication is not belt and braces. Tier 1 is supposed to hold exactly one row per
     * {@code (content, model, config)}, but it is an append-only Iceberg table with no uniqueness
     * enforcement: two derive batches running concurrently can both miss the dedup probe and both
     * append the same key. Clustering that content twice puts the same vector in the index twice,
     * which inflates the cluster it lands in and makes a probe of that partition read repeats of one
     * vector -- the opposite of the candidate reduction this table exists to demonstrate. Since the
     * embedding is a pure function of the key the duplicates are byte-identical, so keeping the first
     * loses nothing.
     */
    /**
     * Content hashes the content map currently resolves to for a scope.
     *
     * <p>The embedding store is append-only and keeps every version of every piece of content ever
     * embedded, including content whose source row was later edited or deleted. Indexing it
     * wholesale therefore puts superseded vectors into serving: a probe was observed returning both
     * a document and its revised replacement, and would equally return a deleted row's content. Tier
     * 2 does not have this problem because it is projected from the content map's live entries; Tier
     * 3 has to apply the same filter or the two tiers answer differently for the same query.
     */
    private Set<String> liveContentHashes(String sourceTable, String configId) {
        Table contentMap = ContentMap.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (contentMap == null) {
            return Set.of();
        }
        Set<String> live = new LinkedHashSet<>();
        for (ContentMapEntry entry : ContentMap.liveEntries(contentMap, sourceTable, configId)) {
            if (entry.getContentHash() != null) {
                live.add(entry.getContentHash());
            }
        }
        return live;
    }

    private Canonical readCanonical(Table store,
                                    String sourceTable,
                                    String modelVersion,
                                    String configId) {
        Set<String> live = liveContentHashes(sourceTable, configId);
        if (live.isEmpty()) {
            // No live mapping means nothing is being served for this scope, so there is nothing to
            // index. Returning empty lets build() take its existing refusal path rather than
            // committing an index over content no query can reach.
            log.warn("No live content map entries for {} / {}; nothing to cluster",
                    sourceTable, configId);
            return new Canonical(new LinkedHashMap<>(), new LinkedHashMap<>(), 0, 0);
        }
        return readCanonicalFiltered(store, modelVersion, configId, live);
    }

    private Canonical readCanonicalFiltered(Table store,
                                            String modelVersion,
                                            String configId,
                                            Set<String> live) {
        Map<String, float[]> vectors = new LinkedHashMap<>();
        Map<String, String> texts = new LinkedHashMap<>();
        int dimension = 0;
        long rowsRead = 0;

        try (CloseableIterable<Record> rows = IcebergGenerics.read(store)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .build()) {
            for (Record row : rows) {
                float[] embedding = toFloats(row.getField(Constants.EMBEDDING_COLUMN));
                if (embedding.length == 0) {
                    continue;
                }
                rowsRead++;

                if (dimension == 0) {
                    dimension = embedding.length;
                } else if (dimension != embedding.length) {
                    // A mixed-width array column cannot be scored by any similarity expression, and
                    // embedding_dim is written once per row from this value, so a mixture would also
                    // publish a dimension that is wrong for most of the index.
                    throw new IllegalStateException(String.format(
                            "Tier 1 holds mixed dimensions for %s / %s: content %s has %d floats, "
                                    + "expected %d",
                            modelVersion, configId,
                            String.valueOf(row.getField(Constants.CONTENT_HASH_COLUMN)),
                            embedding.length, dimension));
                }

                String hash = String.valueOf(row.getField(Constants.CONTENT_HASH_COLUMN));
                if (!live.contains(hash)) {
                    // Embedded once and no longer referenced by any live mapping: an edited or
                    // deleted row's content. Skipped rather than indexed, or the probe returns
                    // results Tier 2 would not.
                    continue;
                }
                if (vectors.putIfAbsent(hash, embedding) == null) {
                    Object text = row.getField(Constants.TEXT_COLUMN);
                    texts.put(hash, text == null ? null : String.valueOf(text));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read canonical vectors for " + configId, e);
        }

        if (rowsRead > vectors.size()) {
            log.warn("Tier 1 returned {} rows for {} / {} over {} distinct content hashes; "
                            + "clustering the distinct set",
                    rowsRead, modelVersion, configId, vectors.size());
        }
        return new Canonical(vectors, texts, dimension, rowsRead);
    }

    /** Ranks centroids, reads only the closest {@code probes} partitions, scores exactly within them. */
    public ClusteredIndex.ProbeResult probe(String sourceTable,
                                            String modelVersion,
                                            String configId,
                                            float[] query,
                                            int k,
                                            int probes) {
        return probe(sourceTable, modelVersion, configId, query, k, probes, false);
    }

    /**
     * Ranks centroids, reads only the closest {@code probes} partitions, scores exactly within them.
     *
     * <p>{@code requireFresh} is opt-in and defaults off so that the ordinary probe pays no extra
     * I/O at all: the freshness check is two scan plans over manifest metadata, which is cheap
     * against a row scan but is not free, and a probe is allowed to serve a stale index knowingly.
     * When it is asked for and the index is behind, this refuses instead of returning the neighbours
     * it happens to hold -- a caller that asked for freshness wants an error, not a quietly
     * incomplete answer.
     */
    public ClusteredIndex.ProbeResult probe(String sourceTable,
                                            String modelVersion,
                                            String configId,
                                            float[] query,
                                            int k,
                                            int probes,
                                            boolean requireFresh) {
        String scope = scopeKey(modelVersion, configId);
        List<float[]> centroids = centroidCache.computeIfAbsent(scope, key -> {
            Table centroidTable = ClusteredIndex.loadCentroidsOrCreate(
                    catalogService.getCatalog(), vectorNamespace);
            return ClusteredIndex.readCentroids(centroidTable, modelVersion, configId);
        });
        if (centroids.isEmpty()) {
            centroidCache.remove(scope);
            throw new IllegalStateException("No centroids for " + configId + "; build the index first");
        }

        if (requireFresh) {
            requireFreshIndex(sourceTable, modelVersion, configId);
        }

        VectorClustering.Model model = new VectorClustering.Model(centroids, 0);
        Table clustered = ClusteredIndex.loadOrCreate(
                catalogService.getCatalog(), vectorNamespace, sourceTable);

        long total = scopeSizeCache.computeIfAbsent(scope,
                key -> countScope(clustered, modelVersion, configId));
        return ClusteredIndex.probe(clustered, modelVersion, configId, query,
                model.probe(query, probes), k, total);
    }

    /**
     * Refuses a probe whose index is not known to cover Tier 1.
     *
     * <p>{@link Freshness#UNKNOWN} refuses as well as {@link Freshness#BEHIND}. The caller asked for
     * a guarantee, and "I cannot tell" is not one; this repository refuses rather than guesses in
     * the same places it refuses to empty a projection.
     */
    private void requireFreshIndex(String sourceTable, String modelVersion, String configId) {
        ScopeStatus status = statusFor(sourceTable, modelVersion, configId);
        if (status.freshness() == Freshness.FRESH) {
            return;
        }
        throw new IllegalStateException(String.format(
                "Refusing a requireFresh probe of %s / %s: the clustered index holds %d vectors "
                        + "against %d canonical in Tier 1 (%s -- %s). Rebuild it "
                        + "(POST /api/cluster/build) or probe with requireFresh=false to accept "
                        + "stale neighbours.",
                modelVersion, configId, status.indexedVectors(), status.canonicalVectors(),
                status.freshness(), status.note()));
    }

    /** Exhaustive ground truth for the same scope. */
    public List<ClusteredIndex.Candidate> exact(String sourceTable,
                                                String modelVersion,
                                                String configId,
                                                float[] query,
                                                int k) {
        Table clustered = ClusteredIndex.loadOrCreate(
                catalogService.getCatalog(), vectorNamespace, sourceTable);
        return ClusteredIndex.exact(clustered, modelVersion, configId, query, k);
    }

    // ---------------------------------------------------------------- status

    /**
     * Per-scope staleness of a clustered table, as a read.
     *
     * <p>Staleness was invisible: nothing recorded how much of Tier 1 an index covered, so an index
     * built once and never rebuilt served a shrinking fraction of the corpus and looked identical to
     * a current one. This makes it visible without writing anything -- no watermark in the snapshot
     * summary, which is not retry-safe across scopes, and no scheduled refresh, which would land on
     * a task pool of one.
     *
     * <p>Both counts come from manifest metadata rather than from rows, for the reason
     * {@link #countScope} documents. Neither table is created if it does not exist: a GET must not
     * leave a table behind.
     *
     * @param modelVersion one scope, or {@code null} together with {@code configId} for every scope
     *                     the centroid table knows about
     */
    public List<ScopeStatus> status(String sourceTable, String modelVersion, String configId) {
        if (modelVersion != null && configId != null) {
            return List.of(statusFor(sourceTable, modelVersion, configId));
        }

        List<ScopeStatus> statuses = new ArrayList<>();
        for (String[] scope : knownScopes(modelVersion, configId)) {
            statuses.add(statusFor(sourceTable, scope[0], scope[1]));
        }
        return statuses;
    }

    /**
     * The scopes that have been built, from the centroid table.
     *
     * <p>A row scan, unlike everything else on this path, and affordable precisely because of what
     * {@link ClusteredIndex#replaceCentroids} now guarantees: {@code k} rows per scope and one
     * generation, so a warehouse with fifty scopes at k=32 is 1,600 rows. Only the two scope columns
     * are projected, so the centroid arrays are never decoded. There is no cheaper source -- the
     * clustered table's own partitions would list scopes too, but a scope whose centroids exist and
     * whose rows were purged is exactly the state this endpoint is for.
     */
    private List<String[]> knownScopes(String modelVersion, String configId) {
        Catalog catalog = catalogService.getCatalog();
        TableIdentifier identifier = TableIdentifier.of(
                Namespace.of(vectorNamespace), ClusteredIndex.CENTROIDS_TABLE_NAME);
        if (!catalog.tableExists(identifier)) {
            return List.of();
        }

        Set<String> seen = new LinkedHashSet<>();
        List<String[]> scopes = new ArrayList<>();
        Table centroids = catalog.loadTable(identifier);
        IcebergGenerics.ScanBuilder scan = IcebergGenerics.read(centroids)
                .select(Constants.MODEL_VERSION_COLUMN, Constants.CONFIG_ID_COLUMN);
        if (modelVersion != null) {
            scan = scan.where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion));
        }
        if (configId != null) {
            scan = scan.where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId));
        }

        try (CloseableIterable<Record> rows = scan.build()) {
            for (Record row : rows) {
                String model = String.valueOf(row.getField(Constants.MODEL_VERSION_COLUMN));
                String config = String.valueOf(row.getField(Constants.CONFIG_ID_COLUMN));
                if (seen.add(scopeKey(model, config))) {
                    scopes.add(new String[]{model, config});
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not list clustered index scopes", e);
        }
        return scopes;
    }

    private ScopeStatus statusFor(String sourceTable, String modelVersion, String configId) {
        Catalog catalog = catalogService.getCatalog();

        TableIdentifier clusteredIdentifier =
                ClusteredIndex.identifier(vectorNamespace, sourceTable);
        long indexed = 0;
        boolean indexExists = catalog.tableExists(clusteredIdentifier);
        if (indexExists) {
            indexed = countScope(catalog.loadTable(clusteredIdentifier), modelVersion, configId);
        }

        int centroids = 0;
        TableIdentifier centroidIdentifier = TableIdentifier.of(
                Namespace.of(vectorNamespace), ClusteredIndex.CENTROIDS_TABLE_NAME);
        if (catalog.tableExists(centroidIdentifier)) {
            centroids = (int) countScope(
                    catalog.loadTable(centroidIdentifier), modelVersion, configId);
        }

        Table store = EmbeddingStore.loadIfExists(catalog, vectorNamespace);
        if (store == null) {
            return new ScopeStatus(sourceTable, modelVersion, configId, indexed, 0, centroids,
                    Freshness.UNKNOWN, "Tier 1 has not been initialized");
        }

        CanonicalCount canonical = countCanonical(store, modelVersion, configId);
        if (!indexExists) {
            return new ScopeStatus(sourceTable, modelVersion, configId, 0, canonical.count(),
                    centroids, Freshness.BEHIND, "the clustered table does not exist yet");
        }
        if (!canonical.exact()) {
            return new ScopeStatus(sourceTable, modelVersion, configId, indexed, canonical.count(),
                    centroids, Freshness.UNKNOWN,
                    "this scope holds more than " + MAX_COUNTED_HASHES + " distinct contents, so "
                            + "freshness was not determined rather than counted unboundedly");
        }

        Freshness freshness = indexed >= canonical.count() ? Freshness.FRESH : Freshness.BEHIND;
        String note = freshness == Freshness.FRESH
                ? "the index covers every canonical vector in this scope"
                : (canonical.count() - indexed) + " canonical vectors are not in the index";
        return new ScopeStatus(sourceTable, modelVersion, configId, indexed, canonical.count(),
                centroids, freshness, note);
    }

    /**
     * Size of a scope, from manifest metadata rather than from the rows.
     *
     * <p>This used to scan every row in the scope, on every probe, to produce a denominator for a
     * benchmark. That made the fixed cost of a query proportional to the whole table and hid the
     * actual pruning: probing one cluster read 103 rows and still took 1,176ms, because it had
     * silently scanned all 20,000 to count them. Iceberg records per-file row counts in the
     * manifests, so planning answers this without opening a data file.
     *
     * <p>Exact for the clustered and centroid tables specifically, because {@code model_version} and
     * {@code config_id} are identity partition fields of both: planning prunes to whole files whose
     * every row is in the scope, so summing their record counts is the scope's size and not an
     * estimate of it. See {@link #countCanonical} for the table where that does not hold.
     */
    private long countScope(Table table, String modelVersion, String configId) {
        long count = 0;
        try (CloseableIterable<FileScanTask> tasks =
                     table.newScan()
                             .filter(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                             .filter(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                             .planFiles()) {
            for (FileScanTask task : tasks) {
                count += task.file().recordCount();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not size scope " + configId, e);
        }
        return count;
    }

    /**
     * @param count distinct content hashes Tier 1 holds for a scope
     * @param exact whether the whole scope was counted; false when it exceeded
     *              {@link #MAX_COUNTED_HASHES}, in which case {@code count} is a floor
     */
    private record CanonicalCount(long count, boolean exact) {
    }

    /**
     * Distinct content Tier 1 holds for a scope.
     *
     * <p>Deliberately a projected scan, because the cheap manifest answer -- sum
     * {@code recordCount} over the planned files -- is wrong twice over, and was wrong here.
     *
     * <p>First, Tier 1 is partitioned by {@code (model_version, hash_prefix)} and <em>not</em> by
     * {@code config_id}, so a planned file can hold rows for several configurations and its record
     * count is not this scope's. An earlier version of this method tried to recover exactness from
     * the per-file {@code config_id} bounds Iceberg writes into the manifest, which is sound in
     * principle but depends on column statistics surviving scan planning; when they did not, every
     * scope reported {@link Freshness#UNKNOWN} and the staleness check silently stopped working.
     *
     * <p>Second, and fatally for any row count: Tier 1 is append-only with no row-level dedup, so
     * two derive passes that wrote the same content leave two rows for one content hash. The
     * clustered index holds one vector per <em>distinct</em> content, so a row count is not the
     * denominator that comparison needs regardless of how exactly it is scoped -- it reported a
     * complete index as 6 of 8 and called it BEHIND.
     *
     * <p>Two columns only, and {@code config_id} is one of them because it is a filter column and
     * not a partition column: Iceberg builds the residual evaluator against the projected schema,
     * so projecting it away fails the scan with "Cannot find field". Same rule
     * {@code EmbeddingStore}'s own probe projection documents.
     *
     * <p>Not on the default probe path. Both callers -- {@code GET /api/cluster/status} and a
     * {@code requireFresh} probe -- are explicit opt-ins, and that is what makes a scan affordable
     * here. Putting a count on every probe is the regression that took a 103-row read to 1,176ms.
     * Beyond {@link #MAX_COUNTED_HASHES} distinct hashes this stops accumulating and reports
     * {@code exact=false} rather than holding an unbounded set for an operator's status call.
     */
    private CanonicalCount countCanonical(Table store, String modelVersion, String configId) {
        Set<String> hashes = new LinkedHashSet<>();
        boolean exact = true;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(store)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .select(Constants.CONTENT_HASH_COLUMN, Constants.CONFIG_ID_COLUMN)
                .build()) {
            for (Record row : rows) {
                if (hashes.size() >= MAX_COUNTED_HASHES) {
                    exact = false;
                    break;
                }
                hashes.add(String.valueOf(row.getField(Constants.CONTENT_HASH_COLUMN)));
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Could not count Tier 1 content for " + modelVersion + " / " + configId, e);
        }
        return new CanonicalCount(hashes.size(), exact);
    }

    private static String scopeKey(String modelVersion, String configId) {
        return modelVersion + "\u001F" + configId;
    }

    private static float[] toFloats(Object value) {
        if (!(value instanceof List<?> list)) {
            return new float[0];
        }
        float[] values = new float[list.size()];
        int position = 0;
        for (Object element : list) {
            values[position++] = element instanceof Number number ? number.floatValue() : 0f;
        }
        return values;
    }
}
