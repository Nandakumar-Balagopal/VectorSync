package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.derive.ClusteredIndex;
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

    public record BuildReport(String sourceTable,
                              String modelVersion,
                              String configId,
                              int clusters,
                              int iterations,
                              long vectors,
                              long canonicalRowsRead,
                              List<Integer> clusterSizes) {
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
     * <p>Rebuilds from scratch rather than updating in place. Reassignment after a centroid moves is
     * a full relabel, and a prototype that pretended otherwise would hide the real cost of keeping
     * this layer fresh -- which is the honest weakness of the approach and belongs in the open.
     *
     * <p>Two commits land, the data and then the centroids, and there is no atomicity across them:
     * Iceberg commits one table at a time. Data first is the better of the two orders, because it
     * means the published centroids always describe an assignment that is already on disk. A failure
     * between them leaves the previous centroids ranking a new assignment, which costs recall -- the
     * candidates that are read are still scored exactly -- rather than pointing the probe at
     * partitions that hold no files.
     */
    public BuildReport build(String sourceTable, String modelVersion, String configId, int clusters) {
        Table store = EmbeddingStore.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (store == null) {
            throw new IllegalStateException("No embedding store; nothing has been derived yet");
        }

        Canonical canonical = readCanonical(store, modelVersion, configId);
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

        List<float[]> ordered = new ArrayList<>(hashes.size());
        for (String hash : hashes) {
            ordered.add(canonical.vectors().get(hash));
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

        Table clustered = ClusteredIndex.loadOrCreate(
                catalogService.getCatalog(), vectorNamespace, sourceTable);
        ClusteredIndex.replaceScope(clustered, modelVersion, configId, entries);

        Table centroidTable = ClusteredIndex.loadCentroidsOrCreate(
                catalogService.getCatalog(), vectorNamespace);
        ClusteredIndex.replaceCentroids(centroidTable, modelVersion, configId, model.centroids());

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
                model.iterations(), entries.size(), canonical.rowsRead(), clusterSizes);
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
    private Canonical readCanonical(Table store, String modelVersion, String configId) {
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
