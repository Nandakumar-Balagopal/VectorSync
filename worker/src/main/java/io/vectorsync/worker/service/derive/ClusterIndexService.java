package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.derive.ClusteredIndex;
import io.vectorsync.format.derive.EmbeddingStore;
import io.vectorsync.format.derive.VectorClustering;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
                              List<Integer> clusterSizes) {
    }

    /**
     * Fits centroids over a scope's canonical vectors and writes the clustered table.
     *
     * <p>Rebuilds from scratch rather than updating in place. Reassignment after a centroid moves is
     * a full relabel, and a prototype that pretended otherwise would hide the real cost of keeping
     * this layer fresh -- which is the honest weakness of the approach and belongs in the open.
     */
    public BuildReport build(String sourceTable, String modelVersion, String configId, int clusters) {
        Table store = EmbeddingStore.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (store == null) {
            throw new IllegalStateException("No embedding store; nothing has been derived yet");
        }

        List<String> hashes = new ArrayList<>();
        List<float[]> vectors = new ArrayList<>();
        List<String> texts = new ArrayList<>();
        int dimension = 0;

        try (CloseableIterable<Record> rows = IcebergGenerics.read(store)
                .where(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .build()) {
            for (Record row : rows) {
                float[] embedding = toFloats(row.getField(Constants.EMBEDDING_COLUMN));
                if (embedding.length == 0) {
                    continue;
                }
                hashes.add(String.valueOf(row.getField(Constants.CONTENT_HASH_COLUMN)));
                vectors.add(embedding);
                Object text = row.getField(Constants.TEXT_COLUMN);
                texts.add(text == null ? null : String.valueOf(text));
                dimension = embedding.length;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read canonical vectors for " + configId, e);
        }

        if (vectors.isEmpty()) {
            throw new IllegalStateException("No vectors for " + modelVersion + " / " + configId);
        }

        // A stable order makes the strided initialisation reproducible, so rebuilding over unchanged
        // data produces the same assignment and the recall figures stay comparable.
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < hashes.size(); i++) {
            order.add(i);
        }
        order.sort(Comparator.comparing(hashes::get));

        List<float[]> ordered = new ArrayList<>(order.size());
        for (int index : order) {
            ordered.add(vectors.get(index));
        }

        VectorClustering.Model model = VectorClustering.fit(ordered, clusters, 25);

        Table centroidTable = ClusteredIndex.loadCentroidsOrCreate(
                catalogService.getCatalog(), vectorNamespace);
        ClusteredIndex.appendCentroids(centroidTable, modelVersion, configId, model.centroids());

        // Dropped and rewritten: a rebuild relabels every vector, so appending would leave each one
        // present under both its old and new cluster and quietly break the probe.
        ClusteredIndex.drop(catalogService.getCatalog(), vectorNamespace, sourceTable);
        Table clustered = ClusteredIndex.loadOrCreate(
                catalogService.getCatalog(), vectorNamespace, sourceTable);

        int[] sizes = new int[model.clusterCount()];
        List<ClusteredIndex.Entry> entries = new ArrayList<>(ordered.size());
        for (int position = 0; position < order.size(); position++) {
            int index = order.get(position);
            int cluster = model.assign(vectors.get(index));
            sizes[cluster]++;
            entries.add(new ClusteredIndex.Entry(hashes.get(index), cluster, modelVersion, configId,
                    dimension, vectors.get(index), texts.get(index)));
        }
        ClusteredIndex.append(clustered, entries);

        List<Integer> clusterSizes = new ArrayList<>(sizes.length);
        for (int size : sizes) {
            clusterSizes.add(size);
        }

        String scope = modelVersion + "\u001F" + configId;
        centroidCache.remove(scope);
        scopeSizeCache.remove(scope);

        log.info("Clustered {} vectors for {} / {} into {} clusters in {} iterations",
                entries.size(), modelVersion, configId, model.clusterCount(), model.iterations());
        return new BuildReport(sourceTable, modelVersion, configId, model.clusterCount(),
                model.iterations(), entries.size(), clusterSizes);
    }

    /** Ranks centroids, reads only the closest {@code probes} partitions, scores exactly within them. */
    public ClusteredIndex.ProbeResult probe(String sourceTable,
                                            String modelVersion,
                                            String configId,
                                            float[] query,
                                            int k,
                                            int probes) {
        String scope = modelVersion + "\u001F" + configId;
        List<float[]> centroids = centroidCache.computeIfAbsent(scope, key -> {
            Table centroidTable = ClusteredIndex.loadCentroidsOrCreate(
                    catalogService.getCatalog(), vectorNamespace);
            return ClusteredIndex.readCentroids(centroidTable, modelVersion, configId);
        });
        if (centroids.isEmpty()) {
            centroidCache.remove(scope);
            throw new IllegalStateException("No centroids for " + configId + "; build the index first");
        }

        VectorClustering.Model model = new VectorClustering.Model(centroids, 0);
        Table clustered = ClusteredIndex.loadOrCreate(
                catalogService.getCatalog(), vectorNamespace, sourceTable);

        long total = scopeSizeCache.computeIfAbsent(scope,
                key -> countScope(clustered, modelVersion, configId));
        return ClusteredIndex.probe(clustered, modelVersion, configId, query,
                model.probe(query, probes), k, total);
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

    /**
     * Size of a scope, from manifest metadata rather than from the rows.
     *
     * <p>This used to scan every row in the scope, on every probe, to produce a denominator for a
     * benchmark. That made the fixed cost of a query proportional to the whole table and hid the
     * actual pruning: probing one cluster read 103 rows and still took 1,176ms, because it had
     * silently scanned all 20,000 to count them. Iceberg records per-file row counts in the
     * manifests, so planning answers this without opening a data file.
     */
    private long countScope(Table table, String modelVersion, String configId) {
        long count = 0;
        try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
                     table.newScan()
                             .filter(Expressions.equal(Constants.MODEL_VERSION_COLUMN, modelVersion))
                             .filter(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                             .planFiles()) {
            for (org.apache.iceberg.FileScanTask task : tasks) {
                count += task.file().recordCount();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not size scope " + configId, e);
        }
        return count;
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
