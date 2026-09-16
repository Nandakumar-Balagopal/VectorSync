package io.vectorsync.format.derive;

import java.util.ArrayList;
import java.util.List;

/**
 * Spherical k-means over canonical vectors, producing centroids that a partition column can hold.
 *
 * <p>The purpose is not clustering for its own sake. It is to make a query engine's own partition
 * pruning do the work an ANN index would otherwise do. If every vector carries the id of its nearest
 * centroid and the serving table is partitioned by that id, then a query embeds once, ranks the
 * handful of centroids, and reads only the partitions for the closest few -- exact scoring over a
 * small candidate set instead of approximate scoring over a graph. That is IVF, expressed entirely
 * in table-format features that Spark, Trino, Databricks and Snowflake already implement, at a time
 * when no engine can read an Iceberg-native ANN index and the blob format for one is not standardised.
 *
 * <p>Spherical rather than Euclidean: the metric being served is cosine, so vectors are normalised
 * and centroids are re-normalised after every update. Running plain k-means under a cosine metric
 * lets centroid magnitude drift and silently changes which cluster is "nearest".
 *
 * <p>Deterministic by construction. Initial centroids are chosen by striding a stable ordering rather
 * than at random, so rebuilding an index over unchanged data yields the same assignment. A clustering
 * that moved on every rebuild would make recall unreproducible and the partition layout churn for no
 * reason.
 */
public final class VectorClustering {

    private VectorClustering() {
    }

    /**
     * @param centroids  one normalised centroid per cluster, indexed by cluster id
     * @param iterations iterations actually run before assignments stopped changing
     */
    public record Model(List<float[]> centroids, int iterations) {

        public int clusterCount() {
            return centroids.size();
        }

        /** Cluster id whose centroid is closest to {@code vector} under cosine similarity. */
        public int assign(float[] vector) {
            return nearest(vector, centroids, 1).get(0);
        }

        /**
         * The {@code probes} nearest cluster ids, closest first.
         *
         * <p>This is the query-side half: the caller turns these into a partition predicate, so the
         * number of probes is the direct trade between recall and how much of the table is read.
         */
        public List<Integer> probe(float[] query, int probes) {
            return nearest(query, centroids, Math.max(1, Math.min(probes, centroids.size())));
        }
    }

    /**
     * Fits {@code clusters} centroids over {@code vectors}.
     *
     * @param vectors    input vectors; not mutated
     * @param clusters   requested cluster count, clamped to the number of distinct inputs
     * @param maxRounds  iteration ceiling, so a pathological corpus cannot spin
     */
    public static Model fit(List<float[]> vectors, int clusters, int maxRounds) {
        List<float[]> normalised = new ArrayList<>(vectors.size());
        for (float[] vector : vectors) {
            float[] unit = normalise(vector);
            if (unit != null) {
                normalised.add(unit);
            }
        }
        if (normalised.isEmpty()) {
            throw new IllegalArgumentException("Cannot cluster an empty vector set");
        }

        int k = Math.max(1, Math.min(clusters, normalised.size()));
        int dimension = normalised.get(0).length;

        // Strided initialisation over the input order. The caller supplies a stable order (sorted by
        // content hash), so this is reproducible without carrying a seed through the API.
        List<float[]> centroids = new ArrayList<>(k);
        double stride = (double) normalised.size() / k;
        for (int i = 0; i < k; i++) {
            centroids.add(normalised.get((int) (i * stride)).clone());
        }

        int[] assignment = new int[normalised.size()];
        int round = 0;
        for (; round < Math.max(1, maxRounds); round++) {
            boolean moved = false;
            for (int i = 0; i < normalised.size(); i++) {
                int best = nearest(normalised.get(i), centroids, 1).get(0);
                if (best != assignment[i]) {
                    assignment[i] = best;
                    moved = true;
                }
            }
            if (!moved && round > 0) {
                break;
            }

            float[][] sums = new float[k][dimension];
            int[] counts = new int[k];
            for (int i = 0; i < normalised.size(); i++) {
                float[] vector = normalised.get(i);
                int cluster = assignment[i];
                counts[cluster]++;
                for (int d = 0; d < dimension; d++) {
                    sums[cluster][d] += vector[d];
                }
            }

            for (int cluster = 0; cluster < k; cluster++) {
                if (counts[cluster] == 0) {
                    // An empty cluster would make one partition permanently unreachable and waste a
                    // probe slot, so it is re-seeded from the input rather than left to collapse.
                    centroids.set(cluster, normalised.get(cluster % normalised.size()).clone());
                    continue;
                }
                float[] centroid = new float[dimension];
                for (int d = 0; d < dimension; d++) {
                    centroid[d] = sums[cluster][d] / counts[cluster];
                }
                float[] unit = normalise(centroid);
                centroids.set(cluster, unit == null ? centroids.get(cluster) : unit);
            }
        }

        return new Model(List.copyOf(centroids), round + 1);
    }

    /** Cosine similarity between two equal-length vectors, assuming neither is zero-length. */
    public static double cosine(float[] a, float[] b) {
        if (a == null || b == null || a.length != b.length) {
            return 0.0;
        }
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0) {
            return 0.0;
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    private static List<Integer> nearest(float[] vector, List<float[]> centroids, int count) {
        // count is small (a probe list, usually single digits) so a partial selection beats a sort.
        List<Integer> chosen = new ArrayList<>(count);
        double[] best = new double[count];
        for (int i = 0; i < count; i++) {
            best[i] = Double.NEGATIVE_INFINITY;
            chosen.add(0);
        }

        for (int cluster = 0; cluster < centroids.size(); cluster++) {
            double score = cosine(vector, centroids.get(cluster));
            for (int slot = 0; slot < count; slot++) {
                if (score > best[slot]) {
                    for (int shift = count - 1; shift > slot; shift--) {
                        best[shift] = best[shift - 1];
                        chosen.set(shift, chosen.get(shift - 1));
                    }
                    best[slot] = score;
                    chosen.set(slot, cluster);
                    break;
                }
            }
        }
        return chosen;
    }

    /** @return a unit-length copy, or null when the vector has no direction */
    private static float[] normalise(float[] vector) {
        if (vector == null || vector.length == 0) {
            return null;
        }
        double norm = 0.0;
        for (float value : vector) {
            norm += (double) value * value;
        }
        if (norm == 0.0) {
            return null;
        }
        double length = Math.sqrt(norm);
        float[] unit = new float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            unit[i] = (float) (vector[i] / length);
        }
        return unit;
    }
}
