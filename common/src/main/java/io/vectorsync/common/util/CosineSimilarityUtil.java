package io.vectorsync.common.util;

import java.util.List;

public class CosineSimilarityUtil {

    private CosineSimilarityUtil() {
    }

    /**
     * Cosine similarity of two equal-length vectors.
     *
     * <p>A length mismatch throws rather than returning 0.0. Returning zero made a dimension
     * mismatch invisible: every candidate of the wrong width scored 0, so an exhaustive scan across
     * two embedding spaces still produced a confident-looking ranking made entirely of the vectors
     * that happened to match the query's width. That is how a 768-dimension row stayed absent from
     * results a 384-dimension query ranked. Comparing different dimensions is a programming error,
     * and the scan that does it is the ground truth other measurements are taken against.
     *
     * @throws IllegalArgumentException if the vectors have different lengths
     */
    public static double cosineSimilarity(List<Double> vec1, List<Double> vec2) {
        if (vec1 == null || vec2 == null) {
            return 0.0;
        }
        if (vec1.size() != vec2.size()) {
            throw new IllegalArgumentException(
                    "Cannot compare embeddings of different dimensions: " + vec1.size() + " vs " + vec2.size());
        }

        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;

        for (int i = 0; i < vec1.size(); i++) {
            double v1 = vec1.get(i);
            double v2 = vec2.get(i);

            dotProduct += v1 * v2;
            magnitude1 += v1 * v1;
            magnitude2 += v2 * v2;
        }

        magnitude1 = Math.sqrt(magnitude1);
        magnitude2 = Math.sqrt(magnitude2);

        if (magnitude1 == 0.0 || magnitude2 == 0.0) {
            return 0.0;
        }

        return dotProduct / (magnitude1 * magnitude2);
    }
}
