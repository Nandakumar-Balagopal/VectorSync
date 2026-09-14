package io.vectorsync.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This function is the ground truth that index recall is measured against, so an error here does
 * not surface as a wrong similarity: it surfaces as an index that appears to be losing neighbours,
 * or as one that appears perfect while returning the wrong rows.
 */
class CosineSimilarityUtilTest {

    private static final double EPSILON = 1e-9;

    @Test
    @DisplayName("identical vectors score 1 and opposite vectors score -1")
    void boundsAreExact() {
        List<Double> vector = List.of(1.0, 2.0, 3.0);
        List<Double> opposite = List.of(-1.0, -2.0, -3.0);

        assertEquals(1.0, CosineSimilarityUtil.cosineSimilarity(vector, vector), EPSILON);
        assertEquals(-1.0, CosineSimilarityUtil.cosineSimilarity(vector, opposite), EPSILON);
    }

    @Test
    @DisplayName("orthogonal vectors score 0")
    void orthogonalScoresZero() {
        assertEquals(0.0, CosineSimilarityUtil.cosineSimilarity(
                List.of(1.0, 0.0), List.of(0.0, 1.0)), EPSILON);
    }

    @Test
    @DisplayName("magnitude does not affect the score")
    void scaleInvariant() {
        double unscaled = CosineSimilarityUtil.cosineSimilarity(
                List.of(1.0, 2.0, 3.0), List.of(4.0, 5.0, 6.0));
        double scaled = CosineSimilarityUtil.cosineSimilarity(
                List.of(10.0, 20.0, 30.0), List.of(4.0, 5.0, 6.0));

        assertEquals(unscaled, scaled, EPSILON);
    }

    @Test
    @DisplayName("a dimension mismatch throws instead of scoring zero")
    void dimensionMismatchThrows() {
        // Scoring zero is what let a 768-dimension row sit silently absent from results ranked by
        // a 384-dimension query: every mismatched candidate tied at the bottom and the ranking
        // still looked plausible.
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> CosineSimilarityUtil.cosineSimilarity(List.of(1.0, 2.0), List.of(1.0, 2.0, 3.0)));

        assertTrue(thrown.getMessage().contains("2 vs 3"), thrown.getMessage());
    }

    @Test
    @DisplayName("a zero vector scores 0 rather than dividing by zero")
    void zeroVectorIsSafe() {
        assertEquals(0.0, CosineSimilarityUtil.cosineSimilarity(
                List.of(0.0, 0.0), List.of(1.0, 1.0)), EPSILON);
    }

    @Test
    @DisplayName("nulls score 0")
    void nullsAreSafe() {
        assertEquals(0.0, CosineSimilarityUtil.cosineSimilarity(null, List.of(1.0)), EPSILON);
        assertEquals(0.0, CosineSimilarityUtil.cosineSimilarity(List.of(1.0), null), EPSILON);
    }

    @Test
    @DisplayName("empty vectors score 0")
    void emptyIsSafe() {
        assertEquals(0.0, CosineSimilarityUtil.cosineSimilarity(List.of(), List.of()), EPSILON);
    }
}
