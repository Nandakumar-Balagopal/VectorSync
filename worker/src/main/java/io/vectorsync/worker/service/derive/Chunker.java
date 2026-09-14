package io.vectorsync.worker.service.derive;

import io.vectorsync.format.derive.MaterializationSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Splits the assembled text of a source row into the units that actually get embedded.
 *
 * <p>A chunker must be a pure, deterministic function of {@code (text, chunkSize, chunkOverlap)}
 * and nothing else. Chunk boundaries feed the content hash, so any nondeterminism -- a default
 * locale, a sentence model whose weights drift, iteration over a {@code HashSet} -- silently
 * changes content identity for unchanged text. The consequence is not a cosmetic difference: every
 * sync would miss the dedup probe and re-embed content the store already holds, which is precisely
 * the cost this architecture exists to eliminate.
 *
 * <p>The chunker name and its parameters are hashed into {@link MaterializationSpec#configId()}, so
 * changing any of them produces a new configuration that materializes alongside the old one rather
 * than corrupting it. That is also why {@link #byName} refuses an unknown name instead of falling
 * back to {@code whole}: a typo would be recorded as a legitimate configuration id, and the day the
 * misspelled chunker is implemented for real, that id would start meaning different content while
 * the store keeps serving vectors written under the fallback.
 */
public interface Chunker {

    /** One chunk per row: the entire assembled text. */
    String WHOLE = "whole";

    /** Fixed-width windows of {@code chunkSize} characters advancing by {@code size - overlap}. */
    String FIXED = "fixed";

    /**
     * @param text assembled canonical text, possibly null or blank
     * @return the chunks in order; empty when there is nothing to embed. Chunk ordinal is the
     *         index in this list, so order is part of the on-disk contract.
     */
    List<String> chunk(String text);

    /**
     * True when this chunker can never emit an ordinal above 0.
     *
     * <p>Lets a caller skip reading the existing mapping to look for chunks orphaned by text that
     * shrank: with at most one chunk per row there is never a higher ordinal left behind.
     */
    default boolean singleChunk() {
        return false;
    }

    static Chunker forSpec(MaterializationSpec spec) {
        return byName(spec.getChunker(), spec.getChunkSize(), spec.getChunkOverlap());
    }

    static Chunker byName(String name, int chunkSize, int chunkOverlap) {
        // Locale.ROOT, not the default locale: under a Turkish locale "WHOLE".toLowerCase() is
        // "wholé"-style dotless-i nonsense, so the same spec would resolve to a different chunker
        // depending on which machine the worker happens to run on.
        String key = name == null || name.isBlank() ? WHOLE : name.trim().toLowerCase(Locale.ROOT);

        switch (key) {
            case WHOLE:
                return new Whole();
            case FIXED:
                return new Fixed(chunkSize, chunkOverlap);
            default:
                throw new IllegalArgumentException(
                        "Unknown chunker '" + name + "'; supported: " + WHOLE + ", " + FIXED);
        }
    }

    /**
     * The whole assembled text as a single chunk.
     *
     * <p>The right default: chunking only helps when a passage is longer than the model's context,
     * and splitting shorter text multiplies both inference cost and the number of vectors a search
     * has to rank over.
     */
    record Whole() implements Chunker {

        @Override
        public List<String> chunk(String text) {
            if (text == null || text.isBlank()) {
                return List.of();
            }
            return List.of(text);
        }

        @Override
        public boolean singleChunk() {
            return true;
        }
    }

    /**
     * Fixed-width character windows with a trailing overlap, so a sentence straddling a boundary
     * still appears whole in one of the two chunks.
     */
    record Fixed(int chunkSize, int chunkOverlap) implements Chunker {

        public Fixed {
            if (chunkSize <= 0) {
                throw new IllegalArgumentException(
                        "chunker '" + FIXED + "' requires chunkSize > 0, got " + chunkSize);
            }
            if (chunkOverlap < 0) {
                throw new IllegalArgumentException(
                        "chunkOverlap must not be negative, got " + chunkOverlap);
            }
            if (chunkOverlap >= chunkSize) {
                // The window would advance by zero or move backwards, so chunking would never
                // terminate. Rejected at construction rather than discovered as a hung worker.
                throw new IllegalArgumentException("chunkOverlap " + chunkOverlap
                        + " must be smaller than chunkSize " + chunkSize);
            }
        }

        @Override
        public List<String> chunk(String text) {
            if (text == null || text.isBlank()) {
                return List.of();
            }

            // Windows are measured in code points, not chars. Slicing a String by char index can
            // cut a surrogate pair in half, and a lone surrogate encodes to a replacement byte in
            // UTF-8 -- so two genuinely different chunks can hash to the same content and share a
            // vector, on top of handing the model a corrupt character.
            int[] codePoints = text.codePoints().toArray();
            int step = chunkSize - chunkOverlap;

            List<String> chunks = new ArrayList<>((codePoints.length / step) + 1);
            for (int start = 0; start < codePoints.length; start += step) {
                int end = Math.min(start + chunkSize, codePoints.length);
                chunks.add(new String(codePoints, start, end - start));
                if (end == codePoints.length) {
                    // Stop at the end instead of letting overlap start another window. Without
                    // this, size 10 / overlap 5 over 12 characters emits a third chunk that is a
                    // strict suffix of the second one: a duplicate vector and a duplicate search
                    // hit for the same passage.
                    break;
                }
            }
            return chunks;
        }
    }
}
