package io.vectorsync.format.derive;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

/**
 * Content identity for a unit of text about to be embedded.
 *
 * <p>This is the hinge the whole system turns on. An embedding is a pure function of
 * {@code (content, model, configuration)}, so content is the correct key for a vector -- not the
 * row it happened to arrive on. Keying by row identity, as the original design did, means a row
 * that changes any column pays for inference again even when the embedded text is byte-identical,
 * and two rows carrying the same text pay twice. Keying by content means a re-embedding campaign
 * costs novel content only.
 *
 * <p>The hash covers the assembled text and nothing else. Model and configuration are separate
 * components of the store key, which is what lets the same content be looked up per model.
 */
public final class ContentHash {

    /**
     * Unit separator. The same reason {@code VectorIds} uses one: joining with an empty separator
     * would make ("ab","c") and ("a","bc") hash identically, so two different rows could collide
     * onto one vector.
     */
    public static final String SEPARATOR = "";

    private ContentHash() {
    }

    /** Store key for a vector: content under a specific model and configuration. */
    public static String storeKey(String contentHash, String modelVersion, String configId) {
        return String.join(SEPARATOR, contentHash, modelVersion, configId);
    }

    /**
     * Assembles the exact text that will be handed to the model.
     *
     * <p>Deliberately mechanical and total: the output is what gets hashed, so any normalization
     * applied here silently changes vector identity for every row in the warehouse. Anything
     * optional belongs in the spec (and therefore in {@code config_id}), never hidden in here.
     *
     * @param values         column values in spec order; nulls become empty strings so a null and
     *                       an empty string are the same content, which is what an embedder sees
     * @param joinSeparator  separator from the spec, part of {@code config_id}
     */
    public static String canonicalText(List<String> values, String joinSeparator) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        String separator = joinSeparator == null ? " " : joinSeparator;

        StringBuilder assembled = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                assembled.append(separator);
            }
            String value = values.get(i);
            assembled.append(value == null ? "" : value);
        }
        return assembled.toString();
    }

    /** SHA-256 of the assembled text, hex. Full width: this is a dedup key across billions. */
    public static String of(String canonicalText) {
        return sha256(canonicalText == null ? "" : canonicalText);
    }

    public static String of(List<String> values, String joinSeparator) {
        return of(canonicalText(values, joinSeparator));
    }

    /**
     * Leading hex characters of a content hash. Still written on every embedding-store row, but no
     * longer a partition column.
     *
     * <p>It was one, on the reasoning that uniformly distributed hashes give 256 evenly sized
     * buckets for free. Measurement went the other way on both sides -- a realistic probe's prefix
     * set covered every bucket so reads degraded to a full scan, and writes fragmented into 44,587
     * files averaging 9.2 KiB -- so {@code EmbeddingStore.partitionSpec} now partitions by model
     * version alone. The column survives because removing it from the schema would brick the table:
     * historical partition specs still source its field id.
     */
    public static String prefix(String contentHash) {
        if (contentHash == null || contentHash.length() < 2) {
            return "00";
        }
        return contentHash.substring(0, 2);
    }

    /**
     * A digest identifying exactly which content a derived artifact covers.
     *
     * <p>This is what lets a vector index be identified by <em>what it contains</em> rather than by
     * the source snapshot it was built from, and the difference is the whole point. An index keyed
     * by snapshot id must be rebuilt whenever a new snapshot appears, because its key changed --
     * even when the snapshot only moved bytes. Compaction, a data-file rewrite, a sort
     * reorganisation and a partition rewrite all produce a new snapshot and change no content, so
     * they all leave this digest identical and the existing index provably still correct.
     *
     * <p>Order-independent by sorting before hashing, because the caller's iteration order is a
     * property of a scan rather than of the data: the same content read back in a different file
     * order must not look like different coverage. Distinct-by-construction for the same reason --
     * duplicate rows for one content hash are a legitimate state of the append-only embedding store
     * and must not change what the index is said to cover.
     *
     * <p>An empty set has its own digest rather than hashing to the empty string, so "covers
     * nothing" is distinguishable from "was never recorded".
     */
    public static String coverageDigest(Collection<String> contentHashes) {
        if (contentHashes == null || contentHashes.isEmpty()) {
            return sha256("vectorsync.coverage.empty");
        }

        List<String> sorted = new ArrayList<>(new TreeSet<>(contentHashes));
        StringBuilder assembled = new StringBuilder(sorted.size() * 65);
        for (String hash : sorted) {
            assembled.append(hash == null ? "" : hash).append(SEPARATOR);
        }
        return sha256(assembled.toString());
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hashed.length * 2);
            for (byte b : hashed) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
