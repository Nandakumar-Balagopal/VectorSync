package io.vectorsync.format.vector;

import io.vectorsync.common.dto.VectorRecord;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Deterministic identity for stored artifacts.
 *
 * <p>Random UUIDs made retries non-idempotent: re-running a materialization appended a second row
 * for the same logical embedding, and nothing stable existed for an index manifest or an
 * evaluation result to reference. Deriving the id from lineage makes re-running a no-op and gives
 * provenance a stable handle.
 *
 * <p>Note the key deliberately excludes the source data-file path. Iceberg compaction rewrites
 * files without changing row values, so keying on file path would change identity for unchanged
 * data.
 */
public final class VectorIds {

    /**
     * ASCII unit separator. A non-empty delimiter is required: with an empty one,
     * ("ab","c") and ("a","bc") would hash identically, so two distinct source rows
     * could collide on the same vector id.
     */
    private static final String SEPARATOR = "\u001F";

    private VectorIds() {
    }

    public static String vectorId(VectorRecord record) {
        return vectorId(
                record.getSourceTable(),
                record.getSourceRowId(),
                record.getSourceSnapshotId(),
                record.getChunkOrdinal(),
                record.getEmbeddingModel(),
                record.getEmbeddingVersion(),
                record.getPreprocessingId());
    }

    public static String vectorId(String sourceTable,
                                  String sourceRowId,
                                  long sourceSnapshotId,
                                  int chunkOrdinal,
                                  String embeddingModel,
                                  String embeddingVersion,
                                  String preprocessingId) {
        return sha256(String.join(SEPARATOR,
                nullSafe(sourceTable),
                nullSafe(sourceRowId),
                Long.toString(sourceSnapshotId),
                Integer.toString(chunkOrdinal),
                nullSafe(embeddingModel),
                nullSafe(embeddingVersion),
                nullSafe(preprocessingId)));
    }

    /**
     * Identity of an index artifact: the data it covers plus the algorithm configuration used to
     * build it. Two builds with identical inputs and parameters yield the same index id.
     */
    public static String indexId(String sourceTable,
                                 long sourceSnapshotId,
                                 String embeddingModel,
                                 String embeddingVersion,
                                 String indexAlgorithm,
                                 String indexParams) {
        return sha256(String.join(SEPARATOR,
                nullSafe(sourceTable),
                Long.toString(sourceSnapshotId),
                nullSafe(embeddingModel),
                nullSafe(embeddingVersion),
                nullSafe(indexAlgorithm),
                nullSafe(indexParams)));
    }

    /**
     * Stable hash of a preprocessing configuration. Changing which columns are embedded, or how
     * they are joined, yields a different id and so a different logical embedding.
     */
    public static String preprocessingId(java.util.List<String> embeddingColumns, String joinSeparator) {
        String columns = embeddingColumns == null ? "" : String.join(",", embeddingColumns);
        return sha256(columns + SEPARATOR + nullSafe(joinSeparator)).substring(0, 16);
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }
}
