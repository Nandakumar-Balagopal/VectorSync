package io.vectorsync.format.index;

/**
 * One vector in the form an index builder actually consumes.
 *
 * <p>Exists because {@code VectorRecord} holds its embedding as {@code List<Double>}, which is
 * convenient for a single record and ruinous for a million. A 384-dimension embedding costs roughly
 * 9KB as boxed Doubles against about 1.6KB as a {@code float[]}, and an index build has to hold the
 * whole set at once, so the boxed form put a hard ceiling on index size long before the vectors
 * themselves were large.
 *
 * <p>Float, not double, is also the honest width: the Iceberg column is float32 and the Lucene HNSW
 * field is float32, so the double-precision form in between never carried real precision.
 *
 * <p>This carries only the fields an index stores. It is not a general-purpose replacement for
 * {@code VectorRecord}, which still describes lineage.
 */
public record IndexVector(String vectorId,
                          String sourceTable,
                          String sourceRowId,
                          String text,
                          String modelVersion,
                          long sourceSnapshotId,
                          long sourceSequenceNumber,
                          float[] embedding) {

    public int dimension() {
        return embedding == null ? 0 : embedding.length;
    }

    public boolean hasEmbedding() {
        return embedding != null && embedding.length > 0;
    }
}
