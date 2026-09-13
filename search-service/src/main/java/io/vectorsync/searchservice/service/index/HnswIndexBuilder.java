package io.vectorsync.searchservice.service.index;

import io.vectorsync.common.dto.VectorRecord;
import io.vectorsync.format.index.IndexManifestEntry;
import io.vectorsync.format.index.IndexStatus;
import io.vectorsync.format.vector.VectorIds;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Builds a durable, versioned HNSW artifact and registers it in the manifest.
 *
 * <p>Replaces the previous in-memory {@code ByteBuffersDirectory} that was rebuilt from a full
 * table scan on every query. An artifact now outlives the process, is addressable by index id,
 * and carries the lineage needed to reproduce it.
 */
@Service
@Slf4j
public class HnswIndexBuilder {

    public static final String ALGORITHM = "hnsw";
    public static final String METRIC = "cosine";

    /** Lucene's write lock is local bookkeeping and must not become part of the artifact. */
    private static final String WRITE_LOCK = "write.lock";

    @Value("${vectorsync.index.hnsw.max-conn:16}")
    private int maxConn;

    @Value("${vectorsync.index.hnsw.beam-width:100}")
    private int beamWidth;

    private final IndexRegistry registry;

    public HnswIndexBuilder(IndexRegistry registry) {
        this.registry = registry;
    }

    public Map<String, String> params() {
        return Map.of("maxConn", String.valueOf(maxConn), "beamWidth", String.valueOf(beamWidth));
    }

    /**
     * Builds an index over {@code vectors} and records it as READY.
     *
     * <p>A failed build is still recorded, as FAILED with the error, so the manifest reflects
     * every attempt rather than silently omitting the broken ones.
     */
    public IndexManifestEntry build(String sourceTable,
                                    long sourceSnapshotId,
                                    String embeddingModel,
                                    String embeddingVersion,
                                    List<VectorRecord> vectors) {
        String paramSignature = "maxConn=" + maxConn + ",beamWidth=" + beamWidth;
        String indexId = VectorIds.indexId(
                sourceTable, sourceSnapshotId, embeddingModel, embeddingVersion, ALGORITHM, paramSignature);

        int dimension = vectors.stream()
                .filter(vector -> vector.getEmbedding() != null && !vector.getEmbedding().isEmpty())
                .mapToInt(vector -> vector.getEmbedding().size())
                .findFirst()
                .orElse(0);

        IndexManifestEntry.IndexManifestEntryBuilder builder = IndexManifestEntry.builder()
                .indexId(indexId)
                .sourceTable(sourceTable)
                .sourceSnapshotId(sourceSnapshotId)
                .embeddingModel(embeddingModel)
                .embeddingVersion(embeddingVersion)
                .indexAlgorithm(ALGORITHM)
                .indexParams(params())
                .similarityMetric(METRIC)
                .dimension(dimension)
                .indexUri(registry.artifactUri(indexId))
                .vectorCount(vectors.size())
                .builtAt(Instant.now());

        // Publish BUILDING first so a crash mid-build leaves an explicit trace rather than
        // nothing, and serving never mistakes a partial artifact for a usable one.
        registry.manifest().put(builder
                .status(IndexStatus.BUILDING)
                .indexFiles(List.of())
                .builtAt(Instant.now())
                .build());

        Path workDir = null;
        try {
            workDir = Files.createTempDirectory("vectorsync-index-" + indexId.substring(0, 8) + "-");
            writeIndex(workDir, vectors);
            Files.deleteIfExists(workDir.resolve(WRITE_LOCK));

            List<String> files = registry.artifacts().upload(workDir, registry.artifactUri(indexId));

            IndexManifestEntry ready = builder
                    .status(IndexStatus.READY)
                    .indexFiles(files)
                    .builtAt(Instant.now())
                    .build();
            registry.manifest().put(ready);

            log.info("Built index {} for {} {}:{} over {} vectors ({} files)",
                    indexId, sourceTable, embeddingModel, embeddingVersion, vectors.size(), files.size());
            return ready;
        } catch (Exception e) {
            IndexManifestEntry failed = builder
                    .status(IndexStatus.FAILED)
                    .indexFiles(List.of())
                    .errorMessage(e.getMessage())
                    .builtAt(Instant.now())
                    .build();
            registry.manifest().put(failed);
            throw new IllegalStateException("Failed to build index " + indexId, e);
        } finally {
            deleteRecursively(workDir);
        }
    }

    private void writeIndex(Path workDir, List<VectorRecord> vectors) throws IOException {
        KnnVectorsFormat hnswFormat = new Lucene99HnswVectorsFormat(maxConn, beamWidth);
        Lucene99Codec codec = new Lucene99Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return hnswFormat;
            }
        };

        IndexWriterConfig config = new IndexWriterConfig();
        config.setCodec(codec);
        config.setRAMBufferSizeMB(256.0);

        try (FSDirectory directory = FSDirectory.open(workDir);
             IndexWriter writer = new IndexWriter(directory, config)) {
            for (VectorRecord record : vectors) {
                Document document = toDocument(record);
                if (document != null) {
                    writer.addDocument(document);
                }
            }
            writer.commit();
            // Single segment keeps the artifact small and search latency predictable.
            writer.forceMerge(1);
        }
    }

    private Document toDocument(VectorRecord record) {
        List<Double> embedding = record.getEmbedding();
        if (embedding == null || embedding.isEmpty()) {
            // Tombstones carry no vector and are excluded before this point; skip defensively
            // rather than writing a zero vector that would pollute nearest-neighbour results.
            return null;
        }

        float[] vector = new float[embedding.size()];
        for (int i = 0; i < embedding.size(); i++) {
            vector[i] = embedding.get(i).floatValue();
        }

        Document document = new Document();
        document.add(new KnnFloatVectorField(IndexFields.VECTOR, vector, VectorSimilarityFunction.COSINE));
        document.add(new StringField(IndexFields.VECTOR_ID, nullSafe(record.getVectorId()), Field.Store.YES));
        document.add(new StoredField(IndexFields.SOURCE_TABLE, nullSafe(record.getSourceTable())));
        document.add(new StoredField(IndexFields.SOURCE_ROW_ID, nullSafe(record.getSourceRowId())));
        document.add(new StoredField(IndexFields.TEXT, nullSafe(record.getText())));
        document.add(new StoredField(IndexFields.MODEL_VERSION, nullSafe(record.modelVersion())));
        document.add(new StoredField(IndexFields.SOURCE_SNAPSHOT_ID, record.getSourceSnapshotId()));
        return document;
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static void deleteRecursively(Path dir) {
        if (dir == null) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // Temp cleanup only.
                }
            });
        } catch (IOException ignored) {
            // Temp cleanup only.
        }
    }
}
