package io.vectorsync.searchservice.service.index;

import io.vectorsync.common.Constants;
import io.vectorsync.common.dto.VectorRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Service for building and searching HNSW (Hierarchical Navigable Small World) vector index
 * using Apache Lucene for efficient approximate nearest neighbor (ANN) search.
 */
@Service
@Slf4j
public class HnswIndexService {

    private static final String FIELD_VECTOR = "vector";
    private static final String FIELD_VECTOR_ID = "vectorId";
    private static final String FIELD_SOURCE_TABLE = "sourceTable";
    private static final String FIELD_SOURCE_ROW_ID = "sourceRowId";
    private static final String FIELD_TEXT = "text";
    
    // HNSW parameters
    private static final int MAX_CONN = 16;  // M parameter: max connections per layer
    private static final int BEAM_WIDTH = 100;  // efConstruction: size of dynamic candidate list
    
    private Directory indexDirectory;
    private IndexReader indexReader;
    private IndexSearcher indexSearcher;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    private volatile boolean indexBuilt = false;
    private volatile int indexedVectorCount = 0;
    private volatile String indexedSignature = "";

    /**
     * Build HNSW index from a list of vector records.
     * This replaces any existing index.
     *
     * @param vectors List of vector records to index
     * @throws IOException if index building fails
     */
    public void buildIndex(List<VectorRecord> vectors) throws IOException {
        lock.writeLock().lock();
        try {
            log.info("Building HNSW index for {} vectors", vectors.size());
            long startTime = System.currentTimeMillis();
            
            // Close existing index if any
            closeIndex();
            
            // Create new in-memory index directory
            indexDirectory = new ByteBuffersDirectory();
            
            // Configure HNSW codec
            KnnVectorsFormat hnswFormat = new Lucene99HnswVectorsFormat(MAX_CONN, BEAM_WIDTH);
            Lucene99Codec codec = new Lucene99Codec() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return hnswFormat;
                }
            };
            
            // Configure index writer
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(codec);
            config.setRAMBufferSizeMB(256.0); // Use more memory for faster indexing
            
            // Build index
            try (IndexWriter writer = new IndexWriter(indexDirectory, config)) {
                for (VectorRecord record : vectors) {
                    Document doc = createDocument(record);
                    writer.addDocument(doc);
                }
                writer.commit();
                writer.forceMerge(1); // Optimize to single segment for better search performance
            }
            
            // Open reader and searcher
            indexReader = DirectoryReader.open(indexDirectory);
            indexSearcher = new IndexSearcher(indexReader);
            
            indexBuilt = true;
            indexedVectorCount = vectors.size();
            indexedSignature = signature(vectors);
            
            long duration = System.currentTimeMillis() - startTime;
            log.info("HNSW index built successfully: {} vectors indexed in {}ms", 
                     indexedVectorCount, duration);
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Search for k nearest neighbors using the HNSW index.
     *
     * @param queryVector Query vector
     * @param k Number of nearest neighbors to return
     * @param sourceTable Optional filter by source table
     * @return List of search results with vector IDs and similarity scores
     * @throws IOException if search fails
     */
    public List<VectorSearchResult> search(List<Double> queryVector, int k, String sourceTable) 
            throws IOException {
        lock.readLock().lock();
        try {
            if (!indexBuilt) {
                log.warn("Index not built yet, returning empty results");
                return new ArrayList<>();
            }
            
            // Convert query vector to float array
            float[] queryArray = new float[queryVector.size()];
            for (int i = 0; i < queryVector.size(); i++) {
                queryArray[i] = queryVector.get(i).floatValue();
            }
            
            // Create KNN query
            KnnFloatVectorQuery query = new KnnFloatVectorQuery(FIELD_VECTOR, queryArray, k);
            
            // Execute search
            TopDocs topDocs = indexSearcher.search(query, k);
            
            // Convert results
            List<VectorSearchResult> results = new ArrayList<>();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = indexSearcher.storedFields().document(scoreDoc.doc);
                
                String docSourceTable = doc.get(FIELD_SOURCE_TABLE);
                
                // Apply source table filter if specified
                if (sourceTable != null && !sourceTable.isEmpty() && 
                    docSourceTable != null && !sourceTable.equals(docSourceTable)) {
                    continue;
                }
                
                VectorSearchResult result = VectorSearchResult.builder()
                        .vectorId(doc.get(FIELD_VECTOR_ID))
                        .sourceTable(docSourceTable)
                        .sourceRowId(doc.get(FIELD_SOURCE_ROW_ID))
                        .text(doc.get(FIELD_TEXT))
                        .similarity((double) scoreDoc.score)
                        .build();
                
                results.add(result);
            }
            
            log.debug("HNSW search returned {} results for k={}", results.size(), k);
            return results;
            
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Check if index is built and ready for search.
     */
    public boolean isIndexBuilt() {
        return indexBuilt;
    }
    
    /**
     * Get the number of vectors in the index.
     */
    public int getIndexedVectorCount() {
        return indexedVectorCount;
    }

    /**
     * Detect same-count updates/deletes that should refresh the in-memory index.
     */
    public boolean isStale(List<VectorRecord> vectors) {
        return !Objects.equals(indexedSignature, signature(vectors));
    }
    
    /**
     * Create Lucene document from vector record.
     */
    private Document createDocument(VectorRecord record) {
        Document doc = new Document();
        
        // Convert embedding to float array for Lucene
        List<Double> embedding = record.getEmbedding();
        float[] vectorArray = new float[embedding.size()];
        for (int i = 0; i < embedding.size(); i++) {
            vectorArray[i] = embedding.get(i).floatValue();
        }
        
        // Add vector field with COSINE similarity
        doc.add(new KnnFloatVectorField(FIELD_VECTOR, vectorArray, 
                VectorSimilarityFunction.COSINE));
        
        // Add stored fields for retrieval
        doc.add(new StringField(FIELD_VECTOR_ID, record.getVectorId(), Field.Store.YES));
        doc.add(new StoredField(FIELD_SOURCE_TABLE, record.getSourceTable()));
        doc.add(new StoredField(FIELD_SOURCE_ROW_ID, record.getSourceRowId()));
        doc.add(new StoredField(FIELD_TEXT, record.getText()));
        
        return doc;
    }
    
    /**
     * Close index resources.
     */
    private void closeIndex() throws IOException {
        if (indexReader != null) {
            indexReader.close();
            indexReader = null;
        }
        if (indexDirectory != null) {
            indexDirectory.close();
            indexDirectory = null;
        }
        indexSearcher = null;
        indexBuilt = false;
        indexedVectorCount = 0;
        indexedSignature = "";
    }

    private String signature(List<VectorRecord> vectors) {
        return vectors.stream()
                .map(record -> String.join("|",
                        nullSafe(record.getVectorId()),
                        nullSafe(record.getSourceTable()),
                        nullSafe(record.getSourceRowId()),
                        nullSafe(record.getModelName()),
                        nullSafe(record.getCreatedAt() == null ? null : record.getCreatedAt().toString())))
                .sorted()
                .collect(Collectors.joining("\n"));
    }

    private String nullSafe(String value) {
        return value == null ? "" : value;
    }
    
    @PreDestroy
    public void cleanup() {
        lock.writeLock().lock();
        try {
            closeIndex();
            log.info("HNSW index service cleaned up");
        } catch (IOException e) {
            log.error("Error closing index during cleanup", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}

// Made with Bob
