package io.vectorsync.searchservice.service.index;

import io.vectorsync.format.index.IndexManifestEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * An opened index artifact, ready to search.
 *
 * <p>Immutable once opened: a new build produces a new artifact and a new manifest entry rather
 * than mutating this one. That is what lets a rollback simply re-point the alias.
 */
@Slf4j
public class HnswIndexArtifact implements AutoCloseable {

    private final IndexManifestEntry entry;
    private final Directory directory;
    private final IndexReader reader;
    private final IndexSearcher searcher;
    private final Path localDir;

    HnswIndexArtifact(IndexManifestEntry entry, Directory directory, Path localDir) throws IOException {
        this.entry = entry;
        this.directory = directory;
        this.localDir = localDir;
        this.reader = DirectoryReader.open(directory);
        this.searcher = new IndexSearcher(reader);
    }

    public IndexManifestEntry entry() {
        return entry;
    }

    public IndexSearcher searcher() {
        return searcher;
    }

    public String indexId() {
        return entry.getIndexId();
    }

    public int numDocs() {
        return reader.numDocs();
    }

    @Override
    public void close() {
        closeQuietly(reader, "reader");
        closeQuietly(directory, "directory");
        deleteLocalDir();
    }

    private void closeQuietly(AutoCloseable closeable, String what) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("Failed to close index {} for {}: {}", what, entry.getIndexId(), e.getMessage());
        }
    }

    private void deleteLocalDir() {
        if (localDir == null) {
            return;
        }
        try (Stream<Path> paths = Files.walk(localDir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    log.warn("Failed to delete {}: {}", path, e.getMessage());
                }
            });
        } catch (IOException e) {
            log.warn("Failed to clean local index cache {}: {}", localDir, e.getMessage());
        }
    }
}
