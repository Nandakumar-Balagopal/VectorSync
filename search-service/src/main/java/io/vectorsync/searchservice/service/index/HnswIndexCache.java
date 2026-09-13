package io.vectorsync.searchservice.service.index;

import io.vectorsync.format.index.IndexManifestEntry;
import io.vectorsync.format.index.IndexStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.store.FSDirectory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Opens index artifacts on demand and keeps them open, keyed by index id.
 *
 * <p>Because an artifact is immutable, caching is trivially safe: an id always denotes the same
 * bytes. Promotion changes which id serves traffic, not the contents of one, so a promotion needs
 * no invalidation — the next query resolves a different id and this loads it.
 */
@Service
@Slf4j
public class HnswIndexCache {

    private final IndexRegistry registry;
    private final Map<String, HnswIndexArtifact> open = new ConcurrentHashMap<>();

    public HnswIndexCache(IndexRegistry registry) {
        this.registry = registry;
    }

    public HnswIndexArtifact get(IndexManifestEntry entry) {
        if (entry.getStatus() != IndexStatus.READY && entry.getStatus() != IndexStatus.ARCHIVED) {
            throw new IllegalStateException("Index " + entry.getIndexId()
                    + " is not servable (status=" + entry.getStatus() + ")");
        }

        return open.computeIfAbsent(entry.getIndexId(), indexId -> load(entry));
    }

    private HnswIndexArtifact load(IndexManifestEntry entry) {
        Path localDir;
        try {
            localDir = Files.createTempDirectory("vectorsync-idx-" + entry.getIndexId().substring(0, 8) + "-");
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create a local cache dir for index " + entry.getIndexId(), e);
        }

        registry.artifacts().download(entry.getIndexUri(), entry.getIndexFiles(), localDir);

        try {
            HnswIndexArtifact artifact = new HnswIndexArtifact(entry, FSDirectory.open(localDir), localDir);
            log.info("Opened index {} ({} docs) for {} {}",
                    entry.getIndexId(), artifact.numDocs(), entry.getSourceTable(), entry.modelVersion());
            return artifact;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open index artifact " + entry.getIndexId(), e);
        }
    }

    /** Drops a cached artifact, releasing its handles and local cache directory. */
    public void evict(String indexId) {
        HnswIndexArtifact artifact = open.remove(indexId);
        if (artifact != null) {
            artifact.close();
            log.info("Evicted index {}", indexId);
        }
    }

    public int openCount() {
        return open.size();
    }

    @PreDestroy
    public void closeAll() {
        open.values().forEach(HnswIndexArtifact::close);
        open.clear();
        log.info("Closed all cached index artifacts");
    }
}
