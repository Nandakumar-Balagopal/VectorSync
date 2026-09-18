package io.vectorsync.worker.service.derive;

import io.vectorsync.format.derive.ContentMap;
import io.vectorsync.format.derive.ContentMapEntry;
import io.vectorsync.format.derive.EmbeddingEntry;
import io.vectorsync.format.derive.EmbeddingStore;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Answers "where did this vector come from" from the tables themselves.
 *
 * <p>Provenance is not a log. Every fact it reports is reconstructed from the artifacts a query
 * actually reads -- the content map's append-only history and the embedding store -- so it cannot
 * drift from what is being served. A separate audit log would be a second source of truth that is
 * right until it is not.
 *
 * <p>What makes this answerable at all is that the content map is a history rather than a cache. It
 * records every assertion ever made about a chunk, with the source sequence number that ordered it
 * and a tombstone when the row went away, so the question "which version superseded which, and at
 * what source version" has an answer on disk. A design that overwrote the mapping in place would
 * serve the same queries and be unable to explain any of them.
 *
 * <p>Deliberately read-only and derived. Nothing here writes, and nothing depends on it, so it
 * cannot become a correctness dependency of the derivation path.
 */
@Service
@Slf4j
public class ProvenanceService {

    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    public ProvenanceService(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    /**
     * One assertion in a chunk's history.
     *
     * @param superseded true for every entry except the one that currently wins, so a reader can
     *                   see the chain rather than having to re-derive which entry is current
     */
    public record Assertion(String contentHash,
                            boolean deleted,
                            long sourceSnapshotId,
                            long sourceSequenceNumber,
                            long sourceCommittedAtMillis,
                            Instant recordedAt,
                            boolean superseded) {
    }

    /**
     * @param served      whether this chunk currently resolves to a vector a query can return
     * @param contentHash the content this chunk resolves to now, or null when tombstoned
     * @param text        the embedded text, when the store retained it
     * @param history     every assertion ever recorded, oldest first
     */
    public record Chain(String sourceTable,
                        String sourceRowId,
                        int chunkOrdinal,
                        String configId,
                        String modelVersion,
                        boolean served,
                        String contentHash,
                        Integer embeddingDim,
                        String text,
                        Instant embeddedAt,
                        List<Assertion> history,
                        String note) {
    }

    /**
     * The full derivation chain for one chunk of one source row.
     *
     * <p>Resolves the winner from the history rather than from {@code liveEntries}, deliberately:
     * computing it here and reading it there would be two implementations of the same ordering rule,
     * and the one that matters is the one serving queries. {@link ContentMap#historyOf} returns the
     * history under the same comparator resolution uses, so the last live entry is the served one by
     * construction.
     */
    public Chain chainFor(String sourceTable, String configId, String sourceRowId, int chunkOrdinal) {
        Table contentMap = ContentMap.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (contentMap == null) {
            return absent(sourceTable, configId, sourceRowId, chunkOrdinal,
                    "nothing has been derived yet: there is no content map");
        }

        List<ContentMapEntry> history =
                ContentMap.historyOf(contentMap, sourceTable, configId, sourceRowId, chunkOrdinal);
        if (history.isEmpty()) {
            return absent(sourceTable, configId, sourceRowId, chunkOrdinal,
                    "this row and chunk have never been derived under this configuration");
        }

        ContentMapEntry winner = history.get(history.size() - 1);
        List<Assertion> assertions = new ArrayList<>(history.size());
        for (int position = 0; position < history.size(); position++) {
            ContentMapEntry entry = history.get(position);
            assertions.add(new Assertion(
                    entry.getContentHash(),
                    entry.isDeleted(),
                    entry.getSourceSnapshotId(),
                    entry.getSourceSequenceNumber(),
                    entry.getSourceCommittedAtMillis(),
                    entry.getCreatedAt(),
                    position < history.size() - 1));
        }

        if (winner.isDeleted()) {
            // A tombstone is a complete answer, not a missing one: the row existed, was derived, and
            // was retired at a known source version. Reporting it as "not found" would lose exactly
            // the fact someone asking about a disappeared result needs.
            return new Chain(sourceTable, sourceRowId, chunkOrdinal, configId,
                    winner.getModelVersion(), false, null, null, null, null, assertions,
                    "retired at source sequence " + winner.getSourceSequenceNumber());
        }

        Optional<EmbeddingEntry> vector = loadVector(
                winner.getContentHash(), winner.getModelVersion(), configId);

        return new Chain(sourceTable, sourceRowId, chunkOrdinal, configId,
                winner.getModelVersion(), vector.isPresent(), winner.getContentHash(),
                vector.map(EmbeddingEntry::getEmbeddingDim).orElse(null),
                vector.map(EmbeddingEntry::getText).orElse(null),
                vector.map(EmbeddingEntry::getCreatedAt).orElse(null),
                assertions,
                vector.isPresent()
                        ? "live"
                        // Mapped but unresolved: the pointer is committed and the vector it names is
                        // not in the store. The derive path writes vectors before the pointers that
                        // reference them, so this is a state a retry clears rather than a dangling
                        // reference -- and it is also what caps projection coverage, so it is worth
                        // reporting as a distinct condition rather than as absence.
                        : "mapped but no vector in the store for this content and model");
    }

    private Chain absent(String sourceTable, String configId, String sourceRowId,
                         int chunkOrdinal, String note) {
        return new Chain(sourceTable, sourceRowId, chunkOrdinal, configId, null, false,
                null, null, null, null, List.of(), note);
    }

    private Optional<EmbeddingEntry> loadVector(String contentHash, String modelVersion,
                                                String configId) {
        Table store = EmbeddingStore.loadIfExists(catalogService.getCatalog(), vectorNamespace);
        if (store == null || contentHash == null) {
            return Optional.empty();
        }
        Map<String, EmbeddingEntry> loaded =
                EmbeddingStore.load(store, List.of(contentHash), modelVersion, configId);
        return Optional.ofNullable(loaded.get(contentHash));
    }
}
