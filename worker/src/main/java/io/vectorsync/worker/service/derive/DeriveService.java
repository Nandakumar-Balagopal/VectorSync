package io.vectorsync.worker.service.derive;

import io.vectorsync.common.Constants;
import io.vectorsync.format.derive.ContentHash;
import io.vectorsync.format.derive.ContentMap;
import io.vectorsync.format.derive.ContentMapEntry;
import io.vectorsync.format.derive.EmbeddingEntry;
import io.vectorsync.format.derive.EmbeddingStore;
import io.vectorsync.format.derive.MaterializationSpec;
import io.vectorsync.worker.service.embedding.EmbeddingException;
import io.vectorsync.worker.service.embedding.EmbeddingRequest;
import io.vectorsync.worker.service.embedding.EmbeddingService;
import io.vectorsync.worker.service.iceberg.IcebergCatalogService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Turns source rows into content-keyed Tier-1 writes: vectors into the embedding store, row-to-content
 * pointers into the content map.
 *
 * <p>The shape of this class is the architecture. A batch is first resolved all the way down to
 * content hashes without touching a model, then asks the store <em>once</em> which of those hashes
 * it already holds, and embeds only what is left -- deduplicated, so content repeated across five
 * hundred rows costs one inference call. The mapping rows are written for every chunk regardless,
 * hit or miss, because the mapping is what makes a row discoverable.
 *
 * <p>The predecessor did the opposite: it embedded every changed row before looking at anything,
 * which meant an edit to a column that was not embedded, a re-run of a failed sync, or a row that
 * merely duplicated another row's text each paid full inference price. A model migration therefore
 * cost one call per row in the warehouse rather than one per distinct passage.
 *
 * <p>Ordering inside {@link #derive} is deliberate and load-bearing:
 * <ol>
 *   <li>vectors are appended before the pointers that reference them. A pointer committed ahead of
 *       its vector is a dangling reference that search resolves to nothing; a vector with no pointer
 *       yet is inert and becomes a cache hit on the retry.</li>
 *   <li>a row whose vector could not be written is left out of the mapping entirely and counted as
 *       failed, so the watermark holds and the row is retried rather than being recorded as synced
 *       while pointing at a vector that does not exist.</li>
 * </ol>
 */
@Service
@Slf4j
public class DeriveService {

    /**
     * Columns needed to decide which prior mapping entry for a chunk is the newest and whether it
     * is live. {@code source_row_id} is projected because it is a filter column and not a partition
     * column: Iceberg builds the residual evaluator against the projected schema, so projecting a
     * filter column away makes the scan fail with "Cannot find field" at read time.
     */
    private static final List<String> PRIOR_COLUMNS = List.of(
            Constants.SOURCE_ROW_ID_COLUMN,
            Constants.CHUNK_ORDINAL_COLUMN,
            Constants.CONTENT_HASH_COLUMN,
            Constants.DELETED_COLUMN,
            Constants.SOURCE_SEQUENCE_NUMBER_COLUMN,
            Constants.SOURCE_COMMITTED_AT_COLUMN,
            Constants.CREATED_AT_COLUMN);

    /**
     * Oldest-first over the source <em>sequence number</em>, then commit time, then write time.
     *
     * <p>Never over {@code source_snapshot_id}: Iceberg snapshot ids are random longs, so ordering
     * by one shuffles history and lets a tombstone lose to the entry it was meant to retire. That
     * bug has been fixed three times in this repository. The write-time tiebreak matters here
     * because a retried file is materialized twice at the same source version.
     */
    private static final Comparator<PriorEntry> OLDEST_FIRST =
            Comparator.comparingLong(PriorEntry::sourceSequenceNumber)
                    .thenComparingLong(PriorEntry::committedAtMillis)
                    .thenComparing(PriorEntry::createdAt,
                            Comparator.nullsFirst(Comparator.naturalOrder()));

    private final EmbeddingService embeddingService;
    private final IcebergCatalogService catalogService;

    @Value("${iceberg.vector.namespace:vector}")
    private String vectorNamespace;

    private final ContentHashIndex hashIndex;

    public DeriveService(EmbeddingService embeddingService,
                         IcebergCatalogService catalogService,
                         ContentHashIndex hashIndex) {
        this.embeddingService = embeddingService;
        this.catalogService = catalogService;
        this.hashIndex = hashIndex;
    }

    /** One source row resolved to content hashes, before anything has been embedded. */
    private record PlannedChunk(int chunkOrdinal, String contentHash, String text) {
    }

    private record RowPlan(String sourceRowId, List<PlannedChunk> chunks) {
    }

    /** A prior mapping assertion, reduced to what supersession needs to decide. */
    private record PriorEntry(String sourceRowId,
                              int chunkOrdinal,
                              boolean live,
                              long sourceSequenceNumber,
                              long committedAtMillis,
                              Instant createdAt) {
    }

    /**
     * Derives one batch of source rows at a single source version.
     *
     * @param spec              immutable configuration; supplies the columns, the chunker and the model
     * @param sourceRows        rows read from the source table, all belonging to {@code snapshotId}
     * @param snapshotId        source snapshot these rows came from; recorded for provenance only
     * @param sequenceNumber    Iceberg sequence number of that snapshot -- the only ordered version
     *                          field, and what history resolution uses
     * @param committedAtMillis commit time of that snapshot
     * @return counts for the batch; the caller advances the watermark only on
     *         {@link DeriveResult#complete()}
     */
    public DeriveResult derive(MaterializationSpec spec,
                               List<Record> sourceRows,
                               long snapshotId,
                               long sequenceNumber,
                               long committedAtMillis) {
        if (spec == null) {
            throw new IllegalArgumentException("spec is required");
        }
        if (sourceRows == null || sourceRows.isEmpty()) {
            return DeriveResult.empty();
        }

        // Resolved once per batch: an unknown chunker or an impossible window is a configuration
        // error, and discovering it per row would report five hundred identical failures.
        Chunker chunker = Chunker.forSpec(spec);
        String configId = spec.configId();
        String modelVersion = spec.modelVersion();

        List<RowPlan> plans = new ArrayList<>(sourceRows.size());
        int planFailures = 0;
        for (Record row : sourceRows) {
            try {
                plans.add(plan(spec, chunker, row));
            } catch (Exception e) {
                planFailures++;
                // Message only, no stack trace: a batch almost always fails for one reason (a
                // renamed column), and hundreds of identical traces bury the line that says which.
                log.error("Skipping row from {}: {}", spec.getSourceTable(), e.getMessage());
            }
        }

        // Distinct content, in first-seen order so the request batch is reproducible across runs.
        Map<String, String> textByHash = new LinkedHashMap<>();
        Map<String, String> firstRowByHash = new HashMap<>();
        int chunksProcessed = 0;
        for (RowPlan plan : plans) {
            for (PlannedChunk chunk : plan.chunks()) {
                chunksProcessed++;
                textByHash.putIfAbsent(chunk.contentHash(), chunk.text());
                firstRowByHash.putIfAbsent(chunk.contentHash(), plan.sourceRowId());
            }
        }
        int distinctHashes = textByHash.size();

        if (plans.isEmpty()) {
            return new DeriveResult(0, 0, 0, 0, 0, sourceRows.size());
        }

        Catalog catalog = catalogService.getCatalog();
        Table store = EmbeddingStore.loadOrCreate(catalog, vectorNamespace);
        Table contentMap = ContentMap.loadOrCreate(catalog, vectorNamespace);

        Set<String> alreadyEmbedded;
        try {
            // Through the index rather than straight to Iceberg: the store probe is a full scan of
            // the matching partitions at realistic batch sizes, which made backfill cost quadratic
            // in store size. See ContentHashIndex for the measurements.
            alreadyEmbedded = hashIndex.findExisting(
                    store, textByHash.keySet(), modelVersion, configId);
        } catch (Exception e) {
            // Without a trustworthy answer the only safe moves are to re-embed everything or to
            // write nothing. Writing nothing is cheaper and the watermark holds, so the batch is
            // retried intact once the store is readable again.
            log.error("Dedup probe failed for {} / {}; holding the batch: {}",
                    spec.getSourceTable(), configId, e.getMessage(), e);
            return new DeriveResult(0, chunksProcessed, distinctHashes, 0, 0, sourceRows.size());
        }

        int cacheHits = 0;
        for (RowPlan plan : plans) {
            for (PlannedChunk chunk : plan.chunks()) {
                if (alreadyEmbedded.contains(chunk.contentHash())) {
                    cacheHits++;
                }
            }
        }

        Map<String, String> novelByHash = new LinkedHashMap<>();
        textByHash.forEach((hash, text) -> {
            if (!alreadyEmbedded.contains(hash)) {
                novelByHash.put(hash, text);
            }
        });

        // Hashes with no vector in the store once this batch is done. Rows referencing any of them
        // must not be mapped, or search would resolve them to nothing.
        Set<String> unavailable = new HashSet<>();
        List<DeriveResult.Written> written = new ArrayList<>();
        int inferenceCalls = 0;

        if (!novelByHash.isEmpty()) {
            List<EmbeddingEntry> newVectors = List.of();
            try {
                newVectors = embed(spec, modelVersion, configId, novelByHash, firstRowByHash);
                inferenceCalls = newVectors.size();
            } catch (Exception e) {
                // The provider batch is all-or-nothing, so nothing from it is trusted.
                log.error("Embedding {} novel chunks failed for {}: {}",
                        novelByHash.size(), spec.getSourceTable(), e.getMessage(), e);
                unavailable.addAll(novelByHash.keySet());
            }

            if (!newVectors.isEmpty()) {
                try {
                    EmbeddingStore.append(store, newVectors);
                    // Collected, not recorded. The durable record of these hashes is written by the
                    // control plane in the same transaction that marks this work item complete, so
                    // "the vector exists" and "the work finished" cannot be observed separately.
                    for (EmbeddingEntry entry : newVectors) {
                        written.add(new DeriveResult.Written(
                                entry.getContentHash(), entry.getEmbeddingDim()));
                    }
                } catch (Exception e) {
                    // A single commit, so a failure means none of these vectors landed. Inference
                    // was still paid for and stays counted; the retry re-embeds only these hashes.
                    log.error("Appending {} vectors to the embedding store failed for {}: {}",
                            newVectors.size(), spec.getSourceTable(), e.getMessage(), e);
                    unavailable.addAll(novelByHash.keySet());
                }
            }
        }

        List<ContentMapEntry> mapEntries = new ArrayList<>(chunksProcessed);
        Map<String, Integer> writtenChunkCounts = new LinkedHashMap<>();
        // Counted per plan rather than taken from the size of writtenChunkCounts. Key columns that
        // are not in fact unique put the same row id in one batch twice, and a distinct-row-id
        // count subtracted from the batch size then reports phantom failures on every attempt: the
        // pass never reaches complete(), so the watermark never advances and the table stops
        // syncing with nothing in the logs to say why.
        int rowsProcessed = 0;
        int heldBack = 0;
        for (RowPlan plan : plans) {
            boolean dangling = plan.chunks().stream()
                    .anyMatch(chunk -> unavailable.contains(chunk.contentHash()));
            if (dangling) {
                heldBack++;
                continue;
            }

            for (PlannedChunk chunk : plan.chunks()) {
                mapEntries.add(entry(spec, modelVersion, configId, plan.sourceRowId(),
                        chunk.chunkOrdinal(), chunk.contentHash(), false,
                        snapshotId, sequenceNumber, committedAtMillis));
            }
            rowsProcessed++;
            // Highest count wins when a row id repeats in the batch, so supersession never
            // tombstones an ordinal that this same commit also writes live: the two entries would
            // share a sequence number and commit time, leaving write time to pick a winner.
            writtenChunkCounts.merge(plan.sourceRowId(), plan.chunks().size(), Math::max);
        }

        try {
            mapEntries.addAll(supersededTombstones(contentMap, spec, modelVersion, configId,
                    chunker, writtenChunkCounts, snapshotId, sequenceNumber, committedAtMillis));
            ContentMap.append(contentMap, mapEntries);
        } catch (Exception e) {
            // One commit for the batch and its tombstones together, so a failure leaves the
            // mapping exactly as it was. The vectors already appended are not garbage: the retry
            // finds them through the dedup probe and pays no inference for them a second time.
            log.error("Appending {} content map entries failed for {}: {}",
                    mapEntries.size(), spec.getSourceTable(), e.getMessage(), e);
            return new DeriveResult(0, chunksProcessed, distinctHashes, cacheHits,
                    inferenceCalls, sourceRows.size(), written);
        }

        DeriveResult result = new DeriveResult(rowsProcessed, chunksProcessed, distinctHashes,
                cacheHits, inferenceCalls, planFailures + heldBack, written);

        log.info("Derived {} / {}: {} rows, {} chunks, {} distinct, {} cache hits ({} dedup), "
                        + "{} inference calls, {} failed",
                spec.getSourceTable(), configId, result.rowsProcessed(), result.chunksProcessed(),
                result.distinctHashes(), result.cacheHits(),
                String.format("%.1f%%", result.dedupRate() * 100), result.inferenceCalls(),
                result.failed());
        return result;
    }

    /**
     * Records source rows as deleted, tombstoning every live chunk they currently have.
     *
     * <p>Only the content map is touched. Deleting from the embedding store would be wrong on both
     * counts: the vector is keyed by content, so it is very likely shared with rows that still exist
     * (and with this row's future, if the same text comes back), and the store is immutable history
     * by design. A delete is the withdrawal of a pointer, not the destruction of a fact.
     *
     * @return counts with {@code rowsProcessed} set to the rows tombstoned
     */
    public DeriveResult tombstone(MaterializationSpec spec,
                                  Collection<String> sourceRowIds,
                                  long snapshotId,
                                  long sequenceNumber,
                                  long committedAtMillis) {
        if (spec == null) {
            throw new IllegalArgumentException("spec is required");
        }
        if (sourceRowIds == null || sourceRowIds.isEmpty()) {
            return DeriveResult.empty();
        }

        Set<String> rowIds = new LinkedHashSet<>(sourceRowIds);
        String configId = spec.configId();
        String modelVersion = spec.modelVersion();

        try {
            Table contentMap = ContentMap.loadOrCreate(catalogService.getCatalog(), vectorNamespace);
            Map<String, Set<Integer>> liveOrdinals =
                    liveOrdinals(contentMap, spec.getSourceTable(), configId, rowIds);

            List<ContentMapEntry> tombstones = new ArrayList<>();
            for (String rowId : rowIds) {
                Set<Integer> ordinals = liveOrdinals.get(rowId);
                if (ordinals == null || ordinals.isEmpty()) {
                    // A delete that arrives before the row was ever materialized still gets one
                    // tombstone at ordinal 0, so it is recorded rather than dropped -- otherwise a
                    // later out-of-order insert would resurrect the row.
                    tombstones.add(entry(spec, modelVersion, configId, rowId, 0, null, true,
                            snapshotId, sequenceNumber, committedAtMillis));
                    continue;
                }
                for (int ordinal : ordinals) {
                    tombstones.add(entry(spec, modelVersion, configId, rowId, ordinal, null, true,
                            snapshotId, sequenceNumber, committedAtMillis));
                }
            }

            ContentMap.append(contentMap, tombstones);
            return new DeriveResult(rowIds.size(), tombstones.size(), 0, 0, 0, 0);
        } catch (Exception e) {
            log.error("Tombstoning {} rows of {} failed: {}",
                    rowIds.size(), spec.getSourceTable(), e.getMessage(), e);
            return new DeriveResult(0, 0, 0, 0, 0, rowIds.size());
        }
    }

    /**
     * Resolves one source row to content hashes: identity from the key columns, text from the
     * embedding columns in spec order, then chunked and hashed.
     */
    private RowPlan plan(MaterializationSpec spec, Chunker chunker, Record row) {
        String sourceRowId = rowId(spec, row);

        List<String> values = new ArrayList<>(spec.getEmbeddingColumns().size());
        for (String column : spec.getEmbeddingColumns()) {
            // Spec order, not schema order, and every column contributes even when null: the
            // assembled string is what gets hashed, so dropping a null column would shift the
            // remaining values across separator positions and change the content identity of a row
            // whose embedded text did not change.
            values.add(stringValue(row, column, spec.getSourceTable()));
        }

        String canonicalText = ContentHash.canonicalText(values, spec.getJoinSeparator());
        List<String> chunks = chunker.chunk(canonicalText);

        List<PlannedChunk> planned = new ArrayList<>(chunks.size());
        for (int ordinal = 0; ordinal < chunks.size(); ordinal++) {
            String chunk = chunks.get(ordinal);
            planned.add(new PlannedChunk(ordinal, ContentHash.of(chunk), chunk));
        }
        return new RowPlan(sourceRowId, planned);
    }

    /**
     * Row identity from the spec's key columns.
     *
     * <p>Composite keys are ordinary here. The predecessor demanded a column literally named
     * {@code id} and threw otherwise, which excluded every join table and every natural-key table
     * in the warehouse.
     */
    private String rowId(MaterializationSpec spec, Record row) {
        List<String> keyValues = new ArrayList<>(spec.getKeyColumns().size());
        for (String column : spec.getKeyColumns()) {
            String value = stringValue(row, column, spec.getSourceTable());
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException(
                        "key column '" + column + "' is null; a row with no key has no stable identity");
            }
            keyValues.add(value);
        }
        // Unit separator, never a printable one: with "-" the keys ("a-b","c") and ("a","b-c")
        // produce the same row id, so two distinct rows would overwrite each other's mapping.
        return String.join(ContentHash.SEPARATOR, keyValues);
    }

    /**
     * Reads a column as text.
     *
     * <p>A column absent from the source schema throws rather than reading as null: a typo in the
     * spec would otherwise quietly embed the wrong text, and because content identity includes that
     * text, the mistake would be baked into the hashes of every row it touched.
     *
     * <p>Types without a faithful text form are refused for the same reason. Iceberg serves
     * {@code binary} and {@code fixed} as a {@link java.nio.ByteBuffer}, whose {@code toString}
     * reports position and capacity rather than bytes, so two different values of the same length
     * would assemble to identical text and share one vector. A nested type falls back to an
     * identity-based {@code toString} that differs on every run, which misses the dedup probe
     * forever and re-embeds the row on every sync.
     */
    private String stringValue(Record row, String column, String sourceTable) {
        Types.NestedField field = row.struct().field(column);
        if (field == null) {
            throw new IllegalArgumentException(
                    "source table " + sourceTable + " has no column '" + column + "'");
        }
        Type type = field.type();
        if (!type.isPrimitiveType()
                || type.typeId() == Type.TypeID.BINARY
                || type.typeId() == Type.TypeID.FIXED) {
            throw new IllegalArgumentException("column '" + column + "' of " + sourceTable
                    + " has type " + type + ", which has no stable text form and cannot take part "
                    + "in content identity");
        }

        Object value = row.getField(column);
        if (value == null) {
            return null;
        }
        return value instanceof CharSequence text ? text.toString() : String.valueOf(value);
    }

    /**
     * Embeds novel content, one request per distinct hash.
     *
     * <p>The correlation key is the content hash, which is the whole design in one line: the
     * provider answers about content, not about rows, so five hundred rows sharing a passage
     * contribute a single request. The row id attached to the request is one arbitrary row that
     * carried the text, useful for provider-side logging and nothing else -- the resulting vector is
     * not keyed by it.
     *
     * <p>Every entry is staged before any is returned, so a hash missing from the response fails the
     * batch instead of committing a partial set of vectors that the mapping would then point past.
     */
    private List<EmbeddingEntry> embed(MaterializationSpec spec,
                                       String modelVersion,
                                       String configId,
                                       Map<String, String> novelByHash,
                                       Map<String, String> firstRowByHash) throws EmbeddingException {
        List<EmbeddingRequest> requests = new ArrayList<>(novelByHash.size());
        novelByHash.forEach((hash, text) -> requests.add(new EmbeddingRequest(
                hash,
                spec.getSourceTable(),
                firstRowByHash.get(hash),
                // The spec's model, never the provider's configured default. Sending the default
                // while recording the spec's model made the stored lineage a lie, and lineage is
                // the only thing that makes a vector reproducible.
                spec.getModelName(),
                text)));

        Map<String, List<Double>> vectors = embeddingService.generateEmbeddings(requests);

        Instant now = Instant.now();
        List<EmbeddingEntry> entries = new ArrayList<>(novelByHash.size());
        for (Map.Entry<String, String> novel : novelByHash.entrySet()) {
            List<Double> vector = vectors.get(novel.getKey());
            if (vector == null || vector.isEmpty()) {
                throw new IllegalStateException(
                        "Embedding provider returned nothing for content " + novel.getKey());
            }

            float[] embedding = toFloats(vector, spec.isNormalize());
            entries.add(EmbeddingEntry.builder()
                    .contentHash(novel.getKey())
                    .modelVersion(modelVersion)
                    .configId(configId)
                    .embeddingDim(embedding.length)
                    .embedding(embedding)
                    .text(novel.getValue())
                    .createdAt(now)
                    .build());
        }
        return entries;
    }

    /**
     * Tombstones for chunks that used to be live on a row and are not produced any more.
     *
     * <p>Text that shrinks from five chunks to three leaves ordinals 3 and 4 with no newer entry, so
     * history resolution keeps finding the stale ones live and search keeps returning passages the
     * source no longer contains. Only rows written by this batch are considered -- a row held back
     * for a failed vector must keep its current mapping until the retry succeeds.
     *
     * <p>The existing mapping is read only when the chunker can emit more than one chunk. A
     * single-chunk chunker can never have left an ordinal above 0 behind, so the common case pays
     * nothing for this.
     *
     * @return the tombstones, in source row order and then ascending ordinal, so a retried batch
     *         produces the same entries in the same order
     */
    private List<ContentMapEntry> supersededTombstones(Table contentMap,
                                                       MaterializationSpec spec,
                                                       String modelVersion,
                                                       String configId,
                                                       Chunker chunker,
                                                       Map<String, Integer> writtenChunkCounts,
                                                       long snapshotId,
                                                       long sequenceNumber,
                                                       long committedAtMillis) {
        List<ContentMapEntry> tombstones = new ArrayList<>();

        if (chunker.singleChunk()) {
            writtenChunkCounts.forEach((rowId, chunks) -> {
                // A row whose embedded columns went blank produces no chunks, which on its own
                // would leave the previous content mapped forever.
                if (chunks == 0) {
                    tombstones.add(entry(spec, modelVersion, configId, rowId, 0, null, true,
                            snapshotId, sequenceNumber, committedAtMillis));
                }
            });
            return tombstones;
        }

        Map<String, Set<Integer>> liveOrdinals = liveOrdinals(contentMap, spec.getSourceTable(),
                configId, writtenChunkCounts.keySet());
        writtenChunkCounts.forEach((rowId, chunks) -> {
            for (int ordinal : liveOrdinals.getOrDefault(rowId, Set.of())) {
                if (ordinal >= chunks) {
                    tombstones.add(entry(spec, modelVersion, configId, rowId, ordinal, null, true,
                            snapshotId, sequenceNumber, committedAtMillis));
                }
            }
        });
        return tombstones;
    }

    /**
     * Live chunk ordinals currently mapped for the given rows, keyed by row id and ascending.
     *
     * <p>Scoped to the rows in hand, with every filter pushed into the scan. Resolving the whole
     * mapping for a table and then matching row ids in Java is the defect this architecture
     * removes: the content map is append-only history holding every version of every row, so a
     * whole-table resolve grows without bound and, called once per source file, would undo the
     * file-at-a-time heap bound the orchestrator is built around. {@code source_table} and
     * {@code config_id} are identity partitions and prune files before any is opened; the row-id
     * {@code IN} prunes further on column statistics and is then exact in the residual.
     *
     * <p>History is collapsed while streaming rather than after materializing it, so heap stays
     * proportional to the rows in the batch and not to how long the table has existed.
     */
    private Map<String, Set<Integer>> liveOrdinals(Table contentMap,
                                                   String sourceTable,
                                                   String configId,
                                                   Set<String> sourceRowIds) {
        if (contentMap == null || sourceRowIds.isEmpty()) {
            return Map.of();
        }

        Map<String, Map<Integer, PriorEntry>> newestByChunk = new LinkedHashMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(contentMap)
                .where(Expressions.equal(Constants.SOURCE_TABLE_COLUMN, sourceTable))
                .where(Expressions.equal(Constants.CONFIG_ID_COLUMN, configId))
                .where(Expressions.in(Constants.SOURCE_ROW_ID_COLUMN, sourceRowIds))
                .select(PRIOR_COLUMNS)
                .build()) {

            for (Record row : rows) {
                PriorEntry candidate = priorEntry(row);
                // Guard against a wider IN match than asked for: an engine is free to satisfy an
                // IN predicate approximately and leave exact matching to the residual.
                if (candidate.sourceRowId() == null || !sourceRowIds.contains(candidate.sourceRowId())) {
                    continue;
                }
                newestByChunk
                        .computeIfAbsent(candidate.sourceRowId(), key -> new LinkedHashMap<>())
                        .merge(candidate.chunkOrdinal(), candidate,
                                (known, next) -> OLDEST_FIRST.compare(known, next) <= 0 ? next : known);
            }
        } catch (Exception e) {
            throw new IllegalStateException(String.format(
                    "Failed to read the current content map for %s / %s", sourceTable, configId), e);
        }

        Map<String, Set<Integer>> live = new LinkedHashMap<>();
        newestByChunk.forEach((rowId, byOrdinal) -> {
            // Tombstones are dropped only after the newest entry per chunk is known. Filtering
            // them out during the scan would let an older live entry win and resurrect a chunk
            // that was deleted.
            Set<Integer> ordinals = new TreeSet<>();
            byOrdinal.forEach((ordinal, entry) -> {
                if (entry.live()) {
                    ordinals.add(ordinal);
                }
            });
            if (!ordinals.isEmpty()) {
                live.put(rowId, ordinals);
            }
        });
        return live;
    }

    private static PriorEntry priorEntry(Record row) {
        Object contentHash = row.getField(Constants.CONTENT_HASH_COLUMN);
        boolean deleted = Boolean.TRUE.equals(row.getField(Constants.DELETED_COLUMN));
        // A live entry must point at content. An entry that does not is a tombstone however the
        // deleted flag reads, which is the same rule the content map enforces on write.
        boolean live = !deleted && contentHash != null && !contentHash.toString().isBlank();

        return new PriorEntry(
                asString(row.getField(Constants.SOURCE_ROW_ID_COLUMN)),
                asInt(row.getField(Constants.CHUNK_ORDINAL_COLUMN)),
                live,
                asLong(row.getField(Constants.SOURCE_SEQUENCE_NUMBER_COLUMN)),
                asLong(row.getField(Constants.SOURCE_COMMITTED_AT_COLUMN)),
                asInstant(row.getField(Constants.CREATED_AT_COLUMN)));
    }

    private static Instant asInstant(Object value) {
        if (value instanceof OffsetDateTime timestamp) {
            return timestamp.toInstant();
        }
        return value instanceof Instant instant ? instant : null;
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static long asLong(Object value) {
        return value instanceof Number number ? number.longValue() : 0L;
    }

    private static int asInt(Object value) {
        return value instanceof Number number ? number.intValue() : 0;
    }

    private ContentMapEntry entry(MaterializationSpec spec,
                                  String modelVersion,
                                  String configId,
                                  String sourceRowId,
                                  int chunkOrdinal,
                                  String contentHash,
                                  boolean deleted,
                                  long snapshotId,
                                  long sequenceNumber,
                                  long committedAtMillis) {
        return ContentMapEntry.builder()
                .sourceTable(spec.getSourceTable())
                .sourceRowId(sourceRowId)
                .chunkOrdinal(chunkOrdinal)
                .contentHash(contentHash)
                .configId(configId)
                .modelVersion(modelVersion)
                .sourceSnapshotId(snapshotId)
                .sourceSequenceNumber(sequenceNumber)
                .sourceCommittedAtMillis(committedAtMillis)
                .deleted(deleted)
                .createdAt(Instant.now())
                .build();
    }

    /**
     * Narrows the provider's doubles to the float32 the store holds, normalizing when the spec says
     * so.
     *
     * <p>Normalization is part of {@code config_id}, so a spec that declares it and stores raw
     * vectors is claiming something false about its own contents -- and search, which treats a dot
     * product as cosine similarity for normalized vectors, would silently rank by magnitude.
     * Re-normalizing an already unit-length vector is a no-op, so this is safe to apply to any
     * provider's output.
     */
    private float[] toFloats(List<Double> vector, boolean normalize) {
        double scale = 1.0;
        if (normalize) {
            double magnitude = 0.0;
            for (Double value : vector) {
                double component = value == null ? 0.0 : value;
                magnitude += component * component;
            }
            magnitude = Math.sqrt(magnitude);
            if (magnitude > 0.0) {
                scale = 1.0 / magnitude;
            }
        }

        float[] embedding = new float[vector.size()];
        for (int i = 0; i < embedding.length; i++) {
            Double value = vector.get(i);
            embedding[i] = (float) ((value == null ? 0.0 : value) * scale);
        }
        return embedding;
    }
}
