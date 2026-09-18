package io.vectorsync.format.derive;

import io.vectorsync.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Collapsing the append-only content map into "what is live now" is the one piece of read logic the
 * whole system's correctness rests on: get it wrong and a deleted row is served, or an edit is not.
 *
 * <p>The resolution used to materialize every version of every row into a list, sort it, and then
 * overwrite into a map. That is O(all history) in heap -- unbounded by construction, because the
 * table is append-only -- and it is the measured OOM in this system. It now collapses while the scan
 * streams. A rewrite of the one piece of logic nothing can afford to have wrong needs a proof that
 * it is behaviour-preserving, so {@link #matchesTheMaterializingReference} encodes the old algorithm
 * inline as an oracle and asserts the two agree.
 *
 * <p>Runs against a real Iceberg table on the local filesystem, because the property under test
 * involves scan order: the streaming merge sees rows in whatever order Iceberg hands them back,
 * which is not the order they were written and not the order they must resolve in.
 */
class ContentMapCollapseTest {

    private static final String NAMESPACE = "vector";
    private static final String SOURCE_TABLE = "default.products";
    private static final String OTHER_TABLE = "default.support_tickets";
    private static final String CONFIG_ID = "0123456789abcdef";
    private static final String MODEL_VERSION = "all-MiniLM-L6-v2:v1";

    /**
     * The comparator the production code orders by, restated here rather than shared. A test that
     * imported the production comparator could not detect a change to it, and the tie-break fields
     * are exactly what a streaming merge is easy to get wrong.
     */
    private static final Comparator<ContentMapEntry> OLDEST_FIRST =
            Comparator.comparingLong(ContentMapEntry::getSourceSequenceNumber)
                    .thenComparingLong(ContentMapEntry::getSourceCommittedAtMillis)
                    .thenComparing(ContentMapEntry::getCreatedAt,
                            Comparator.nullsFirst(Comparator.naturalOrder()));

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;
    private Table contentMap;

    @BeforeEach
    void setUp() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        contentMap = ContentMap.loadOrCreate(catalog, NAMESPACE);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    private static ContentMapEntry live(String rowId, int ordinal, String hash, long sequence) {
        return entry(SOURCE_TABLE, rowId, ordinal, hash, sequence, false);
    }

    private static ContentMapEntry tombstone(String rowId, int ordinal, long sequence) {
        return entry(SOURCE_TABLE, rowId, ordinal, null, sequence, true);
    }

    private static ContentMapEntry entry(String sourceTable,
                                         String rowId,
                                         int ordinal,
                                         String hash,
                                         long sequence,
                                         boolean deleted) {
        return ContentMapEntry.builder()
                .sourceTable(sourceTable)
                .sourceRowId(rowId)
                .chunkOrdinal(ordinal)
                .contentHash(hash)
                .configId(CONFIG_ID)
                .modelVersion(MODEL_VERSION)
                // Deliberately descending as the sequence number ascends. Iceberg snapshot ids are
                // random longs, and ordering history by them instead of by the sequence number is a
                // bug this repository has fixed three times; an implementation that slipped back to
                // comparing snapshot ids resolves every test below to the wrong entry.
                .sourceSnapshotId(Long.MAX_VALUE - sequence * 7919L)
                .sourceSequenceNumber(sequence)
                .sourceCommittedAtMillis(1_700_000_000_000L + sequence * 1_000L)
                // Explicit, so no assertion here depends on wall-clock resolution or on two entries
                // written in the same millisecond tying.
                .createdAt(Instant.ofEpochSecond(1_700_000_000L + sequence))
                .deleted(deleted)
                .build();
    }

    /** One commit per call, so history lands in several data files and the scan order is not write order. */
    private void append(ContentMapEntry... entries) {
        ContentMap.append(contentMap, List.of(entries));
    }

    /** The live mapping as {@code chunkKey -> contentHash}; order is deliberately not asserted. */
    private Map<String, String> collapsed(long asOfSourceSequenceNumber) {
        Map<String, String> byChunk = new LinkedHashMap<>();
        for (ContentMapEntry entry :
                ContentMap.liveEntriesAsOf(contentMap, SOURCE_TABLE, CONFIG_ID, asOfSourceSequenceNumber)) {
            byChunk.put(entry.chunkKey(), entry.getContentHash());
        }
        return byChunk;
    }

    private Map<String, String> collapsed() {
        return collapsed(Long.MAX_VALUE);
    }

    /**
     * The pre-rewrite algorithm, verbatim: read every version of every row into a list, sort it
     * oldest-first, overwrite into a map so the last write per chunk wins, and only then drop
     * tombstones. This is the oracle the streaming implementation has to agree with.
     */
    private Map<String, String> materializingReference(long asOfSourceSequenceNumber) {
        Expression filter = Expressions.and(
                Expressions.equal(Constants.SOURCE_TABLE_COLUMN, SOURCE_TABLE),
                Expressions.equal(Constants.CONFIG_ID_COLUMN, CONFIG_ID));

        List<ContentMapEntry> history = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(contentMap).where(filter).build()) {
            for (Record row : rows) {
                ContentMapEntry entry = ContentMap.fromRecord(row);
                if (entry.getSourceSequenceNumber() <= asOfSourceSequenceNumber) {
                    history.add(entry);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not read the content map", e);
        }

        Map<String, ContentMapEntry> newestByChunk = new LinkedHashMap<>();
        history.stream()
                .sorted(OLDEST_FIRST)
                .forEach(entry -> newestByChunk.put(entry.chunkKey(), entry));

        Map<String, String> live = new LinkedHashMap<>();
        newestByChunk.values().stream()
                .filter(ContentMapEntry::isLive)
                .forEach(entry -> live.put(entry.chunkKey(), entry.getContentHash()));
        return live;
    }

    private static String key(String rowId, int ordinal) {
        return String.join("::", SOURCE_TABLE, rowId, Integer.toString(ordinal), CONFIG_ID);
    }

    @Test
    @DisplayName("three versions of one chunk resolve to the newest sequence number")
    void newestVersionWins() {
        append(live("r-1", 0, "hash-v1", 10L));
        append(live("r-1", 0, "hash-v2", 20L));
        append(live("r-1", 0, "hash-v3", 30L));

        assertEquals(Map.of(key("r-1", 0), "hash-v3"), collapsed());
    }

    @Test
    @DisplayName("a tombstone at a higher sequence number hides the earlier live entry")
    void tombstoneHidesEarlierLiveEntry() {
        // The tombstone is written first so that the scan is likely to see it before the live entry
        // it supersedes. A merge that dropped tombstones during the scan, or that kept the first
        // entry it saw, resurrects a deleted row here.
        append(tombstone("r-2", 0, 40L));
        append(live("r-2", 0, "hash-deleted", 10L));

        assertTrue(collapsed().isEmpty(), "a deleted row must not be resurrected by its own history");
    }

    @Test
    @DisplayName("a live entry at a higher sequence number un-hides a previously tombstoned chunk")
    void reinsertAfterDeleteBecomesLiveAgain() {
        append(live("r-3", 0, "hash-original", 10L));
        append(tombstone("r-3", 0, 20L));
        append(live("r-3", 0, "hash-reinserted", 30L));

        assertEquals(Map.of(key("r-3", 0), "hash-reinserted"), collapsed(),
                "a delete followed by a re-insert is live, at the re-inserted content");
    }

    @Test
    @DisplayName("an as-of read returns the version that was live at that sequence number")
    void asOfReturnsTheVersionLiveAtThatPoint() {
        append(live("r-4", 0, "hash-v1", 10L));
        append(live("r-4", 0, "hash-v2", 20L));
        append(tombstone("r-4", 0, 30L));

        assertEquals(Map.of(key("r-4", 0), "hash-v1"), collapsed(10L));
        assertEquals(Map.of(key("r-4", 0), "hash-v2"), collapsed(20L));
        assertEquals(Map.of(key("r-4", 0), "hash-v2"), collapsed(29L),
                "nothing happened between 20 and 29, so the answer is still the version from 20");
        assertTrue(collapsed(30L).isEmpty());
        assertTrue(collapsed(5L).isEmpty(), "before the row existed there is nothing to return");
    }

    @Test
    @DisplayName("two chunks of the same row resolve independently")
    void chunksOfOneRowResolveIndependently() {
        append(live("r-5", 0, "hash-chunk0-v1", 10L));
        append(live("r-5", 1, "hash-chunk1-v1", 10L));
        // The row shrank to one chunk: ordinal 1 is tombstoned while ordinal 0 is re-asserted.
        append(live("r-5", 0, "hash-chunk0-v2", 20L));
        append(tombstone("r-5", 1, 20L));

        assertEquals(Map.of(key("r-5", 0), "hash-chunk0-v2"), collapsed());
        assertEquals(
                Map.of(key("r-5", 0), "hash-chunk0-v1", key("r-5", 1), "hash-chunk1-v1"),
                collapsed(10L),
                "at sequence 10 the row still had two chunks");
    }

    @Test
    @DisplayName("another source table's history under the same config id is not returned")
    void scopedToTheRequestedSourceTable() {
        // config_id excludes the source table on purpose, so cross-table rows share a partition
        // value for the configuration and only the source_table predicate separates them.
        append(live("r-6", 0, "hash-ours", 10L));
        append(entry(OTHER_TABLE, "r-6", 0, "hash-theirs", 20L, false));

        assertEquals(Map.of(key("r-6", 0), "hash-ours"), collapsed());
    }

    @Test
    @DisplayName("the streaming collapse returns exactly what the materializing reference does")
    void matchesTheMaterializingReference() {
        // A history with every shape that can change the answer: repeated edits, a delete, a
        // re-insert after that delete, a chunk that only ever existed at an early version, two
        // entries at the same sequence number differing only in the tie-break fields, and rows
        // whose write order is unrelated to their sequence order.
        append(live("a", 0, "hash-a-10", 10L),
                live("a", 1, "hash-a1-10", 10L),
                live("b", 0, "hash-b-10", 10L));
        append(tombstone("a", 1, 20L),
                live("c", 0, "hash-c-20", 20L));
        append(live("a", 0, "hash-a-40", 40L),
                tombstone("b", 0, 40L));
        append(live("b", 0, "hash-b-60", 60L),
                tombstone("c", 0, 60L));
        // Same source version materialized twice: identical sequence number, later commit time.
        // This is the tie the comparator's second key exists to break, and the case where a merge
        // that compared only the sequence number would pick the other entry.
        append(live("d", 0, "hash-d-first", 30L));
        append(ContentMapEntry.builder()
                .sourceTable(SOURCE_TABLE)
                .sourceRowId("d")
                .chunkOrdinal(0)
                .contentHash("hash-d-rematerialized")
                .configId(CONFIG_ID)
                .modelVersion(MODEL_VERSION)
                .sourceSnapshotId(Long.MAX_VALUE - 30L * 7919L)
                .sourceSequenceNumber(30L)
                .sourceCommittedAtMillis(1_700_000_000_000L + 30L * 1_000L + 500L)
                .createdAt(Instant.ofEpochSecond(1_700_000_000L + 30L, 500_000_000))
                .deleted(false)
                .build());

        for (long asOf : new long[]{5L, 10L, 20L, 30L, 39L, 40L, 59L, 60L, Long.MAX_VALUE}) {
            assertEquals(materializingReference(asOf), collapsed(asOf),
                    "streaming collapse disagrees with the materializing reference at asOf " + asOf);
        }

        // The comparison above is only as good as the reference, so the answer is also pinned to a
        // literal: a reference that silently returned nothing would otherwise agree with an
        // implementation that also returned nothing.
        assertEquals(
                Map.of(key("a", 0), "hash-a-40",
                        key("b", 0), "hash-b-60",
                        key("d", 0), "hash-d-rematerialized"),
                collapsed(),
                "the rematerialized entry at the same sequence number must win on commit time");
    }
}
