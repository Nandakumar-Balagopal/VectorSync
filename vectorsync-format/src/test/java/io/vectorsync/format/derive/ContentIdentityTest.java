package io.vectorsync.format.derive;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Content and configuration identity are the two keys the entire store is addressed by, so a defect
 * here does not cause a wrong answer: it causes the wrong vector to be served under the right name,
 * or a re-embedding campaign to silently cost full price.
 */
class ContentIdentityTest {

    @Test
    @DisplayName("identical assembled text hashes identically regardless of which row it came from")
    void sameContentSameHash() {
        // The whole dedup claim in one assertion: two unrelated rows carrying the same text are
        // one unit of work, not two.
        String a = ContentHash.of(List.of("Wireless Speaker", "Rich low-end response."), " ");
        String b = ContentHash.of(List.of("Wireless Speaker", "Rich low-end response."), " ");
        assertEquals(a, b);
    }

    @Test
    @DisplayName("column boundaries cannot be forged by moving text across the separator")
    void separatorPreventsBoundaryCollisions() {
        // Without a separator, ("ab","c") and ("a","bc") assemble to the same string and two
        // genuinely different rows would collapse onto one vector.
        assertNotEquals(
                ContentHash.of(List.of("ab", "c"), "|"),
                ContentHash.of(List.of("a", "bc"), "|"));
    }

    @Test
    @DisplayName("column order changes the content, because it changes what the model reads")
    void orderMatters() {
        assertNotEquals(
                ContentHash.of(List.of("title", "body"), " "),
                ContentHash.of(List.of("body", "title"), " "));
    }

    @Test
    @DisplayName("a null column value is the same content as an empty one")
    void nullIsEmpty() {
        // An embedder cannot tell the difference, so the store should not either -- otherwise the
        // same effective text occupies two entries.
        assertEquals(
                ContentHash.of(List.of("title", ""), " "),
                ContentHash.of(java.util.Arrays.asList("title", null), " "));
    }

    @Test
    @DisplayName("the hash is full-width sha256 and the partition prefix is its first two characters")
    void hashShape() {
        String hash = ContentHash.of("anything");
        assertEquals(64, hash.length(), "truncating a dedup key across billions invites collisions");
        assertEquals(hash.substring(0, 2), ContentHash.prefix(hash));
        assertEquals("00", ContentHash.prefix(null), "a missing hash must not throw on the write path");
    }

    @Test
    @DisplayName("the store key separates content from model and configuration")
    void storeKeyComponents() {
        assertNotEquals(
                ContentHash.storeKey("hash", "modelA:v1", "cfg1"),
                ContentHash.storeKey("hash", "modelB:v1", "cfg1"),
                "the same text under a different model is a different vector");
        assertNotEquals(
                ContentHash.storeKey("hash", "modelA:v1", "cfg1"),
                ContentHash.storeKey("hash", "modelA:v1", "cfg2"));
    }

    @Test
    @DisplayName("config id changes when anything that affects the output changes")
    void configIdCoversEveryInput() {
        MaterializationSpec.Builder base = MaterializationSpec.builder()
                .sourceTable("default.products")
                .keyColumns(List.of("id"))
                .embeddingColumns(List.of("name", "description"))
                .joinSeparator(" ")
                .chunker("whole")
                .modelName("all-MiniLM-L6-v2")
                .modelRevision("abc123")
                .embeddingVersion("v1");

        String reference = base.build().configId();

        // Each of these produces genuinely different vectors, so each must produce a different id.
        // A missed input means two configurations share a key and the store serves one caller the
        // other's vectors.
        assertNotEquals(reference, base.embeddingColumns(List.of("description", "name")).build().configId());
        base.embeddingColumns(List.of("name", "description"));
        assertNotEquals(reference, base.joinSeparator(" | ").build().configId());
        base.joinSeparator(" ");
        assertNotEquals(reference, base.chunker("fixed").chunkSize(256).chunkOverlap(32).build().configId());
        base.chunker("whole").chunkSize(0).chunkOverlap(0);
        assertNotEquals(reference, base.modelName("all-mpnet-base-v2").build().configId());
        base.modelName("all-MiniLM-L6-v2");
        assertNotEquals(reference, base.modelRevision("def456").build().configId(),
                "a model name is not a version; weights change under a fixed name");
        base.modelRevision("abc123");
        assertNotEquals(reference, base.normalize(true).build().configId());
        base.normalize(false);

        assertEquals(reference, base.build().configId(), "restoring every field restores the id");
    }

    @Test
    @DisplayName("config id ignores the source table and key columns, so dedup crosses tables")
    void configIdIsIndependentOfWhereContentLives() {
        // This is the cross-table deduplication property, and it is easy to destroy by accident:
        // folding the table name into the id gives every table a private key space, so identical
        // text in two tables is embedded twice. Measured before this was fixed: a 2-table corpus
        // with 5 distinct texts across 100 rows cost 10 inference calls rather than 5.
        MaterializationSpec.Builder base = MaterializationSpec.builder()
                .keyColumns(List.of("id"))
                .embeddingColumns(List.of("name", "description"))
                .modelName("all-MiniLM-L6-v2")
                .embeddingVersion("v1");

        String products = base.sourceTable("default.products").build().configId();
        String tickets = base.sourceTable("default.support_tickets").build().configId();
        assertEquals(products, tickets,
                "the same derivation function over a different table is the same function");

        String composite = base.sourceTable("default.products")
                .keyColumns(List.of("tenant", "id")).build().configId();
        assertEquals(products, composite,
                "key columns identify rows, not vectors");
    }

    @Test
    @DisplayName("config id is stable across rebuilds of the same spec")
    void configIdIsDeterministic() {
        // Reproducibility depends on this: an id that varied per process would make the store
        // unreadable across restarts.
        assertEquals(spec().configId(), spec().configId());
        assertEquals(16, spec().configId().length());
    }

    @Test
    @DisplayName("a spec without key columns is refused rather than guessed at")
    void keyColumnsRequired() {
        // The replaced implementation looked for a column literally named "id" and threw at
        // materialization time on any table that did not have one.
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> MaterializationSpec.builder()
                        .sourceTable("default.products")
                        .embeddingColumns(List.of("name"))
                        .modelName("m")
                        .build());
        assertTrue(thrown.getMessage().contains("keyColumns"));
    }

    @Test
    @DisplayName("a spec without embedding columns is refused")
    void embeddingColumnsRequired() {
        assertThrows(IllegalArgumentException.class, () -> MaterializationSpec.builder()
                .sourceTable("default.products")
                .keyColumns(List.of("id"))
                .modelName("m")
                .build());
    }

    @Test
    @DisplayName("model version is the partition value the store is keyed by")
    void modelVersionShape() {
        assertEquals("all-MiniLM-L6-v2:v1", spec().modelVersion());
    }

    private static MaterializationSpec spec() {
        return MaterializationSpec.builder()
                .sourceTable("default.products")
                .keyColumns(List.of("id"))
                .embeddingColumns(List.of("name", "description"))
                .modelName("all-MiniLM-L6-v2")
                .embeddingVersion("v1")
                .build();
    }

    // ------------------------------------------------------- coverage digest
    //
    // The digest is what lets a derived index be identified by the content it covers instead of by
    // the source snapshot it was built from. Every property below is load-bearing for that: if the
    // digest responds to anything other than the SET of content, then a layout-only rewrite of the
    // source changes it, and the index has to be rebuilt for a change that touched no embedding.

    @Test
    @DisplayName("coverage digest ignores order, because scan order is not a property of the data")
    void coverageDigestIsOrderIndependent() {
        List<String> ascending = List.of("aa", "bb", "cc");
        List<String> descending = List.of("cc", "bb", "aa");

        assertEquals(ContentHash.coverageDigest(ascending), ContentHash.coverageDigest(descending),
                "the same content read back in a different file order looked like different "
                        + "coverage, which would force a rebuild after any reorganisation");
    }

    @Test
    @DisplayName("coverage digest ignores duplicates, because Tier 1 is append-only")
    void coverageDigestIsDuplicateInsensitive() {
        // Two derive passes writing the same content leave two rows for one hash. That is a
        // legitimate state of an append-only store and must not change what the index covers.
        assertEquals(
                ContentHash.coverageDigest(List.of("aa", "bb")),
                ContentHash.coverageDigest(List.of("aa", "bb", "aa", "bb", "bb")),
                "duplicate rows for one content hash changed the coverage digest");
    }

    @Test
    @DisplayName("coverage digest changes when the content set changes")
    void coverageDigestRespondsToContent() {
        String base = ContentHash.coverageDigest(List.of("aa", "bb"));

        assertNotEquals(base, ContentHash.coverageDigest(List.of("aa", "bb", "cc")),
                "adding content did not change the digest, so new content would never be indexed");
        assertNotEquals(base, ContentHash.coverageDigest(List.of("aa")),
                "removing content did not change the digest, so deletions would never be applied");
        assertNotEquals(base, ContentHash.coverageDigest(List.of("aa", "bc")),
                "substituting content did not change the digest");
    }

    @Test
    @DisplayName("covering nothing is distinguishable from never having been recorded")
    void emptyCoverageHasItsOwnDigest() {
        String empty = ContentHash.coverageDigest(List.of());

        assertTrue(empty != null && !empty.isBlank(), "an empty scope produced no digest");
        assertEquals(empty, ContentHash.coverageDigest(null),
                "null and empty must agree; they are the same statement about coverage");
        assertNotEquals(empty, ContentHash.coverageDigest(List.of("")),
                "a scope covering one empty-string hash is not a scope covering nothing");
    }
}
