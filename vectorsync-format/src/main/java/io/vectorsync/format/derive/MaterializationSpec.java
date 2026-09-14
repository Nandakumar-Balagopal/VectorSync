package io.vectorsync.format.derive;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * The declarative definition of a derived embedding dataset: what to read, how to assemble text,
 * and which model to embed it with.
 *
 * <p>This replaces the old per-table config, and the difference is not cosmetic. The old config
 * was a mutable row that could be edited in place, so changing the model silently changed what the
 * pipeline produced while the already-written vectors kept claiming the old lineage. A spec is
 * immutable and identified by {@link #configId()}: a hash over everything that can change the
 * output. Change any field and you have a different spec, a different configuration id, and a
 * different set of vectors -- side by side with the old ones rather than on top of them.
 *
 * <p>The hash must cover <em>everything</em> that affects the bytes handed to the model. Missing an
 * input means two genuinely different configurations share an id, and the store then serves a
 * vector produced under one configuration to a caller who asked for the other. The old
 * {@code preprocessingId} hashed only the column list and a separator, which is why it could not
 * support the reproducibility claim.
 */
public final class MaterializationSpec {

    private final String sourceTable;
    private final List<String> keyColumns;
    private final List<String> embeddingColumns;
    private final String joinSeparator;
    private final String chunker;
    private final int chunkSize;
    private final int chunkOverlap;
    private final String modelName;
    private final String modelRevision;
    private final String embeddingVersion;
    private final boolean normalize;

    private MaterializationSpec(Builder builder) {
        this.sourceTable = require(builder.sourceTable, "sourceTable");
        this.keyColumns = List.copyOf(builder.keyColumns);
        this.embeddingColumns = List.copyOf(builder.embeddingColumns);
        this.joinSeparator = builder.joinSeparator == null ? " " : builder.joinSeparator;
        this.chunker = builder.chunker == null ? "whole" : builder.chunker;
        this.chunkSize = builder.chunkSize;
        this.chunkOverlap = builder.chunkOverlap;
        this.modelName = require(builder.modelName, "modelName");
        // Not defaulted. A model name is not a version: "all-mpnet-base-v2" identifies a family
        // whose weights can change, so a spec that does not pin a revision cannot claim to be
        // reproducible. Empty is allowed but recorded as empty, never silently filled in.
        this.modelRevision = builder.modelRevision == null ? "" : builder.modelRevision;
        this.embeddingVersion = builder.embeddingVersion == null ? "v1" : builder.embeddingVersion;
        this.normalize = builder.normalize;

        if (this.embeddingColumns.isEmpty()) {
            throw new IllegalArgumentException("embeddingColumns must not be empty");
        }
        if (this.keyColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "keyColumns must not be empty: row identity cannot be guessed from the schema");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Stable identity of the <em>derivation function</em>: 16 hex characters over a canonical
     * serialization of everything that turns text into a vector.
     *
     * <p>Deliberately excludes the source table and the key columns, and that exclusion is the
     * point. The embedding store is keyed by {@code (content_hash, model_version, config_id)}, so
     * including the table name would give every table a private key space and identical text in two
     * tables would be embedded twice -- which is precisely the cross-table deduplication this design
     * exists to provide. An early version of this method included them, and a 100-row corpus holding
     * 5 distinct texts across 2 tables cost 10 inference calls instead of 5.
     *
     * <p>Key columns are excluded for the same reason at one remove: they determine row identity,
     * which belongs to the content map, not to the vector. Changing them changes which row points at
     * a vector, never the vector itself.
     *
     * <p>Order matters and is preserved, because column order changes the assembled text. Field
     * names are included so that adding a field later cannot make an old spec collide with a new
     * one that happens to serialize to the same concatenation.
     */
    public String configId() {
        List<String> parts = new ArrayList<>();
        parts.add("cols=" + String.join(",", embeddingColumns));
        parts.add("sep=" + joinSeparator);
        parts.add("chunker=" + chunker);
        parts.add("chunkSize=" + chunkSize);
        parts.add("chunkOverlap=" + chunkOverlap);
        parts.add("model=" + modelName);
        parts.add("revision=" + modelRevision);
        parts.add("version=" + embeddingVersion);
        parts.add("normalize=" + normalize);
        return sha256(String.join(ContentHash.SEPARATOR, parts)).substring(0, 16);
    }

    /** {@code model:version}, the partition value the embedding store and indexes are keyed by. */
    public String modelVersion() {
        return modelName + ":" + embeddingVersion;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public List<String> getEmbeddingColumns() {
        return embeddingColumns;
    }

    public String getJoinSeparator() {
        return joinSeparator;
    }

    public String getChunker() {
        return chunker;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getChunkOverlap() {
        return chunkOverlap;
    }

    public String getModelName() {
        return modelName;
    }

    public String getModelRevision() {
        return modelRevision;
    }

    public String getEmbeddingVersion() {
        return embeddingVersion;
    }

    public boolean isNormalize() {
        return normalize;
    }

    private static String require(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private static String sha256(String value) {
        try {
            byte[] hashed = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hashed.length * 2);
            for (byte b : hashed) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public static final class Builder {
        private String sourceTable;
        private List<String> keyColumns = List.of();
        private List<String> embeddingColumns = List.of();
        private String joinSeparator;
        private String chunker;
        private int chunkSize;
        private int chunkOverlap;
        private String modelName;
        private String modelRevision;
        private String embeddingVersion;
        private boolean normalize;

        public Builder sourceTable(String value) {
            this.sourceTable = value;
            return this;
        }

        public Builder keyColumns(List<String> value) {
            this.keyColumns = value == null ? List.of() : value;
            return this;
        }

        public Builder embeddingColumns(List<String> value) {
            this.embeddingColumns = value == null ? List.of() : value;
            return this;
        }

        public Builder joinSeparator(String value) {
            this.joinSeparator = value;
            return this;
        }

        public Builder chunker(String value) {
            this.chunker = value;
            return this;
        }

        public Builder chunkSize(int value) {
            this.chunkSize = value;
            return this;
        }

        public Builder chunkOverlap(int value) {
            this.chunkOverlap = value;
            return this;
        }

        public Builder modelName(String value) {
            this.modelName = value;
            return this;
        }

        public Builder modelRevision(String value) {
            this.modelRevision = value;
            return this;
        }

        public Builder embeddingVersion(String value) {
            this.embeddingVersion = value;
            return this;
        }

        public Builder normalize(boolean value) {
            this.normalize = value;
            return this;
        }

        public MaterializationSpec build() {
            return new MaterializationSpec(this);
        }
    }
}
