-- The authoritative record of which content has a durable embedding.
--
-- Replaces a process-local HashSet whose positives could not be falsified. That set assumed every
-- commit its process believed it made had survived; when one had not, it reported a cache hit
-- forever, inference was skipped, and content_map committed a pointer to a vector that does not
-- exist -- which caps projection coverage at the unresolved sequence number and stalls the serving
-- watermark permanently, with no way to detect or repair it at runtime. It also meant the measured
-- deduplication property held only for a single worker.
--
-- A row here is written in the SAME transaction that marks a work item DONE. That is the whole
-- point: "this content has a vector" and "the work that wrote it finished" cannot disagree. The
-- Iceberg appends happen before completion, so the error is one-directional -- this table can
-- under-report (costing a redundant embedding on retry, which is byte-identical and harmless) but
-- can never over-report, which would cost a missing vector.

CREATE TABLE embedded_content (
    model_version   VARCHAR(255) NOT NULL,
    config_id       VARCHAR(64)  NOT NULL,
    content_hash    VARCHAR(64)  NOT NULL,
    embedding_dim   INTEGER      NOT NULL,
    first_seen_at   TIMESTAMP(6) NOT NULL,

    -- Composite primary key in probe order. The dedup probe asks
    --   WHERE model_version = ? AND config_id = ? AND content_hash = ANY(?)
    -- so the leading columns are the equality-scoped ones and the btree answers a 500-key batch
    -- with one index scan. Ordering content_hash first would make every probe scan the whole index.
    CONSTRAINT pk_embedded_content PRIMARY KEY (model_version, config_id, content_hash)
);

-- Counting rows per scope is how an operator sees store size and how the dedup rate is reported.
-- The primary key already serves it, so no extra index is created here on purpose: this table is
-- write-heavy during a backfill and every additional index is write amplification on the hot path.

COMMENT ON TABLE embedded_content IS
    'Authoritative set of (model_version, config_id, content_hash) with a durable vector in '
    'embedding_store. Written transactionally with work-item completion.';
