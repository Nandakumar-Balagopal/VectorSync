package io.vectorsync.worker.service.derive;

/**
 * Outcome of deriving one batch of source rows into the content-keyed tables.
 *
 * <p>{@link #failed} is the field the caller must respect: the sync watermark may advance only when
 * {@link #complete()} is true. Advancing past a batch whose rows did not all materialize loses
 * those rows permanently, because the next cycle diffs from the advanced watermark and never sees
 * them again. {@code rowsProcessed + failed} always equals the number of source rows handed in, so
 * a partial write is visible rather than inferred.
 *
 * <p>The rest is the benchmark this architecture is judged on. {@link #dedupRate()} is the share of
 * chunks served from content already in the embedding store; {@link #inferenceCalls} is what the
 * batch actually paid for. Under row-keyed storage those two numbers were 0 and "one call per
 * changed row" by construction.
 *
 * @param rowsProcessed   source rows whose mapping entries were committed
 * @param chunksProcessed chunks derived from the rows that got as far as the dedup probe
 * @param distinctHashes  distinct content hashes among those chunks; {@code chunksProcessed} minus
 *                        this is duplication found inside the batch itself
 * @param cacheHits       chunks whose content the store already held before this batch
 * @param inferenceCalls  texts actually sent to the model -- one per novel content hash, however
 *                        many rows carried it. Counts inference paid for, so it stays non-zero if
 *                        a later write fails.
 * @param failed          source rows not committed; the watermark must hold when this is non-zero
 */
public record DeriveResult(int rowsProcessed,
                           int chunksProcessed,
                           int distinctHashes,
                           int cacheHits,
                           int inferenceCalls,
                           int failed) {

    /** Nothing to do. A legitimate, watermark-advancing outcome. */
    public static DeriveResult empty() {
        return new DeriveResult(0, 0, 0, 0, 0, 0);
    }

    public boolean complete() {
        return failed == 0;
    }

    /**
     * Share of chunks that needed no inference because the content was already embedded.
     *
     * <p>Zero rather than NaN on an empty batch: this value is logged and serialized into status
     * payloads, and a NaN both poisons any average computed over it and is not valid JSON.
     */
    public double dedupRate() {
        if (chunksProcessed == 0) {
            return 0.0;
        }
        return (double) cacheHits / chunksProcessed;
    }

    /**
     * Share of chunks that needed no inference call, from either source of saving.
     *
     * <p>This is the honest headline number, and it is not {@link #dedupRate()}. Saving comes from
     * two places: content already in the store (counted by {@code cacheHits}) and duplicate content
     * <em>within this batch</em>, which collapses to one call before the store is ever consulted.
     * A fresh table whose rows are 95% duplicates records zero cache hits -- the store was empty --
     * while making 5% of the inference calls, so reporting only {@code dedupRate} would understate
     * the result to nearly nothing on exactly the corpus the design is built for.
     */
    public double inferenceAvoidedRate() {
        if (chunksProcessed == 0) {
            return 0.0;
        }
        return 1.0 - ((double) inferenceCalls / chunksProcessed);
    }
}
