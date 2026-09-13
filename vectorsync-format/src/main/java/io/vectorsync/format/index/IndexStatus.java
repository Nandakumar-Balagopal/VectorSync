package io.vectorsync.format.index;

/**
 * Lifecycle of an index artifact. Recorded on the manifest entry so a partially built index is
 * never mistaken for a servable one.
 */
public enum IndexStatus {

    /** A build has started; artifact files are incomplete and must not be served. */
    BUILDING,

    /** Artifact is complete and servable, but not necessarily promoted. */
    READY,

    /** Build failed; {@code errorMessage} carries the reason. */
    FAILED,

    /** Superseded and retained for rollback or audit. */
    ARCHIVED;

    public static IndexStatus parse(String value) {
        if (value == null || value.isBlank()) {
            return FAILED;
        }
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return FAILED;
        }
    }
}
