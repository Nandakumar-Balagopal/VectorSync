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

    /**
     * How far through its lifecycle this status is.
     *
     * <p>Used to break ties when two manifest rows for one index id carry the same timestamp,
     * which happens for the BUILDING and terminal rows of a single build. Without this the winner
     * depends on scan order and a completed index can read back as still BUILDING.
     */
    public int progressionRank() {
        return switch (this) {
            case BUILDING -> 0;
            case FAILED -> 1;
            case READY -> 2;
            case ARCHIVED -> 3;
        };
    }

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
