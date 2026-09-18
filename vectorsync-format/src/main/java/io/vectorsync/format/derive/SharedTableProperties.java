package io.vectorsync.format.derive;

import io.vectorsync.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Commit-retry settings for the two tables every writer shares.
 *
 * <p>{@code content_map} and {@code embedding_store} are Tier 1, which means one table each for the
 * whole warehouse: every materialization, every configuration and every parallel derive thread
 * appends to the same two tables. That is the point -- content-addressed dedup only works if all
 * derivations consult one store -- but it also makes them the most contended tables in the system
 * by construction, while Iceberg's defaults gave them no more commit-retry budget than a table with
 * a single writer.
 *
 * <p>This was not theoretical. On a run with derivation parallelism at 4, an append to
 * {@code embedding_store} lost the compare-and-set race three times in five seconds; because the
 * control plane allows a work item three attempts before failing it permanently, a transient lock
 * conflict put the materialization into DEGRADED with 2,000 rows reported as unmaterialised. The
 * data was never at risk -- the append is one commit, so a loss leaves nothing behind -- but the
 * pipeline stopped, and it stopped for the most ordinary reason a lakehouse writer encounters.
 *
 * <p>Iceberg's default of four retries is calibrated for occasional conflict, not for a table that
 * is designed to be shared. Twenty retries backing off to five seconds gives roughly a minute of
 * absorbing contention, which is longer than any commit here takes and far cheaper than a DEGRADED
 * materialization that needs an operator. Retrying a compare-and-set is also safe in a way retrying
 * most things is not: the loser re-reads the current metadata and re-applies its own appended files,
 * so a retried append adds its files once regardless of how many attempts it took.
 *
 * <p>Applied at creation, and lazily to tables that already exist -- the properties are worth
 * nothing on the warehouses that already have the problem. The lazy path commits only when the
 * setting is absent, so it is one commit per table per lifetime rather than per load.
 */
@Slf4j
public final class SharedTableProperties {

    /**
     * Marks that retry settings have been applied, so the lazy upgrade is a property read rather
     * than a commit on every load. A dedicated key rather than checking the retry values
     * themselves, so an operator who deliberately tunes them down is not overridden on next load.
     */
    public static final String TUNED_PROPERTY = "vectorsync.commit-retry-tuned";

    private SharedTableProperties() {
    }

    /** Table properties for a newly created shared table, including the format version. */
    public static Map<String, String> forCreate() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(Constants.FORMAT_VERSION_PROPERTY,
                String.valueOf(Constants.VECTOR_FORMAT_VERSION));
        properties.putAll(commitRetry());
        return properties;
    }

    private static Map<String, String> commitRetry() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(TableProperties.COMMIT_NUM_RETRIES, "20");
        properties.put(TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "100");
        // Five seconds rather than Iceberg's two, because backing off further is what actually
        // breaks a standoff between writers that keep colliding at the same cadence.
        properties.put(TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "5000");
        properties.put(TableProperties.COMMIT_TOTAL_RETRY_TIME_MS, "120000");
        properties.put(TUNED_PROPERTY, "true");
        return properties;
    }

    /**
     * Applies the retry settings to an existing table, once.
     *
     * <p>Never throws. This is a durability improvement for the next commit, so failing to apply it
     * must not fail the commit that is happening now -- and the most likely reason it fails is the
     * very contention it is meant to reduce.
     */
    public static void ensureTuned(Table table) {
        if (table == null || Boolean.parseBoolean(table.properties().get(TUNED_PROPERTY))) {
            return;
        }
        try {
            var update = table.updateProperties();
            commitRetry().forEach(update::set);
            update.commit();
            log.info("Raised commit-retry budget on shared table {}", table.name());
        } catch (Exception e) {
            log.debug("Could not tune commit retries on {}: {}", table.name(), e.getMessage());
        }
    }
}
