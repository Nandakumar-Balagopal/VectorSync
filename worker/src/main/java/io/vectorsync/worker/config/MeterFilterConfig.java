package io.vectorsync.worker.config;

import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Caps the cardinality of the derivation meters.
 *
 * <p>The derive counters are tagged by {@code source_table} and {@code config_id}, which is the only
 * tagging that makes them actionable: a fleet total cannot say which materialization stopped
 * deduplicating. But both values are user-supplied and unbounded. A deployment with a thousand
 * tables produces a thousand series per meter, and a benchmark sweep is worse -- one run of
 * {@code bench/run.py --tables 100} mints a hundred, and a loop over model revisions mints a fresh
 * set for every revision, forever, because a counter is never removed once registered.
 *
 * <p>Unbounded tag cardinality is a metrics-backend incident, not a monitoring feature, and the
 * failure lands on the operator's Prometheus rather than here -- which is exactly why it has to be
 * bounded here. Past the cap Micrometer denies new meters rather than degrading, so the numbers
 * already being collected keep working and the missing ones are absent rather than wrong.
 *
 * <p>The cap is per meter name, so every {@code vectorsync.*} counter gets its own budget.
 */
@Configuration
public class MeterFilterConfig {

    /**
     * Distinct tag combinations allowed per meter name.
     *
     * <p>2,000 is well above any plausible real deployment of this prototype and well below the
     * point where a metrics backend struggles, which is the right side of both errors to be on.
     */
    @Value("${vectorsync.metrics.max-tag-values:2000}")
    private int maxTagValues;

    @Bean
    MeterFilter vectorsyncCardinalityCap() {
        return MeterFilter.maximumAllowableTags(
                "vectorsync", "source_table", maxTagValues, MeterFilter.deny());
    }

    /**
     * The same cap on {@code config_id}, which is the tag more likely to run away: it is a hash, so
     * every change to a chunker setting or a model revision produces a value that has never been
     * seen before and never will be again.
     */
    @Bean
    MeterFilter vectorsyncConfigCardinalityCap() {
        return MeterFilter.maximumAllowableTags(
                "vectorsync", "config_id", maxTagValues, MeterFilter.deny());
    }
}
