/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autoforcemerge;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Objects;
import java.util.Optional;

/**
 * Class containing metrics (counters/latency) specific to AutoForceMergeManager.
 *
 * @opensearch.internal
 */
public class AutoForceMergeManagerMetric {

    private static final String LATENCY_METRIC_UNIT_MS = "ms";
    private static final String COUNTER_METRICS_UNIT = "1";

    private static AutoForceMergeManagerMetric instance;
    public Counter totalMergesTriggered;
    public Counter totalMergesSkipped;
    public Counter skipsFromNodeValidator;
    public Counter skipsFromConfigValidator;
    public Counter failedForceMerges;
    public Histogram jobExecutionTimeHistogram;

    private AutoForceMergeManagerMetric(MetricsRegistry metricsRegistry) {
        totalMergesTriggered = metricsRegistry.createCounter("autoforcemerge.counter.merges.triggered.total",
            "Counter for number of times force merges were triggered.",
            COUNTER_METRICS_UNIT);
        totalMergesSkipped = metricsRegistry.createCounter("autoforcemerge.counter.merges.skipped.total",
            "Counter for number of times force merges were skipped.",
            COUNTER_METRICS_UNIT);
        skipsFromConfigValidator = metricsRegistry.createCounter("autoforcemerge.counter.merges.skipped.config",
            "Counter for number of times force merges were skipped due to Configuration validation.",
            COUNTER_METRICS_UNIT);
        skipsFromNodeValidator = metricsRegistry.createCounter("autoforcemerge.counter.merges.skipped.node",
            "Counter for number of times force merges were skipped due to Node validation.",
            COUNTER_METRICS_UNIT);
        failedForceMerges = metricsRegistry.createCounter("autoforcemerge.counter.merges.failed",
            "Counter for number of times force merges failed.",
            COUNTER_METRICS_UNIT);
        jobExecutionTimeHistogram = metricsRegistry.createHistogram("autoforcemerge.counter.merges.duration",
            "Histogram for tracking the latency of auto force merge execution in the last iteration.",
            LATENCY_METRIC_UNIT_MS);
    }

    public static AutoForceMergeManagerMetric createInstance(MetricsRegistry metricsRegistry) {
        if (instance == null) {
            instance = new AutoForceMergeManagerMetric(metricsRegistry);
        }
        return instance;
    }

    public static AutoForceMergeManagerMetric getInstance() {
        if (instance == null) {
            throw new RuntimeException("AutoForceMergeManagerMetric was not initiated properly.");
        }
        return instance;
    }

    public void recordLatency(Histogram histogram, Double value) {
        recordLatency(histogram, value, Optional.empty());
    }

    public void recordLatency(Histogram histogram, Double value, Optional<Tags> tags) {
        if (Objects.isNull(tags) || tags.isEmpty()) {
            histogram.record(value);
            return;
        }
        histogram.record(value, tags.get());
    }

    public void incrementCounter(Counter counter, Double value) {
        incrementCounter(counter, value, Optional.empty());
    }

    public void incrementCounter(Counter counter, Double value, Optional<Tags> tags) {
        if (Objects.isNull(tags) || tags.isEmpty()) {
            counter.add(value);
            return;
        }
        counter.add(value, tags.get());
    }
}
