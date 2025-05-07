/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autoforcemerge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.junit.BeforeClass;
import org.opensearch.telemetry.TestInMemoryMetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class AutoForceMergeManagerMetricTest extends OpenSearchTestCase {

    private static TestInMemoryMetricsRegistry metricsRegistry;
    private static AutoForceMergeManagerMetric autoForceMergeManagerMetric;

    @BeforeClass
    public static void testSetUp() {
        metricsRegistry = new TestInMemoryMetricsRegistry();
        autoForceMergeManagerMetric = AutoForceMergeManagerMetric.createInstance(metricsRegistry);
    }

    public void testIncrementCounterMethodWithoutTags() {
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.triggered.total").getCounterValue());
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.total").getCounterValue());
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.config").getCounterValue());
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.node").getCounterValue());
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.failed").getCounterValue());
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.totalMergesTriggered, 2.0);
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.totalMergesSkipped, 2.0);
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.skipsFromConfigValidator, 2.0);
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.skipsFromNodeValidator, 2.0);
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.failedForceMerges, 2.0);
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.triggered.total").getCounterValue());
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.total").getCounterValue());
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.config").getCounterValue());
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.node").getCounterValue());
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.failed").getCounterValue());
    }

    public void testIncrementCounterMethodWithTags() {
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.triggered.total").getCounterValue());
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.total").getCounterValue());
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.config").getCounterValue());
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.node").getCounterValue());
        assertEquals(Integer.valueOf(0), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.failed").getCounterValue());
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.totalMergesTriggered, 2.0);
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.totalMergesSkipped, 2.0);
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.skipsFromConfigValidator, 2.0);
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.skipsFromNodeValidator, 2.0);
        autoForceMergeManagerMetric.incrementCounter(autoForceMergeManagerMetric.failedForceMerges, 2.0);
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.triggered.total").getCounterValue());
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.total").getCounterValue());
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.config").getCounterValue());
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.skipped.node").getCounterValue());
        assertEquals(Integer.valueOf(2), metricsRegistry.getCounterStore().get("autoforcemerge.counter.merges.failed").getCounterValue());
    }

    public void testRecordLatencyMethodWithoutTags() {
        assertEquals(Integer.valueOf(0), metricsRegistry.getHistogramStore().get("autoforcemerge.counter.merges.duration").getHistogramValue());
        autoForceMergeManagerMetric.recordLatency(autoForceMergeManagerMetric.jobExecutionTimeHistogram, 2.0);
        assertEquals(Integer.valueOf(2), metricsRegistry.getHistogramStore().get("autoforcemerge.counter.merges.duration").getHistogramValue());
    }

    public void testRecordLatencyMethodWithTags() {
        assertEquals(Integer.valueOf(0), metricsRegistry.getHistogramStore().get("autoforcemerge.counter.merges.duration").getHistogramValue());
        autoForceMergeManagerMetric.recordLatency(autoForceMergeManagerMetric.jobExecutionTimeHistogram, 2.0);
        assertEquals(Integer.valueOf(2), metricsRegistry.getHistogramStore().get("autoforcemerge.counter.merges.duration").getHistogramValue());
    }
}
