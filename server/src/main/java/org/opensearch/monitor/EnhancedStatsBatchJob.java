/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexService;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.monitor.os.OsService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/**
 * The main OpenSearch enhanced stats batch job
 *
 * @opensearch.api
 */
public class EnhancedStatsBatchJob extends AbstractLifecycleComponent {
    private final ThreadPool threadPool;
    private final OsService osService;
    private final IndicesService indicesService;
    private final TimeValue interval;
    private Scheduler.Cancellable scheduledFuture;

    private static final Logger logger = LogManager.getLogger(EnhancedStatsBatchJob.class);

    public EnhancedStatsBatchJob(Settings settings, ThreadPool threadPool, OsService osService, IndicesService indicesService) {
        this.threadPool = threadPool;
        this.osService = osService;
        this.indicesService = indicesService;
        this.interval = TimeValue.timeValueMinutes(1); // 30 minutes interval
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(new StatsCollector(), interval, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() {
    }

    private class StatsCollector implements Runnable {
        @Override
        public void run() {
            try {
                // Collect OS stats
                collectOsStats();

                // Collect index and shard stats
                collectIndexAndShardStats();

            } catch (Exception e) {
                logger.error("Error collecting stats", e);
            }
        }

        private void collectOsStats() {
            double cpuPercent = osService.stats().getCpu().getPercent();
            double loadAverage1m = osService.stats().getCpu().getLoadAverage()[0];
            double loadAverage5m = osService.stats().getCpu().getLoadAverage()[0];
            double loadAverage15m = osService.stats().getCpu().getLoadAverage()[0];
            // CPU Metrics
            logger.info("CPU Metrics - CPU Percent: {}%, Load Averages: 1m: {}, 5m: {}, 15m: {}",
                cpuPercent, loadAverage1m, loadAverage5m, loadAverage15m);

            long totalMemory = osService.stats().getMem().getTotal().getBytes();
            long freeMemory = osService.stats().getMem().getFree().getBytes();
            long usedMemory = osService.stats().getMem().getUsed().getBytes();
            short freeMemoryPercent = osService.stats().getMem().getFreePercent();
            short usedMemoryPercent = osService.stats().getMem().getUsedPercent();

            // Memory Metrics
            logger.info("Memory Metrics - Total Memory: {}%, Free Memory: {}, Used Memory: {}, Free Memory Percent: {}, Used Memory Percent: {}",
                totalMemory, freeMemory, usedMemory, freeMemoryPercent, usedMemoryPercent);
        }

        private void collectIndexAndShardStats() {
            for (IndexService indexService : indicesService) {
                String indexName = indexService.index().getName();

                // Collect stats for all primary shards in this index
                for (IndexShard shard : indexService) {
                    if (shard.routingEntry().primary()) {
                        // Get shard stats
                        CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Merge, CommonStatsFlags.Flag.Translog, CommonStatsFlags.Flag.Refresh, CommonStatsFlags.Flag.Segments);
                        CommonStats commonStats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);

                        // Merge stats
                        MergeStats mergeStats = commonStats.getMerge();
                        long totalMerges = mergeStats.getTotal();
                        long totalMergeTime = mergeStats.getTotalTimeInMillis();

                        logger.info("Shard Merge Stats for index {} shard {} - Total Merges: {}, Total Merge Time: {} ms",
                            indexName, shard.shardId().id(), totalMerges, totalMergeTime);

                        // Translog stats
                        TranslogStats translogStats = commonStats.getTranslog();
                        long translogAge = translogStats.getEarliestLastModifiedAge();
                        long translogSize = translogStats.getUncommittedSizeInBytes();

//                        logger.info("Translog Stats for Primary Shard {} in index {} - Operations: {}, Uncommitted Size: {} bytes",
//                            shard.shardId().id(), indexName, translogOperations, translogSize);

                        // Add more stats as needed...
                    }
                }
            }
        }
    }
}
