/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.refresh.RefreshStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;

public class AutoForceMergeManager {

    private final ThreadPool threadPool;
    private final OsService osService;
    private final JvmService jvmService;
    private final IndicesService indicesService;
    private final TimeValue interval;
    private final AsyncForceMergeTask task;
    private final Integer optimalMaxSegmentsCount;

    private static final Logger logger = LogManager.getLogger(AutoForceMergeManager.class);

    public AutoForceMergeManager(ThreadPool threadPool, OsService osService, JvmService jvmService,
                                 IndicesService indicesService) {
        this.threadPool = threadPool;
        this.osService = osService;
        this.indicesService = indicesService;
        this.jvmService = jvmService;
        this.interval = TimeValue.timeValueMinutes(1); // 30 minutes interval
        task = new AsyncForceMergeTask();
        optimalMaxSegmentsCount = 1;
    }

    private void triggerForceMerge() {
        if (!(new ConfigurationValidator().validate().isAllowed())) {
            logger.info("Domain configuration is not meeting the criteria");
            return;
        }
        if (!(new NodeValidator().validate().isAllowed())) {
            logger.info("Node capacity constraints are not allowing to trigger auto ForceMerge");
            return;
        }
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                if (shard.routingEntry().primary()) {
                    if (new ShardValidator(shard).validate().isAllowed()) {
                        ForceMergeRequest localForceMergeRequest = new ForceMergeRequest()
                            .maxNumSegments(optimalMaxSegmentsCount);
                        try {
                            shard.forceMerge(localForceMergeRequest);
                        } catch (IOException exception) {
                            logger.error("Exception while consuming Shard Level ForceMerge action", exception);
                        }
                    }
                }
            }
        }
    }

    //Configuration based validator such as tiering enabled domain
    private class ConfigurationValidator implements ValidationStrategy {
        @Override
        public ValidationResult validate() {
            return new ValidationResult(true);
        }
    }


    //Node Capacity validator
    private class NodeValidator implements ValidationStrategy {
        @Override
        public ValidationResult validate() {
            collectOsStats();
            return new ValidationResult(true);
        }
    }

    //Shard validator
    private class ShardValidator implements ValidationStrategy {
        IndexShard indexShard;

        public ShardValidator(final IndexShard indexShard) {
            this.indexShard = indexShard;
        }

        @Override
        public ValidationResult validate() {
            collectIndexAndShardStats(this.indexShard);
            return new ValidationResult(true);
        }
    }

    private void collectOsStats() {
        // CPU Metrics
        double cpuPercent = osService.stats().getCpu().getPercent();
        double loadAverage1m = osService.stats().getCpu().getLoadAverage()[0];
        double loadAverage5m = osService.stats().getCpu().getLoadAverage()[0];
        double loadAverage15m = osService.stats().getCpu().getLoadAverage()[0];
        logger.info("CPU Metrics - CPU Percent: {}%, Load Averages: 1m: {}, 5m: {}, 15m: {}",
            cpuPercent, loadAverage1m, loadAverage5m, loadAverage15m);

        // Memory Metrics
        long totalMemory = osService.stats().getMem().getTotal().getBytes();
        long freeMemory = osService.stats().getMem().getFree().getBytes();
        long usedMemory = osService.stats().getMem().getUsed().getBytes();
        short freeMemoryPercent = osService.stats().getMem().getFreePercent();
        short usedMemoryPercent = osService.stats().getMem().getUsedPercent();
        logger.info("Memory Metrics - Total Memory: {}%, Free Memory: {}, Used Memory: {}, Free Memory Percent: {}, Used Memory Percent: {}",
            totalMemory, freeMemory, usedMemory, freeMemoryPercent, usedMemoryPercent);

        // JVM Metrics
        JvmStats.Mem jvmMemory = jvmService.stats().getMem();
        JvmStats.GarbageCollectors jvmGc = jvmService.stats().getGc();
        List<JvmStats.BufferPool> jvmBufferPool = jvmService.stats().getBufferPools();
        logger.info("JVM Metrics - Memory: {}%, GC : {}, Buffer pool: {}",
            jvmMemory, jvmGc, jvmBufferPool);

        //Thread Pool Metrics for force_merge. Can be added :  ultrawarm_migration, ultrawarm_shard_loader
        /*
        for (Iterator<ThreadPoolStats.Stats> stats : threadPool.stats().iterator()) {
            if (stats.getName().equals(ThreadPool.Names.FORCE_MERGE)) {
                long threads = stats.getThreads();
                long activeThreads = stats.getActive();
                long queue = stats.getQueue();
                logger.info("Threadpool Metrics - Threads: {}%, ActiveThreads : {}, Queue: {}",
                    threads, activeThreads, queue);
                break;
            }
        }
         */

    }

    private void collectIndexAndShardStats(final IndexShard shard) {
        // Get shard stats
        CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Merge, CommonStatsFlags.Flag.Translog, CommonStatsFlags.Flag.Refresh, CommonStatsFlags.Flag.Segments);
        CommonStats commonStats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);

        // Merge stats
        MergeStats mergeStats = commonStats.getMerge();
        if (mergeStats != null) {
            long currentMerges = mergeStats.getCurrent();
            long currentMergesInBytes = mergeStats.getCurrentSizeInBytes();
            long totalMerges = mergeStats.getTotal();
            long totalMergeTime = mergeStats.getTotalTimeInMillis();
            logger.info("Shard Merge Stats for shard {} - Current Merges: {}, CurrentMergesInBytes: {}, Total Merges: {}, Total Merge Time: {} ms",
                shard.shardId().id(), currentMerges, currentMergesInBytes, totalMerges, totalMergeTime);
        }

        // Refresh stats
        RefreshStats refreshStats = commonStats.getRefresh();
        if (refreshStats != null) {
            long totalRefresh = refreshStats.getTotal();
            long totalExternalRefresh = refreshStats.getExternalTotal();
            logger.info("Shard Refresh Stats for shard {} - Total Refreshes: {}, Total  External refreshes: {}",
                shard.shardId().id(), totalRefresh, totalExternalRefresh);
        }

        // Translog stats
        TranslogStats translogStats = commonStats.getTranslog();
        if (translogStats != null) {
            long translogAge = translogStats.getEarliestLastModifiedAge();
            long translogSize = translogStats.getUncommittedSizeInBytes();
            logger.info("Translog Stats for Primary Shard {} - Earliest Age: {}, Translog Size : {} ",
                shard.shardId().id(), translogAge, translogSize);
        }

        // Segment stats
        SegmentsStats segmentsStats = commonStats.getSegments();
        if (segmentsStats != null) {
            long segmentsCount = segmentsStats.getCount();
            logger.info("Segment Stats for Primary Shard {} - Segments Count: {} ",
                shard.shardId().id(), segmentsCount);
        }

        // Add more stats as needed..
    }

    private final class AsyncForceMergeTask extends AbstractAsyncTask {
        public AsyncForceMergeTask() {
            super(logger, threadPool, interval, true);
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            logger.info("Not running intentionally now");
            //triggerForceMerge();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.FORCE_MERGE;
        }
    }

    private interface ValidationStrategy {
        ValidationResult validate();
    }

    private static final class ValidationResult {
        private final boolean allowed;

        public ValidationResult(boolean allowed) {
            this.allowed = allowed;
        }

        public boolean isAllowed() {
            return allowed;
        }
    }
}
