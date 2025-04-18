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
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
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
import org.opensearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * AutoForceMergeManager
 *
 * @opensearch.internal
 */
public class AutoForceMergeManager {

    private final ThreadPool threadPool;
    private final OsService osService;
    private final JvmService jvmService;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TimeValue interval;
    private final AsyncForceMergeTask task;
    private final Integer OPTIMAL_MAX_SEGMENT_COUNT;

    private static final Logger logger = LogManager.getLogger(AutoForceMergeManager.class);

    public AutoForceMergeManager(ThreadPool threadPool, OsService osService, JvmService jvmService,
                                 IndicesService indicesService, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.osService = osService;
        this.indicesService = indicesService;
        this.jvmService = jvmService;
        this.clusterService = clusterService;
        this.interval = TimeValue.timeValueMinutes(1); // 30 minutes interval
        OPTIMAL_MAX_SEGMENT_COUNT = 1;
        task = new AsyncForceMergeTask();
    }

    private void triggerForceMerge() {
        if (!(new ConfigurationValidator().validate().isAllowed())) {
            logger.info("Domain configuration is not meeting the criteria");
            task.close();
            return;
        }
        NodeValidator nodeValidator = new NodeValidator();
        if (!(nodeValidator.validate().isAllowed())) {
            logger.info("Node capacity constraints are not allowing to trigger auto ForceMerge");
            return;
        }

        // Get and sort shards by translog timestamp
        List<IndexShard> sortedShards = getSortedShardsByTranslogAge();

        for (IndexShard shard : sortedShards) {
            // Revalidate node conditions before each shard
            if (!nodeValidator.validate().isAllowed()) {
                logger.info("Node conditions no longer suitable for force merge");
                break;
            }

            if (shard.routingEntry().primary() && new ShardValidator(shard).validate().isAllowed()) {
                try {
                    shard.forceMerge(new ForceMergeRequest().maxNumSegments(OPTIMAL_MAX_SEGMENT_COUNT));
                    logger.info("Successfully triggered force merge for shard {}", shard.shardId());
                } catch (IOException e) {
                    logger.error("Error during force merge for shard " + shard.shardId(), e);
                }
            }
        }
    }

    //Configuration based validator such as tiering enabled domain
    private class ConfigurationValidator implements ValidationStrategy {
        @Override
        public ValidationResult validate() {
            collectOsStats();
            boolean isValid = isRemoteStorageEnabled() && hasWarmNodes();
            if (!isValid) {
                logger.info("Cluster configuration not valid: Remote storage enabled: {}, Has warm nodes: {}",
                    isRemoteStorageEnabled(), hasWarmNodes());
            }
            return new ValidationResult(isValid);
        }
    }


    //Node Capacity validator
    private class NodeValidator implements ValidationStrategy {

        private static final double THRESHOLD_PERCENTAGE = 80.0;

        @Override
        public ValidationResult validate() {
            // CPU Check
            double cpuPercent = osService.stats().getCpu().getPercent();
            if (cpuPercent > THRESHOLD_PERCENTAGE) {
                logger.info("CPU usage too high: {}%", cpuPercent);
                return new ValidationResult(false);
            }

            // Memory Check
            short usedMemoryPercent = osService.stats().getMem().getUsedPercent();
            if (usedMemoryPercent > THRESHOLD_PERCENTAGE) {
                logger.info("Memory usage too high: {}%", usedMemoryPercent);
                return new ValidationResult(false);
            }

            // JVM Check
            double jvmUsedPercent = jvmService.stats().getMem().getHeapUsedPercent();
            if (jvmUsedPercent > THRESHOLD_PERCENTAGE) {
                logger.info("JVM heap usage too high: {}%", jvmUsedPercent);
                return new ValidationResult(false);
            }

            // GC Check
            if (isGCRunning()) {
                logger.info("GC is currently running");
                return new ValidationResult(false);
            }

            // Thread Pool Check
            if (!areForceMergeThreadsAvailable()) {
                logger.info("No force merge threads available");
                return new ValidationResult(false);
            }

            return new ValidationResult(true);
        }

        private boolean isGCRunning() {
            JvmStats.GarbageCollectors gc = jvmService.stats().getGc();
            return Arrays.stream(gc.getCollectors()).anyMatch(collector -> collector.getCollectionCount() > 0);
        }

        private boolean areForceMergeThreadsAvailable() {
            for (ThreadPoolStats.Stats stats : threadPool.stats()) {
                if (stats.getName().equals(ThreadPool.Names.FORCE_MERGE)) {
                    if (stats.getLargest() - stats.getActive() > 0) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    //Shard validator
    private class ShardValidator implements ValidationStrategy {
        private static final long THIRTY_MINUTES_IN_MILLIS = TimeValue.timeValueMinutes(30).millis();
        private final IndexShard shard;
        private final SegmentsStats segmentsStats;
        private final TranslogStats translogStats;

        public ShardValidator(IndexShard shard) {
            this.shard = shard;
            CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Segments, CommonStatsFlags.Flag.Translog);
            CommonStats stats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);
            this.segmentsStats = stats.getSegments();
            this.translogStats = stats.getTranslog();
        }

        @Override
        public ValidationResult validate() {
            collectIndexAndShardStats(shard);
            // Skip if segment count is less than 2
            if (segmentsStats.getCount() < 2) {
                logger.debug("Shard {} has less than 2 segments", shard.shardId());
                return new ValidationResult(false);
            }

            // Skip if translog is too recent
            if (isTranslogTooRecent()) {
                logger.debug("Shard {} translog is too recent", shard.shardId());
                return new ValidationResult(false);
            }

            // Check segment size vs available memory
            if (isSegmentSizeTooLarge()) {
                logger.debug("Shard {} segments too large for available memory", shard.shardId());
                return new ValidationResult(false);
            }

            return new ValidationResult(true);
        }

        private boolean isTranslogTooRecent() {
            return translogStats.getEarliestLastModifiedAge() < THIRTY_MINUTES_IN_MILLIS;
        }

        private boolean isSegmentSizeTooLarge() {
            AtomicLong totalSegmentSize = new AtomicLong();
            long availableMemory = osService.stats().getMem().getFree().getBytes();
            if (segmentsStats != null) {
                ShardSegments shardSegments = new ShardSegments(shard.routingEntry(), shard.segments(true));
                shardSegments.getSegments().forEach(segment -> totalSegmentSize.addAndGet(segment.getSize().getBytes()));
            }
            return totalSegmentSize.get() < availableMemory;
        }
    }

    private void collectOsStats() {
        // CPU Metrics
        double cpuPercent = osService.stats().getCpu().getPercent();
        double loadAverage1m = osService.stats().getCpu().getLoadAverage()[0];
        double loadAverage5m = osService.stats().getCpu().getLoadAverage()[0];
        double loadAverage15m = osService.stats().getCpu().getLoadAverage()[0];
        logger.info("CPU Metrics - CPU Percent: {}%, Load Averages: 1m: {}%, 5m: {}%, 15m: {}%",
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
            jvmMemory.getHeapUsedPercent(), jvmGc, jvmBufferPool);

        //Thread Pool Metrics for force_merge. Can be added :  ultrawarm_migration, ultrawarm_shard_loader
        for (ThreadPoolStats.Stats stats : threadPool.stats()) {
            if (stats.getName().equals(ThreadPool.Names.FORCE_MERGE)) {
                long threads = stats.getThreads();
                long activeThreads = stats.getActive();
                long totalThreads = stats.getLargest();
                long queue = stats.getQueue();
                logger.info("Threadpool Metrics - Threads: {}%, ActiveThreads : {}, Total threads : {},  Queue: {}",
                    threads, activeThreads, totalThreads, queue);
                break; // ONLY NEED FORCE MERGE METRICS
            }
        }
    }

    private void collectIndexAndShardStats(final IndexShard shard) {
        logger.info("----------Node ID : {}, Index Name : {}-----------------------------------", shard.getNodeId(), shard.shardId().getIndexName());

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
            ShardSegments shardSegments = new ShardSegments(shard.routingEntry(), shard.segments(true));
            long committedSegments = shardSegments.getNumberOfCommitted();
            logger.info("Segment Stats for Primary Shard {} - Segments Count: {}, Committed Segments Count: {}",
                shard.shardId().id(), segmentsCount, committedSegments);
            shardSegments.getSegments().forEach(segment -> {
                logger.info("Segment Generation Stats - Generation: {}, Size: {}", segment.getGeneration(), segment.getSize());
            });
        }

        // Add more stats as needed..
    }

    private boolean hasWarmNodes() {
        ClusterState clusterState = clusterService.state();
        return clusterState.getNodes().getNodes()
            .values()
            .stream()
            .anyMatch(DiscoveryNode::isWarmNode
            );
    }

    private boolean isRemoteStorageEnabled() {
        // Check cluster settings for remote store
        Settings clusterSettings = clusterService.getSettings();

        // Check if remote store is enabled globally
        return clusterSettings.getAsBoolean("cluster.remote_store.enabled", false);
    }

    private List<IndexShard> getSortedShardsByTranslogAge() {
        List<IndexShard> shards = new ArrayList<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                if (shard.routingEntry().primary()) {
                    if (new ShardValidator(shard).validate().isAllowed()) {
                        shards.add(shard);
                    }
                }
            }
        }
        return shards.stream()
            .filter(shard -> shard.routingEntry().primary())
            .sorted((s1, s2) -> {
                long age1 = getTranslogAge(s1);
                long age2 = getTranslogAge(s2);
                return Long.compare(age2, age1); // Oldest first
            })
            .collect(Collectors.toList());
    }

    private long getTranslogAge(IndexShard shard) {
        CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Translog);
        CommonStats stats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);
        return stats.getTranslog().getEarliestLastModifiedAge();
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
            triggerForceMerge();
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
