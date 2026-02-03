// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe.scheduler.slot;

import com.google.common.annotations.VisibleForTesting;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

/**
 * FastQueryStats tracks statistics for fast queries (queries with short execution time).
 * It maintains a sliding window of recent query executions and calculates percentiles
 * of slot sizes for fast queries, which can be used to dynamically adjust small slot
 * classification thresholds.
 */
public class FastQueryStats {

    // Default configuration
    public static final long DEFAULT_FAST_QUERY_THRESHOLD_MS = 3000;  // 3 seconds
    public static final int DEFAULT_MAX_HISTORY_SIZE = 1000;
    public static final double DEFAULT_FAST_QUERY_PERCENTILE = 0.5;   // P50

    private final long fastQueryThresholdMs;
    private final int maxHistorySize;

    // Histogram of slot sizes for fast queries
    private final Map<Integer, Integer> fastQueryHistogram;
    // Sliding window of recent query records
    private final Queue<QueryExecutionRecord> recentQueries;

    // Cached percentile values
    private volatile int p50UpperBound = 1;
    private volatile int p70UpperBound = 1;
    private volatile int p90UpperBound = 1;
    private volatile int totalFastQueries = 0;

    public FastQueryStats() {
        this(DEFAULT_FAST_QUERY_THRESHOLD_MS, DEFAULT_MAX_HISTORY_SIZE);
    }

    @VisibleForTesting
    public FastQueryStats(long fastQueryThresholdMs, int maxHistorySize) {
        this.fastQueryThresholdMs = fastQueryThresholdMs;
        this.maxHistorySize = maxHistorySize;
        this.fastQueryHistogram = new TreeMap<>();
        this.recentQueries = new LinkedList<>();
    }

    /**
     * Record a completed query for statistics tracking.
     * Only successful fast queries are tracked.
     *
     * @param slot the slot associated with the query
     * @param executionTimeMs the actual execution time in milliseconds
     * @param success whether the query completed successfully
     */
    public synchronized void onQueryComplete(LogicalSlot slot, long executionTimeMs, boolean success) {
        if (!success || executionTimeMs >= fastQueryThresholdMs) {
            return;
        }

        int slotSize = slot.getNumPhysicalSlots();
        long now = System.currentTimeMillis();

        // Add to sliding window
        recentQueries.offer(new QueryExecutionRecord(slotSize, executionTimeMs, now));

        // Maintain window size
        while (recentQueries.size() > maxHistorySize) {
            QueryExecutionRecord removed = recentQueries.poll();
            if (removed != null) {
                decrementHistogram(removed.slotSize);
            }
        }

        // Update histogram
        fastQueryHistogram.merge(slotSize, 1, Integer::sum);
        totalFastQueries++;

        // Update percentile values
        updatePercentileValues();
    }

    /**
     * Check if a slot size is within the fast query threshold.
     *
     * @param numPhysicalSlots the slot size to check
     * @return true if the slot size is considered a fast query slot
     */
    public boolean isFastQuerySlot(int numPhysicalSlots) {
        return numPhysicalSlots <= p50UpperBound;
    }

    /**
     * Get the upper bound for the specified percentile.
     *
     * @param percentile the percentile (0.0 - 1.0)
     * @return the slot size upper bound
     */
    public int getUpperBound(double percentile) {
        if (percentile >= 0.9) {
            return p90UpperBound;
        } else if (percentile >= 0.7) {
            return p70UpperBound;
        } else {
            return p50UpperBound;
        }
    }

    /**
     * Get the current P50 upper bound.
     */
    public int getP50UpperBound() {
        return p50UpperBound;
    }

    /**
     * Get the current P70 upper bound.
     */
    public int getP70UpperBound() {
        return p70UpperBound;
    }

    /**
     * Get the current P90 upper bound.
     */
    public int getP90UpperBound() {
        return p90UpperBound;
    }

    /**
     * Get the total number of fast queries recorded.
     */
    public int getTotalFastQueries() {
        return totalFastQueries;
    }

    /**
     * Get the fast query threshold in milliseconds.
     */
    public long getFastQueryThresholdMs() {
        return fastQueryThresholdMs;
    }

    /**
     * Clear all statistics.
     */
    public synchronized void clear() {
        fastQueryHistogram.clear();
        recentQueries.clear();
        p50UpperBound = 1;
        p70UpperBound = 1;
        p90UpperBound = 1;
        totalFastQueries = 0;
    }

    private void decrementHistogram(int slotSize) {
        fastQueryHistogram.computeIfPresent(slotSize, (k, v) -> {
            int newValue = v - 1;
            return newValue > 0 ? newValue : null;
        });
    }

    private void updatePercentileValues() {
        if (fastQueryHistogram.isEmpty()) {
            p50UpperBound = 1;
            p70UpperBound = 1;
            p90UpperBound = 1;
            return;
        }

        p50UpperBound = calculatePercentile(0.5);
        p70UpperBound = calculatePercentile(0.7);
        p90UpperBound = calculatePercentile(0.9);
    }

    private int calculatePercentile(double percentile) {
        if (fastQueryHistogram.isEmpty()) {
            return 1;
        }

        int total = fastQueryHistogram.values().stream().mapToInt(Integer::intValue).sum();
        if (total == 0) {
            return 1;
        }

        int rank = (int) Math.ceil(percentile * total);
        int cumulative = 0;

        for (Map.Entry<Integer, Integer> entry : fastQueryHistogram.entrySet()) {
            cumulative += entry.getValue();
            if (cumulative >= rank) {
                return entry.getKey();
            }
        }

        // Return the largest key if not found
        return fastQueryHistogram.keySet().stream().reduce((first, second) -> second).orElse(1);
    }

    @Override
    public String toString() {
        return String.format("FastQueryStats{threshold=%dms, total=%d, p50=%d, p70=%d, p90=%d}",
                fastQueryThresholdMs, totalFastQueries, p50UpperBound, p70UpperBound, p90UpperBound);
    }

    /**
     * Internal record for tracking query execution.
     */
    private static class QueryExecutionRecord {
        final int slotSize;
        final long executionTimeMs;
        final long timestamp;

        QueryExecutionRecord(int slotSize, long executionTimeMs, long timestamp) {
            this.slotSize = slotSize;
            this.executionTimeMs = executionTimeMs;
            this.timestamp = timestamp;
        }
    }
}
