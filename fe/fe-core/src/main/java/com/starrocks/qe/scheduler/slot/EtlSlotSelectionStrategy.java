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
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class EtlSlotSelectionStrategy extends SlotSelectionStrategyV2 {
    private static final long ETL_GUARD_UPDATE_INTERVAL_MS = 500;
    private static final long ETL_OVERLOAD_COOLDOWN_MS = 2000;
    private static final int ETL_MIN_DYNAMIC_LIMIT = 1;
    private static final int ETL_MAX_ALLOC_PER_ROUND = 16;
    private static final double ETL_DECREASE_FACTOR = 0.7;
    private static final double ETL_UNDERUTILIZED_MEM_PCT = 0.8;
    private static final int ETL_UNDERUTILIZED_CPU_PERMILLE = 800;

    private final ResourceUsageMonitor resourceUsageMonitor;
    private final EtlResourceGuard etlResourceGuard = new EtlResourceGuard();
    private long lastSmallSlotUpperBoundTimeMs = 0;
    private AcquireSlotsStats cachedAcquireSlotsStats = AcquireSlotsStats.empty();

    public EtlSlotSelectionStrategy(BaseSlotManager slotManager, long warehouseId) {
        super(slotManager, warehouseId);
        this.resourceUsageMonitor = slotManager.getResourceUsageMonitor();
    }

    @Override
    public void onRequireSlot(LogicalSlot slot) {
        super.onRequireSlot(slot);
        invalidateSmallSlotUpperBounds();
    }

    @Override
    public void onAllocateSlot(LogicalSlot slot) {
        updateAcquireHistogramOnRemove(slot);
        super.onAllocateSlot(slot);
        invalidateSmallSlotUpperBounds();
    }

    @Override
    public void onReleaseSlot(LogicalSlot slot) {
        updateAcquireHistogramOnRemove(slot);
        super.onReleaseSlot(slot);
        invalidateSmallSlotUpperBounds();
    }

    @Override
    public List<LogicalSlot> peakSlotsToAllocate(BaseSlotTracker slotTracker) {
        updateOptionsPeriodically();

        refreshRequiringSmallSlots();

        List<LogicalSlot> slotsToAllocate = com.google.common.collect.Lists.newArrayList();
        QueryQueueOptions opts = getCurrentOptions();
        etlResourceGuard.update(slotTracker, opts);
        int roundAllocationLimit = etlResourceGuard.getRoundAllocationLimit(slotTracker, opts);
        if (roundAllocationLimit <= 0) {
            return slotsToAllocate;
        }
        int allocatedThisRound = 0;
        int totalAllocatedSlots = slotTracker.getNumAllocatedSlots();

        // allocate small slots
        int curNumAllocatedSmallSlots = getNumAllocatedSmallSlots();
        for (SlotContext slotContext : getRequiringSmallSlots().values()) {
            LogicalSlot slot = slotContext.getSlot();
            if (allocatedThisRound >= roundAllocationLimit ||
                    !isSmallSlotAvailable(slotTracker, slot, curNumAllocatedSmallSlots,
                            totalAllocatedSlots + allocatedThisRound)) {
                break;
            }

            getRequiringQueue().remove(slotContext);

            slotsToAllocate.add(slot);
            slotContext.setAllocateAsSmallSlot();
            curNumAllocatedSmallSlots += slot.getNumPhysicalSlots();
            allocatedThisRound += slot.getNumPhysicalSlots();
        }

        // allocate normal slots
        int numAllocatedSlots = slotTracker.getNumAllocatedSlots() - getNumAllocatedSmallSlots();
        while (!getRequiringQueue().isEmpty()) {
            SlotContext slotContext = getRequiringQueue().peak();
            if (allocatedThisRound >= roundAllocationLimit ||
                    !isGlobalSlotAvailable(slotTracker, numAllocatedSlots, slotContext.getSlot(),
                            totalAllocatedSlots + allocatedThisRound)) {
                break;
            }

            getRequiringQueue().poll();

            slotsToAllocate.add(slotContext.getSlot());
            numAllocatedSlots += slotContext.getSlot().getNumPhysicalSlots();
            allocatedThisRound += slotContext.getSlot().getNumPhysicalSlots();
        }

        return slotsToAllocate;
    }

    @Override
    protected boolean isSmallSlot(LogicalSlot slot) {
        if (!slot.isETLExec() || !GlobalVariable.isEnableEtlExecDynamicSmallSlotStats()) {
            return super.isSmallSlot(slot);
        }
        double percentile = isEtlResourceUnderUtilized() ? 0.7 : 0.5;
        int upperBound = getSmallSlotUpperBound(percentile);
        return slot.getNumPhysicalSlots() <= upperBound;
    }

    @VisibleForTesting
    protected AcquireSlotsStats getAcquireSlotsStats() {
        updateSmallSlotUpperBounds();
        return cachedAcquireSlotsStats;
    }

    private int getSmallSlotUpperBound(double percentile) {
        updateSmallSlotUpperBounds();
        return percentile >= 0.7 ? cachedAcquireSlotsStats.getP70UpperBound() : cachedAcquireSlotsStats.getP50UpperBound();
    }

    private void updateSmallSlotUpperBounds() {
        long now = System.currentTimeMillis();
        if (now - lastSmallSlotUpperBoundTimeMs < ETL_GUARD_UPDATE_INTERVAL_MS) {
            return;
        }
        lastSmallSlotUpperBoundTimeMs = now;
        // update percentile value
        cachedAcquireSlotsStats.updatePercentileValue();
    }

    private void invalidateSmallSlotUpperBounds() {
        lastSmallSlotUpperBoundTimeMs = 0;
    }

    private void updateAcquireHistogramOnRemove(LogicalSlot slot) {
        cachedAcquireSlotsStats.onSlotRelease(slot);
    }

    @VisibleForTesting
    static class AcquireSlotsStats {
        private int totalSlots;
        private int p50UpperBound;
        private int p70UpperBound;
        private final Map<Integer, Integer> histogram;

        private AcquireSlotsStats(int totalSlots, int p50UpperBound, int p70UpperBound,
                                  Map<Integer, Integer> histogram) {
            this.totalSlots = totalSlots;
            this.p50UpperBound = p50UpperBound;
            this.p70UpperBound = p70UpperBound;
            this.histogram = histogram;
        }

        static AcquireSlotsStats empty() {
            return new AcquireSlotsStats(0, 1, 1, new TreeMap<>());
        }

        public int getTotalSlots() {
            return totalSlots;
        }

        public void updatePercentileValue() {
            if (histogram.isEmpty()) {
                return;
            }
            this.totalSlots = histogram.values().stream().mapToInt(Integer::intValue).sum();
            this.p50UpperBound = percentileValue(0.5);
            this.p70UpperBound = percentileValue(0.7);
        }

        private int percentileValue(double percentile) {
            if (histogram.isEmpty()) {
                return 1;
            }
            int total = histogram.values().stream().mapToInt(Integer::intValue).sum();
            int rank = (int) Math.ceil(percentile * total);
            int cumulative = 0;
            for (Map.Entry<Integer, Integer> entry : histogram.entrySet()) {
                cumulative += entry.getValue();
                if (cumulative >= rank) {
                    return entry.getKey();
                }
            }
            return histogram.keySet().stream().reduce((first, second) -> second).orElse(1);
        }

        public int getP50UpperBound() {
            return p50UpperBound;
        }

        public int getP70UpperBound() {
            return p70UpperBound;
        }

        public Map<Integer, Integer> getHistogram() {
            return histogram;
        }

        private void onSlotRelease(LogicalSlot slot) {
            int size = slot.getNumPhysicalSlots();
            histogram.merge(size, 1, Integer::sum);
        }

        private void resetEmpty() {
            this.totalSlots = 0;
            this.p50UpperBound = 1;
            this.p70UpperBound = 1;
            this.histogram.clear();
        }

        @Override
        public String toString() {
            return "AcquireSlotsStats{" +
                    "totalSlots=" + totalSlots +
                    ", p50UpperBound=" + p50UpperBound +
                    ", p70UpperBound=" + p70UpperBound +
                    ", histogram=" + histogram +
                    '}';
        }
    }

    @Override
    protected boolean isSmallSlotAvailable(BaseSlotTracker slotTracker,
                                           LogicalSlot slot,
                                           int curNumAllocatedSmallSlots,
                                           int totalAllocatedSlots) {
        if (!slot.isETLExec() || !GlobalVariable.isEnableEtlExecDynamicConcurrencyLimit()) {
            return super.isSmallSlotAvailable(slotTracker, slot, curNumAllocatedSmallSlots, totalAllocatedSlots);
        }

        QueryQueueOptions opts = getCurrentOptions();
        if (opts == null) {
            return false;
        }

        if (curNumAllocatedSmallSlots + slot.getNumPhysicalSlots() > opts.v2().getTotalSmallSlots()) {
            if (!etlResourceGuard.allowExtraSmallSlots()) {
                return false;
            }
        }
        if (!etlResourceGuard.canAllocate(slotTracker, slot, totalAllocatedSlots, opts)) {
            return false;
        }
        return isQueryConcurrencyLimitAvailable(slotTracker);
    }

    @Override
    protected boolean isGlobalSlotAvailable(BaseSlotTracker slotTracker, int numAllocatedSlots,
                                            LogicalSlot slot, int totalAllocatedSlots) {
        if (!slot.isETLExec() || !GlobalVariable.isEnableEtlExecDynamicConcurrencyLimit()) {
            return super.isGlobalSlotAvailable(slotTracker, numAllocatedSlots, slot, totalAllocatedSlots);
        }
        QueryQueueOptions opts = getCurrentOptions();
        if (opts == null) {
            return false;
        }
        final int numTotalSlots = opts.v2().getTotalSlots();
        if (numAllocatedSlots != 0 && numAllocatedSlots + slot.getNumPhysicalSlots() > numTotalSlots) {
            return false;
        }
        if (slot.isETLExec() && !etlResourceGuard.canAllocate(slotTracker, slot, totalAllocatedSlots, opts)) {
            return false;
        }
        return isQueryConcurrencyLimitAvailable(slotTracker);
    }

    @VisibleForTesting
    protected boolean isEtlResourceUnderUtilized() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        if (globalStateMgr == null) {
            return false;
        }
        List<ComputeNode> nodes = globalStateMgr.getNodeMgr().getClusterInfo()
                .backendAndComputeNodeStream()
                .filter(ComputeNode::isResourceUsageFresh)
                .collect(Collectors.toList());
        if (nodes.isEmpty()) {
            return false;
        }
        return nodes.stream().allMatch(node -> node.getMemUsedPct() < ETL_UNDERUTILIZED_MEM_PCT &&
                node.getCpuUsedPermille() < ETL_UNDERUTILIZED_CPU_PERMILLE);
    }

    private class EtlResourceGuard {
        private long lastUpdateTimeMs = 0;
        private long lastOverloadedTimeMs = 0;
        private int dynamicLimit = -1;
        private int lastMaxLimit = -1;

        private void update(BaseSlotTracker slotTracker, QueryQueueOptions opts) {
            if (opts == null) {
                return;
            }
            long now = System.currentTimeMillis();
            if (now - lastUpdateTimeMs < ETL_GUARD_UPDATE_INTERVAL_MS) {
                return;
            }
            lastUpdateTimeMs = now;

            int maxLimit = getMaxLimit(opts);
            if (lastMaxLimit != maxLimit) {
                if (dynamicLimit > 0) {
                    dynamicLimit = Math.min(dynamicLimit, maxLimit);
                }
                lastMaxLimit = maxLimit;
            }

            if (dynamicLimit <= 0) {
                dynamicLimit = Math.max(ETL_MIN_DYNAMIC_LIMIT, Math.min(maxLimit, slotTracker.getNumAllocatedSlots()));
            }

            boolean globalOverloaded = resourceUsageMonitor.isGlobalResourceOverloaded();
            if (globalOverloaded) {
                lastOverloadedTimeMs = now;
                dynamicLimit = Math.max(ETL_MIN_DYNAMIC_LIMIT, (int) Math.floor(dynamicLimit * ETL_DECREASE_FACTOR));
                return;
            }

            if (now - lastOverloadedTimeMs < ETL_OVERLOAD_COOLDOWN_MS) {
                return;
            }

            long pending = slotTracker.getQueuePendingLength();
            int allocatedSlots = slotTracker.getNumAllocatedSlots();
            if (pending > 0 && allocatedSlots >= dynamicLimit && dynamicLimit < maxLimit) {
                dynamicLimit += 1;
            } else if (pending == 0 && allocatedSlots + 1 < dynamicLimit) {
                dynamicLimit = Math.max(ETL_MIN_DYNAMIC_LIMIT, dynamicLimit - 1);
            }
        }

        private int getRoundAllocationLimit(BaseSlotTracker slotTracker, QueryQueueOptions opts) {
            if (opts == null) {
                return 0;
            }
            if (resourceUsageMonitor.isGlobalResourceOverloaded()) {
                return 0;
            }
            int maxLimit = getMaxLimit(opts);
            int effectiveDynamicLimit = dynamicLimit > 0 ? Math.min(dynamicLimit, maxLimit) : maxLimit;
            int remaining = Math.max(0, effectiveDynamicLimit - slotTracker.getNumAllocatedSlots());
            if (remaining <= 0) {
                return 0;
            }
            long now = System.currentTimeMillis();
            if (now - lastOverloadedTimeMs < ETL_OVERLOAD_COOLDOWN_MS) {
                return Math.min(1, remaining);
            }
            return Math.min(ETL_MAX_ALLOC_PER_ROUND, remaining);
        }

        private boolean canAllocate(BaseSlotTracker slotTracker, LogicalSlot slot,
                                    int totalAllocatedSlots, QueryQueueOptions opts) {
            if (opts == null) {
                return false;
            }
            if (resourceUsageMonitor.isGlobalResourceOverloaded()) {
                return false;
            }
            if (slot.getGroupId() != LogicalSlot.ABSENT_GROUP_ID &&
                    resourceUsageMonitor.isGroupResourceOverloaded(slot.getGroupId())) {
                return false;
            }
            int maxLimit = getMaxLimit(opts);
            int effectiveDynamicLimit = dynamicLimit > 0 ? Math.min(dynamicLimit, maxLimit) : maxLimit;
            return totalAllocatedSlots + slot.getNumPhysicalSlots() <= effectiveDynamicLimit;
        }

        private boolean allowExtraSmallSlots() {
            return isEtlResourceUnderUtilized();
        }

        private int getMaxLimit(QueryQueueOptions opts) {
            int maxLimit = opts.v2().getTotalSlots();
            int concurrencyLimit = getSlotManager().getQueryQueueConcurrencyLimit(getWarehouseId());
            if (concurrencyLimit > 0) {
                maxLimit = Math.min(maxLimit, concurrencyLimit);
            }
            return Math.max(ETL_MIN_DYNAMIC_LIMIT, maxLimit);
        }
    }
}
