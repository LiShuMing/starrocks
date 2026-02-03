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

import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Default implementation of DynamicSlotEvaluator that combines fast query statistics
 * and virtual slot management for dynamic slot evaluation.
 */
public class DefaultDynamicSlotEvaluator implements DynamicSlotEvaluator {

    // Threshold for determining under-utilization
    private static final double UNDERUTILIZED_MEM_PCT = 0.8;
    private static final int UNDERUTILIZED_CPU_PERMILLE = 800;

    private final FastQueryStats fastQueryStats;
    private final VSlotManager vSlotManager;
    private final long warehouseId;

    public DefaultDynamicSlotEvaluator(long warehouseId) {
        this.warehouseId = warehouseId;
        this.fastQueryStats = new FastQueryStats();
        this.vSlotManager = new VSlotManager();
    }

    /**
     * Constructor for testing with custom components.
     */
    DefaultDynamicSlotEvaluator(long warehouseId, FastQueryStats fastQueryStats, VSlotManager vSlotManager) {
        this.warehouseId = warehouseId;
        this.fastQueryStats = fastQueryStats;
        this.vSlotManager = vSlotManager;
    }

    @Override
    public boolean evaluateSmallSlot(LogicalSlot slot) {
        if (!slot.isETLExec() || !GlobalVariable.isEnableEtlExecDynamicSmallSlotStats()) {
            // For non-ETL or when dynamic stats disabled, use basic logic
            return slot.getNumPhysicalSlots() <= 1;
        }

        int slotSize = slot.getNumPhysicalSlots();

        // First check: is it in the fast query histogram?
        if (fastQueryStats.isFastQuerySlot(slotSize)) {
            return true;
        }

        // Second check: use percentile-based threshold
        double percentile = isResourceUnderUtilized() ? 0.7 : 0.5;
        int upperBound = getSmallSlotUpperBound(percentile);
        return slotSize <= upperBound;
    }

    @Override
    public int calculateVSlotSize(int originalSlotSize) {
        return vSlotManager.toVSlotSize(originalSlotSize);
    }

    @Override
    public int fromVSlotSize(int vSlotSize) {
        return vSlotManager.fromVSlotSize(vSlotSize);
    }

    @Override
    public ResourceFactor getCurrentResourceFactor() {
        return vSlotManager.getCurrentFactor();
    }

    @Override
    public void onQueryComplete(LogicalSlot slot, long executionTimeMs, boolean success) {
        fastQueryStats.onQueryComplete(slot, executionTimeMs, success);
    }

    @Override
    public void updateResourceFactors() {
        vSlotManager.updateResourceFactors();
    }

    @Override
    public boolean isResourceUnderUtilized() {
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

        return nodes.stream().allMatch(node ->
                node.getMemUsedPct() < UNDERUTILIZED_MEM_PCT &&
                        node.getCpuUsedPermille() < UNDERUTILIZED_CPU_PERMILLE);
    }

    @Override
    public int getSmallSlotUpperBound(double percentile) {
        return fastQueryStats.getUpperBound(percentile);
    }

    /**
     * Get the fast query stats for monitoring/debugging.
     */
    public FastQueryStats getFastQueryStats() {
        return fastQueryStats;
    }

    /**
     * Get the VSlot manager for monitoring/debugging.
     */
    public VSlotManager getVSlotManager() {
        return vSlotManager;
    }

    /**
     * Check if a slot can fit within capacity using virtual slot sizing.
     *
     * @param currentUsage current usage in physical slots
     * @param capacity total capacity in physical slots
     * @param newSlotSize the new slot size in physical slots
     * @return true if the slot can fit
     */
    public boolean canFit(int currentUsage, int capacity, int newSlotSize) {
        return vSlotManager.canFit(currentUsage, capacity, newSlotSize);
    }

    /**
     * Calculate how many slots can fit within a capacity.
     *
     * @param capacity the total capacity in virtual slots
     * @param slotSize the slot size in physical slots
     * @return the number of slots that can fit
     */
    public int calculateFittingSlots(int capacity, int slotSize) {
        return vSlotManager.calculateFittingSlots(capacity, slotSize);
    }

    @Override
    public String toString() {
        return String.format("DefaultDynamicSlotEvaluator{warehouseId=%d, fastQueryStats=%s, vSlotManager=%s}",
                warehouseId, fastQueryStats, vSlotManager);
    }
}
