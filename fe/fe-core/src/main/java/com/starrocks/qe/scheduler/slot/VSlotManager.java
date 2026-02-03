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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * VSlotManager manages virtual slot sizing based on cluster resource utilization.
 * It calculates resource factors from compute node metrics and converts between
 * physical slot sizes and virtual slot sizes for dynamic resource allocation.
 */
public class VSlotManager {

    // Default configuration
    public static final double DEFAULT_MIN_FACTOR = 0.5;
    public static final double DEFAULT_MAX_FACTOR = 2.0;
    public static final double DEFAULT_CPU_WEIGHT = 0.4;
    public static final double DEFAULT_MEM_WEIGHT = 0.4;
    public static final double DEFAULT_IO_WEIGHT = 0.2;

    // Load thresholds for factor calculation
    private static final double LOW_LOAD_THRESHOLD = 0.3;
    private static final double NORMAL_LOAD_THRESHOLD = 0.7;

    private final double minFactor;
    private final double maxFactor;
    private final double cpuWeight;
    private final double memWeight;
    private final double ioWeight;

    private volatile ResourceFactor currentFactor;
    private long lastUpdateTimeMs = 0;

    public VSlotManager() {
        this(DEFAULT_MIN_FACTOR, DEFAULT_MAX_FACTOR, DEFAULT_CPU_WEIGHT, DEFAULT_MEM_WEIGHT, DEFAULT_IO_WEIGHT);
    }

    @VisibleForTesting
    public VSlotManager(double minFactor, double maxFactor, double cpuWeight, double memWeight, double ioWeight) {
        this.minFactor = minFactor;
        this.maxFactor = maxFactor;
        this.cpuWeight = cpuWeight;
        this.memWeight = memWeight;
        this.ioWeight = ioWeight;
        this.currentFactor = ResourceFactor.neutral();
    }

    /**
     * Update resource factors based on current compute node metrics.
     * Should be called periodically (e.g., every 500ms).
     */
    public void updateResourceFactors() {
        long now = System.currentTimeMillis();
        // Prevent too frequent updates
        if (now - lastUpdateTimeMs < 500) {
            return;
        }
        lastUpdateTimeMs = now;

        List<ComputeNode> nodes = getActiveComputeNodes();
        if (nodes.isEmpty()) {
            return;
        }

        double cpuFactor = calculateCpuFactor(nodes);
        double memFactor = calculateMemFactor(nodes);
        double ioFactor = calculateIoFactor(nodes);

        currentFactor = new ResourceFactor(cpuFactor, memFactor, ioFactor);
    }

    /**
     * Convert original physical slot size to virtual slot size.
     * When resources are constrained (high factor), virtual size is larger,
     * effectively reducing concurrency. When resources are abundant (low factor),
     * virtual size is smaller, increasing concurrency.
     *
     * @param originalSlotSize the original physical slot size
     * @return the virtual slot size
     */
    public int toVSlotSize(int originalSlotSize) {
        double factor = currentFactor.getCompositeFactor();
        // Higher factor = larger virtual size = fewer slots can fit
        return (int) Math.ceil(originalSlotSize * factor);
    }

    /**
     * Convert virtual slot size back to estimated physical slots.
     *
     * @param vSlotSize the virtual slot size
     * @return estimated physical slot size
     */
    public int fromVSlotSize(int vSlotSize) {
        double factor = currentFactor.getCompositeFactor();
        return (int) Math.floor(vSlotSize / factor);
    }

    /**
     * Calculate how many slots of a given size can fit within a capacity.
     *
     * @param capacity the total capacity in virtual slots
     * @param slotSize the slot size in physical slots
     * @return the number of slots that can fit
     */
    public int calculateFittingSlots(int capacity, int slotSize) {
        int vSlotSize = toVSlotSize(slotSize);
        return capacity / vSlotSize;
    }

    /**
     * Check if adding a slot would exceed capacity.
     *
     * @param currentUsage current usage in physical slots
     * @param capacity total capacity in physical slots
     * @param newSlotSize the new slot size in physical slots
     * @return true if the slot can fit
     */
    public boolean canFit(int currentUsage, int capacity, int newSlotSize) {
        int currentVUsage = toVSlotSize(currentUsage);
        int vCapacity = toVSlotSize(capacity);
        int newVSlotSize = toVSlotSize(newSlotSize);
        return currentVUsage + newVSlotSize <= vCapacity;
    }

    /**
     * Get the current resource factor.
     */
    public ResourceFactor getCurrentFactor() {
        return currentFactor;
    }

    /**
     * Check if the cluster is currently under-utilized.
     */
    public boolean isUnderUtilized() {
        return currentFactor.isLowLoad();
    }

    /**
     * Check if the cluster is currently overloaded.
     */
    public boolean isOverloaded() {
        return currentFactor.isHighLoad();
    }

    private List<ComputeNode> getActiveComputeNodes() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        if (globalStateMgr == null) {
            return java.util.Collections.emptyList();
        }
        return globalStateMgr.getNodeMgr().getClusterInfo()
                .backendAndComputeNodeStream()
                .filter(ComputeNode::isResourceUsageFresh)
                .collect(Collectors.toList());
    }

    private double calculateCpuFactor(List<ComputeNode> nodes) {
        double avgCpuUsed = nodes.stream()
                .mapToDouble(node -> node.getCpuUsedPermille() / 1000.0)
                .average()
                .orElse(0.5);
        return mapLoadToFactor(avgCpuUsed);
    }

    private double calculateMemFactor(List<ComputeNode> nodes) {
        double avgMemUsed = nodes.stream()
                .mapToDouble(ComputeNode::getMemUsedPct)
                .average()
                .orElse(0.5);
        return mapLoadToFactor(avgMemUsed);
    }

    private double calculateIoFactor(List<ComputeNode> nodes) {
        // IO factor estimation based on available metrics
        // For now, use a combination of CPU and memory as proxy
        double avgCpuUsed = nodes.stream()
                .mapToDouble(node -> node.getCpuUsedPermille() / 1000.0)
                .average()
                .orElse(0.5);
        // IO tends to correlate with CPU under load
        return mapLoadToFactor(avgCpuUsed * 0.8);
    }

    /**
     * Map resource load (0.0 - 1.0) to a factor (minFactor - maxFactor).
     * Load < 30% -> factor approaches minFactor (0.5)
     * Load 30-70% -> factor linearly increases
     * Load > 70% -> factor approaches maxFactor (2.0)
     */
    private double mapLoadToFactor(double load) {
        double normalizedLoad = Math.max(0.0, Math.min(1.0, load));
        double factor;

        if (normalizedLoad < LOW_LOAD_THRESHOLD) {
            // Low load: 0-30% maps to 0.5-0.8
            factor = minFactor + (normalizedLoad / LOW_LOAD_THRESHOLD) * 0.3;
        } else if (normalizedLoad < NORMAL_LOAD_THRESHOLD) {
            // Normal load: 30-70% maps to 0.8-1.2
            double progress = (normalizedLoad - LOW_LOAD_THRESHOLD) /
                    (NORMAL_LOAD_THRESHOLD - LOW_LOAD_THRESHOLD);
            factor = 0.8 + progress * 0.4;
        } else {
            // High load: 70-100% maps to 1.2-2.0
            double progress = (normalizedLoad - NORMAL_LOAD_THRESHOLD) /
                    (1.0 - NORMAL_LOAD_THRESHOLD);
            factor = 1.2 + progress * (maxFactor - 1.2);
        }

        return Math.max(minFactor, Math.min(maxFactor, factor));
    }

    @Override
    public String toString() {
        return String.format("VSlotManager{factor=%s, min=%.2f, max=%.2f}",
                currentFactor, minFactor, maxFactor);
    }
}
