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

/**
 * DynamicSlotEvaluator provides dynamic evaluation of slot properties based on runtime metrics.
 * It integrates fast query statistics and resource-aware virtual slot sizing to optimize
 * slot allocation decisions.
 */
public interface DynamicSlotEvaluator {

    /**
     * Evaluate whether a slot should be considered as a "small slot" based on historical
     * fast query statistics and current resource conditions.
     *
     * @param slot the logical slot to evaluate
     * @return true if the slot should be treated as a small slot
     */
    boolean evaluateSmallSlot(LogicalSlot slot);

    /**
     * Calculate the virtual slot size based on current resource factors.
     * The virtual size may differ from the original physical slot size to adapt
     * to cluster resource conditions.
     *
     * @param originalSlotSize the original physical slot size
     * @return the virtual slot size adjusted by resource factors
     */
    int calculateVSlotSize(int originalSlotSize);

    /**
     * Convert virtual slot size back to estimated physical slots.
     *
     * @param vSlotSize the virtual slot size
     * @return estimated physical slot size
     */
    int fromVSlotSize(int vSlotSize);

    /**
     * Get the current resource factor snapshot.
     *
     * @return the current resource factor
     */
    ResourceFactor getCurrentResourceFactor();

    /**
     * Record a completed query for fast query statistics tracking.
     *
     * @param slot the slot associated with the completed query
     * @param executionTimeMs the actual execution time in milliseconds
     * @param success whether the query completed successfully
     */
    void onQueryComplete(LogicalSlot slot, long executionTimeMs, boolean success);

    /**
     * Update resource factors based on current compute node metrics.
     * Should be called periodically to keep resource factors up-to-date.
     */
    void updateResourceFactors();

    /**
     * Check if the cluster is currently under-utilized.
     *
     * @return true if cluster resources are under-utilized
     */
    boolean isResourceUnderUtilized();

    /**
     * Get the current upper bound for small slot classification.
     *
     * @param percentile the percentile to use (e.g., 0.5 for P50, 0.7 for P70)
     * @return the slot size upper bound
     */
    int getSmallSlotUpperBound(double percentile);
}
