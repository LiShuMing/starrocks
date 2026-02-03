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

import java.util.Objects;

/**
 * ResourceFactor represents the current resource utilization factors of the cluster.
 * It captures CPU, memory, and IO utilization to calculate a composite factor
 * for dynamic slot sizing adjustments.
 */
public class ResourceFactor {

    // Default threshold constants
    public static final double HIGH_LOAD_THRESHOLD = 1.5;
    public static final double LOW_LOAD_THRESHOLD = 0.8;
    public static final double CRITICAL_LOAD_THRESHOLD = 1.8;

    private final double cpuFactor;
    private final double memFactor;
    private final double ioFactor;
    private final double compositeFactor;
    private final long timestamp;

    /**
     * Creates a ResourceFactor with the specified component factors.
     *
     * @param cpuFactor CPU utilization factor (higher means more loaded)
     * @param memFactor Memory utilization factor (higher means more loaded)
     * @param ioFactor IO utilization factor (higher means more loaded)
     */
    public ResourceFactor(double cpuFactor, double memFactor, double ioFactor) {
        this.cpuFactor = Math.max(0.1, cpuFactor);
        this.memFactor = Math.max(0.1, memFactor);
        this.ioFactor = Math.max(0.1, ioFactor);
        // Weighted composite: CPU 40%, Mem 40%, IO 20%
        this.compositeFactor = this.cpuFactor * 0.4 + this.memFactor * 0.4 + this.ioFactor * 0.2;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Creates a neutral ResourceFactor with all factors set to 1.0.
     */
    public static ResourceFactor neutral() {
        return new ResourceFactor(1.0, 1.0, 1.0);
    }

    public double getCpuFactor() {
        return cpuFactor;
    }

    public double getMemFactor() {
        return memFactor;
    }

    public double getIoFactor() {
        return ioFactor;
    }

    public double getCompositeFactor() {
        return compositeFactor;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Check if the cluster is in high load condition.
     * High load means we should reduce concurrency.
     */
    public boolean isHighLoad() {
        return compositeFactor >= HIGH_LOAD_THRESHOLD;
    }

    /**
     * Check if the cluster is in low load condition.
     * Low load means we can increase concurrency.
     */
    public boolean isLowLoad() {
        return compositeFactor <= LOW_LOAD_THRESHOLD;
    }

    /**
     * Check if the cluster is in critical load condition.
     * Critical load means we should aggressively reduce concurrency.
     */
    public boolean isCriticalLoad() {
        return compositeFactor >= CRITICAL_LOAD_THRESHOLD;
    }

    /**
     * Get the severity level of current resource utilization.
     */
    public LoadLevel getLoadLevel() {
        if (compositeFactor >= CRITICAL_LOAD_THRESHOLD) {
            return LoadLevel.CRITICAL;
        } else if (compositeFactor >= HIGH_LOAD_THRESHOLD) {
            return LoadLevel.HIGH;
        } else if (compositeFactor <= LOW_LOAD_THRESHOLD) {
            return LoadLevel.LOW;
        } else {
            return LoadLevel.NORMAL;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceFactor that = (ResourceFactor) o;
        return Double.compare(that.cpuFactor, cpuFactor) == 0 &&
                Double.compare(that.memFactor, memFactor) == 0 &&
                Double.compare(that.ioFactor, ioFactor) == 0 &&
                Double.compare(that.compositeFactor, compositeFactor) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpuFactor, memFactor, ioFactor, compositeFactor);
    }

    @Override
    public String toString() {
        return String.format("ResourceFactor{cpu=%.2f, mem=%.2f, io=%.2f, composite=%.2f, level=%s}",
                cpuFactor, memFactor, ioFactor, compositeFactor, getLoadLevel());
    }

    /**
     * Load level enumeration.
     */
    public enum LoadLevel {
        LOW,      // Under-utilized, can increase concurrency
        NORMAL,   // Normal operation
        HIGH,     // Overloaded, should reduce concurrency
        CRITICAL  // Critically overloaded, aggressively reduce concurrency
    }
}
