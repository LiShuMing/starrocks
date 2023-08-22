package com.starrocks.catalog;

/**
 * Materialized view's stats to track with.
 */
public class MaterializedViewStats {
    private long refreshJobCount;
    private long accelerationCount;
    private long consideredCount;
    private long matchedCount;
    private long lastRefreshDuration;
    private long rowNums;
    private long storageSizeMB;
}
