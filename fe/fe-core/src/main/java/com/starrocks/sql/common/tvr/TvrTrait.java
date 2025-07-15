package com.starrocks.sql.common.tvr;

public interface TvrTrait {
    /**
     * Check if the TVR is append-only.
     * @return true if the TVR is append-only, false otherwise.
     */
    boolean isAppendOnly();

    TvrDelta getTvrDelta();
}
