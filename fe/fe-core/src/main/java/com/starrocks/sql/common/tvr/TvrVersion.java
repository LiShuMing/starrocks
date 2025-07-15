package com.starrocks.sql.common.tvr;


import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class TvrVersion implements Comparable<TvrVersion> {

    @SerializedName("version")
    private final long version;

    public static final long MIN_TIME = Long.MIN_VALUE;
    public static final long MAX_TIME = Long.MAX_VALUE;
    public static final TvrVersion MIN = new TvrVersion(Long.MIN_VALUE);
    public static final TvrVersion MAX = new TvrVersion(Long.MAX_VALUE);

    protected TvrVersion(long version) {
        this.version = version;
    }

    public static TvrVersion of(long version) {
        if (version == MIN_TIME) {
            return MIN;
        } else if (version == MAX_TIME) {
            return MAX;
        }

        return new TvrVersion(version);
    }

    public boolean isMax() {
        return this.equals(MAX);
    }

    public boolean isMin() {
        return this.equals(MIN);
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        if (version == MIN_TIME) {
            return "MIN";
        } else if (version == MAX_TIME) {
            return "MAX";
        } else {
            return String.valueOf(version);
        }
    }

    @Override
    public int compareTo(TvrVersion o) {
        if (this == MAX && o != MAX) {
            return 1;
        } else if (this == MIN && o != MIN) {
            return -1;
        } else if (this != MAX && o == MAX) {
            return -1;
        } else if (this != MIN && o == MIN) {
            return 1;
        }
        return Long.compare(version, o.version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TvrVersion)) {
            return false;
        }
        TvrVersion that = (TvrVersion) o;
        return version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(version);
    }
}