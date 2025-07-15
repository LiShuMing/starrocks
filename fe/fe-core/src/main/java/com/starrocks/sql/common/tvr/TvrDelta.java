package com.starrocks.sql.common.tvr;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public abstract class TvrDelta {
    public static final TvrSnapshot SNAPSHOT_MAX = TvrSnapshot.of(TvrVersion.MAX);

    @SerializedName("start")
    public final TvrVersion start;

    @SerializedName("end")
    public final TvrVersion end;

    protected TvrDelta(TvrVersion start, TvrVersion end) {
        this.start = start;
        this.end = end;
    }

    public boolean timeRangeEquals(TvrDelta other) {
        return Objects.equals(start, other.start) &&
                Objects.equals(end, other.end);
    }

    public abstract TvrDelta copy(TvrVersion from, TvrVersion to);

    public boolean isOverlapped(TvrDelta other) {
        return start.compareTo(other.end) <= 0 && end.compareTo(other.start) >= 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TvrDelta)) {
            return false;
        }
        TvrDelta other = (TvrDelta) obj;
        return timeRangeEquals(other);
    }

    @Override
    public String toString() {
        return "Delta@(" + start + ", " + end + ")";
    }
}
