package com.starrocks.catalog.mv;

import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.sql.common.EitherOr;

import java.util.List;
import java.util.Optional;

public class MVPartitionKey {
    private final EitherOr<Range<PartitionKey>, List<List<String>>> mvPartitionValue;

    public MVPartitionKey(Range<PartitionKey> rangePartitionKey) {
        this.mvPartitionValue = new EitherOr<>(Optional.of(rangePartitionKey), Optional.empty());
    }

    public MVPartitionKey(List<List<String>> listPartitionKey) {
        this.mvPartitionValue = new EitherOr<>(Optional.empty(), Optional.of(listPartitionKey));
    }

    public EitherOr<Range<PartitionKey>, List<List<String>>> getMvPartitionValue() {
        return mvPartitionValue;
    }

    public Optional<Range<PartitionKey>> getRangePartitionKey() {
        return mvPartitionValue.left();
    }

    public Range<PartitionKey> getRangePartitionKeyChecked() {
        if (mvPartitionValue.left().isEmpty()) {
            throw new IllegalStateException("Range partition key is not present");
        }
        return mvPartitionValue.left().get();
    }

    public Optional<List<List<String>>> getListPartitionKey() {
        return mvPartitionValue.right();
    }

    public List<List<String>> getListPartitionKeyChecked() {
        if (mvPartitionValue.right().isEmpty()) {
            throw new IllegalStateException("List partition key is not present");
        }
        return mvPartitionValue.right().get();
    }
}
