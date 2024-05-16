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

package com.starrocks.sql.common;

import com.google.api.client.util.Lists;

import java.util.List;
import java.util.Objects;

/**
 * `PartitionRange` contains a `PartitionKey` range and the partition's name to represent a table's partition range info.
 */
public class ListPartitionValue {
    // multi values
    private final List<String> partitionKey;
    private final String partitionName;
    private final List<Integer> partitionColumnIds;
    private final List<String> selectedPartitionKeys;

    public ListPartitionValue(String partitionName,
                              List<String> partitionKeys,
                              List<Integer> partitionColumnIds) {
        this.partitionName = partitionName;
        this.partitionKey = partitionKeys;
        this.partitionColumnIds = partitionColumnIds;
        List<String> selectKeys = Lists.newArrayList();
        for (int i = 0; i < partitionColumnIds.size(); i++) {
            selectKeys.add(partitionKeys.get(partitionColumnIds.get(i)));
        }
        this.selectedPartitionKeys = selectKeys;
    }

    public ListPartitionValue(String partitionName,
                              List<String> partitionKey) {
        this.partitionName = partitionName;
        this.partitionKey = partitionKey;
        this.partitionColumnIds = Lists.newArrayList();
        this.selectedPartitionKeys = Lists.newArrayList(partitionKey);
    }

    public List<String> getPartitionKey() {
        return partitionKey;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public List<Integer> getPartitionColumnIds() {
        return partitionColumnIds;
    }

    public List<String> getSelectedPartitionKeys() {
        return selectedPartitionKeys;
    }

    public boolean isIntersected(ListPartitionValue o) {

    }

    @Override
    public int hashCode() {
        // only consider partition key
        return Objects.hash(partitionKey);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof ListPartitionValue)) {
            return false;
        }
        ListPartitionValue other = (ListPartitionValue) o;
        List<String> otherSelectedPartitionKeys = other.getSelectedPartitionKeys();
        if (otherSelectedPartitionKeys.size() != selectedPartitionKeys.size()) {
            return false;
        }
        int len = selectedPartitionKeys.size();
        for (int i = 0; i < len; i++) {
            if (!selectedPartitionKeys.get(i).equals(otherSelectedPartitionKeys.get(i))) {
                return false;
            }
        }
        return true;
    }
}
