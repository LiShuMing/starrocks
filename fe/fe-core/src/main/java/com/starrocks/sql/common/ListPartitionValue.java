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
import com.google.api.client.util.Preconditions;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * `PartitionRange` contains a `PartitionKey` range and the partition's name to represent a table's partition range info.
 */
public class ListPartitionValue {
    // multi values
    private final List<String> partitionKeys;
    private final String partitionName;
    private final List<Integer> partitionColumnIds;
    private final Set<List<String>> selectedPartitionKeys = Sets.newHashSet();

    public ListPartitionValue(String partitionName,
                              List<String> partitionKeys) {
        this(partitionName, partitionKeys, Lists.newArrayList());
        for (List<String> val : partitionKeys) {
            selectedPartitionKeys.add(val);
        }
    }

    public ListPartitionValue(String partitionName,
                              List<String> partitionKeys,
                              List<Integer> partitionColumnIds) {
        this.partitionName = partitionName;
        this.partitionKeys = partitionKeys;
        this.partitionColumnIds = partitionColumnIds;
        for (List<String> val : partitionKeys) {
            List<String> selectKeys = Lists.newArrayList();
            for (int i = 0; i < partitionColumnIds.size(); i++) {
                selectKeys.add(val.get(partitionColumnIds.get(i)));
            }
            selectedPartitionKeys.add(selectKeys);
        }
    }

    public List<List<String>> getPartitionKeys() {
        return partitionKeys;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public List<Integer> getPartitionColumnIds() {
        return partitionColumnIds;
    }

    public Set<List<String>> getSelectedPartitionKeys() {
        return selectedPartitionKeys;
    }

    public boolean isIntersected(ListPartitionValue o) {
        Set<List<String>> otherSelectedPartitionKeys = o.getSelectedPartitionKeys();
        if (!getPartitionColumnIds().isEmpty() && !otherSelectedPartitionKeys.isEmpty()) {
            Preconditions.checkState(getPartitionColumnIds().size() == otherSelectedPartitionKeys.size());
        }
        for (List<String> val : selectedPartitionKeys) {
            if (otherSelectedPartitionKeys.contains(val)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, partitionKeys);
    }
}
