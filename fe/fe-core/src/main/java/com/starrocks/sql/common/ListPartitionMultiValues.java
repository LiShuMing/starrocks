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
public class ListPartitionMultiValues {
    // multi values
    private final List<List<String>> partitionKeys;
    private final String partitionName;

    public ListPartitionMultiValues(String partitionName,
                                    List<List<String>> partitionKeys) {
        this.partitionName = partitionName;
        this.partitionKeys = partitionKeys;
    }

    public List<List<String>> getPartitionKey() {
        return partitionKeys;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public List<ListPartitionValue> toListPartitionValues() {
        List<ListPartitionValue> listPartitionValues = Lists.newArrayList();
        for (List<String> partitionKey : partitionKeys) {
            listPartitionValues.add(new ListPartitionValue(partitionName, partitionKey));
        }
        return listPartitionValues;
    }

    public List<ListPartitionValue> toListPartitionValues(List<Integer> selectColIds) {
        List<ListPartitionValue> listPartitionValues = Lists.newArrayList();
        for (List<String> partitionKey : partitionKeys) {
            List<String> selectedPartitionKey = Lists.newArrayList();
            for (Integer i : selectColIds) {
                selectedPartitionKey.add(partitionKey.get(i));
            }
            listPartitionValues.add(new ListPartitionValue(partitionName, selectedPartitionKey));
        }
        return listPartitionValues;
    }

    @Override
    public int hashCode() {
        // only consider partition key
        return Objects.hash(partitionKeys, partitionName);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof ListPartitionMultiValues)) {
            return false;
        }
        return partitionName.equals(((ListPartitionMultiValues) o).partitionName)
                && partitionKeys.equals(((ListPartitionMultiValues) o).partitionKeys);
    }
}
