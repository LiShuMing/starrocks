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

package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.mv.MVPartitionKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Store the update information of base table for MV
 */
public class MvBaseTableUpdateInfo {
    // The partition names of base table that have been updated
    private final Set<String> toRefreshPartitionNames = Sets.newHashSet();
    // The mapping of partition name to partition range
    private final Map<String, MVPartitionKey> nameToPartKeys = Maps.newHashMap();

    public MvBaseTableUpdateInfo() {
    }

    public Set<String> getToRefreshPartitionNames() {
        return toRefreshPartitionNames;
    }
    public void addToRefreshPartitionNames(Set<String> toRefreshPartitionNames) {
        toRefreshPartitionNames.addAll(toRefreshPartitionNames);
    }

    public void addRangePartitionKeys(String partitionName,
                                      Range<PartitionKey> rangePartitionKey) {
        MVPartitionKey mvPartitionKey = new MVPartitionKey(rangePartitionKey);
        nameToPartKeys.put(partitionName, mvPartitionKey);
    }

    public void addListPartitionKeys(String partitionName,
                                     List<List<String>> listPartitionKey) {
        MVPartitionKey mvPartitionKey = new MVPartitionKey(listPartitionKey);
        nameToPartKeys.put(partitionName, mvPartitionKey);
    }

    public Map<String, Range<PartitionKey>> getPartitionNameWithRanges() {
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        for (Map.Entry<String, MVPartitionKey> e : nameToPartKeys.entrySet()) {
            result.put(e.getKey(), e.getValue().getRangePartitionKeyChecked());
        }
        return result;
    }

    public Map<String, List<List<String>>> getPartitionNameWithLists() {
        Map<String, List<List<String>>> result = Maps.newHashMap();
        for (Map.Entry<String, MVPartitionKey> e : nameToPartKeys.entrySet()) {
            result.put(e.getKey(), e.getValue().getListPartitionKeyChecked());
        }
        return result;
    }

    @Override
    public String toString() {
        return "BaseTableRefreshInfo{" +
                ", toRefreshPartitionNames=" + toRefreshPartitionNames +
                ", partitionNameWithRanges=" + nameToPartKeys +
                '}';
    }
}
