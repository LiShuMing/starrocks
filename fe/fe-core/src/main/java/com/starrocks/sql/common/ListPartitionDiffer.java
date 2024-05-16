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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ListPartitionDiffer {
    public static Map<Table, Map<String, Set<String>>> generateBaseRefMap(
            Map<Table, Map<String, List<List<String>>>> baseRangeMap,
            Map<Table, List<Integer>> basePartitionColumnIds,
            Map<String, List<List<String>>> mvRangeMap) {
        Map<Table, Map<String, Set<String>>> result = Maps.newHashMap();
        // for each partition of base, find the corresponding partition of mv
        Set<ListPartitionMultiValues> mvRangeWithMultiValues = mvRangeMap.keySet().stream()
                .map(name -> new ListPartitionMultiValues(name, mvRangeMap.get(name)))
                .collect(Collectors.toSet());
        Map<ListPartitionValue, ListPartitionMultiValues>  mvRanges = Maps.newHashMap();
        mvRangeWithMultiValues.stream()
                .flatMap(v -> v.toListPartitionValues().stream().map(x -> Pair.create(x, v)))
                .forEach(p -> mvRanges.put(p.first, p.second));
        for (Map.Entry<Table, Map<String, List<List<String>>>> entry : baseRangeMap.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, List<List<String>>> refreshedPartitionsMap = entry.getValue();
            List<Integer> partitionColumnIds = basePartitionColumnIds.get(baseTable);
            Set<ListPartitionMultiValues> baseRangeWithMultiValues = refreshedPartitionsMap.keySet()
                    .stream()
                    .map(name -> new ListPartitionMultiValues(name, refreshedPartitionsMap.get(name)))
                    .collect(Collectors.toSet());
            Map<ListPartitionValue, ListPartitionMultiValues> baseRanges = Maps.newHashMap();
            baseRangeWithMultiValues.stream()
                    .flatMap(v -> v.toListPartitionValues(partitionColumnIds).stream().map(x -> Pair.create(x, v)))
                    .forEach(p -> baseRanges.put(p.first, p.second));
            for (ListPartitionValue baseRange : baseRanges.keySet()) {
                if (mvRanges.containsKey(baseRange)) {
                    ListPartitionMultiValues mvBaseRange = mvRanges.get(baseRange);
                    result.computeIfAbsent(baseTable, k -> Maps.newHashMap())
                            .computeIfAbsent(baseRange.getPartitionName(), k -> Sets.newHashSet())
                            .add(mvBaseRange.getPartitionName());
                }
            }
        }
        return result;
    }

    public static  Map<String, Map<Table, Set<String>>> generateMvRefMap(
            Map<String, List<List<String>>> mvRangeMap,
            Map<Table, List<Integer>> basePartitionColumnIds,
            Map<Table, Map<String, List<List<String>>>> baseRangeMap) {
        Map<String, Map<Table, Set<String>>> result = Maps.newHashMap();
        // for each partition of base, find the corresponding partition of mv
        Set<ListPartitionMultiValues> mvRangeWithMultiValues = mvRangeMap.keySet().stream()
                .map(name -> new ListPartitionMultiValues(name, mvRangeMap.get(name)))
                .collect(Collectors.toSet());
        Map<ListPartitionValue, ListPartitionMultiValues>  mvRanges = Maps.newHashMap();
        mvRangeWithMultiValues.stream()
                .flatMap(v -> v.toListPartitionValues().stream().map(x -> Pair.create(x, v)))
                .forEach(p -> mvRanges.put(p.first, p.second));
        for (Map.Entry<Table, Map<String, List<List<String>>>> entry : baseRangeMap.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, List<List<String>>> refreshedPartitionsMap = entry.getValue();
            List<Integer> partitionColumnIds = basePartitionColumnIds.get(baseTable);
            Set<ListPartitionMultiValues> baseRangeWithMultiValues = refreshedPartitionsMap.keySet()
                    .stream()
                    .map(name -> new ListPartitionMultiValues(name, refreshedPartitionsMap.get(name)))
                    .collect(Collectors.toSet());
            Map<ListPartitionValue, ListPartitionMultiValues> baseRanges = Maps.newHashMap();
            baseRangeWithMultiValues.stream()
                    .flatMap(v -> v.toListPartitionValues(partitionColumnIds).stream().map(x -> Pair.create(x, v)))
                    .forEach(p -> baseRanges.put(p.first, p.second));
            for (ListPartitionValue baseRange : baseRanges.keySet()) {
                if (mvRanges.containsKey(baseRange)) {
                    ListPartitionMultiValues mvBaseRange = mvRanges.get(baseRange);
                    result.computeIfAbsent(mvBaseRange.getPartitionName(), k -> Maps.newHashMap())
                            .computeIfAbsent(baseTable, k -> Sets.newHashSet())
                            .add(baseRange.getPartitionName());
                }
            }
        }
        return result;
    }
}
