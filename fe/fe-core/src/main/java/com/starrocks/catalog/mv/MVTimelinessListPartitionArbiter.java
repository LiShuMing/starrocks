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

package com.starrocks.catalog.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.common.ListPartitionDiff;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.SyncPartitionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.MvRefreshArbiter.getMvBaseTableUpdateInfo;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public class MVTimelinessListPartitionArbiter extends MVTimelinessArbiter {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessListPartitionArbiter.class);

    public MVTimelinessListPartitionArbiter(MaterializedView mv) {
        super(mv);
    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInChecked(boolean isQueryRewrite) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Preconditions.checkState(partitionInfo instanceof ListPartitionInfo);
        Map<Table, Column> partitionInfos = mv.getRelatedPartitionTableAndColumn();
        if (partitionInfos.isEmpty()) {
            mv.setInactiveAndReason("partition configuration changed");
            LOG.warn("mark mv:{} inactive for get partition info failed", mv.getName());
            throw new RuntimeException(String.format("getting partition info failed for mv: %s", mv.getName()));
        }

        MvUpdateInfo.MvToRefreshType refreshType = determineRefreshType(partitionInfos, isQueryRewrite);
        logMVPrepare(mv, "Partitioned mv to refresh type:{}", refreshType);
        if (refreshType == MvUpdateInfo.MvToRefreshType.FULL) {
            return new MvUpdateInfo(refreshType);
        }

        MvUpdateInfo mvRefreshInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL);
        if (!collectBaseTablePartitionInfos(partitionInfos, isQueryRewrite, mvRefreshInfo)) {
            logMVPrepare(mv, "Partitioned mv collect base table infos failed");
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }

        Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfos = mvRefreshInfo.getBaseTableUpdateInfos();
        Map<Table, Map<String, List<List<String>>>> refBaseTablePartitionMap = baseTableUpdateInfos.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPartitionNameWithLists()));
        // TODO: prune the partitions based on ttl
        Pair<Table, Column> directTableAndPartitionColumn = mv.getDirectTableAndPartitionColumn();
        Table refBaseTable = directTableAndPartitionColumn.first;

        Map<String, List<List<String>>> refTablePartitionMap = refBaseTablePartitionMap.get(refBaseTable);
        Map<String, List<List<String>>> mvPartitionNameToListMap = mv.getListPartitionMap();
        ListPartitionDiff listPartitionDiff = SyncPartitionUtils.getListPartitionDiff(
                refTablePartitionMap, mvPartitionNameToListMap);

        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();

        // TODO: no needs to refresh the deleted partitions, because the deleted partitions are not in the mv's partition map.
        needRefreshMvPartitionNames.addAll(listPartitionDiff.getDeletes().keySet());
        // remove ref base table's deleted partitions from `mvPartitionMap`
        for (String deleted : listPartitionDiff.getDeletes().keySet()) {
            mvPartitionNameToListMap.remove(deleted);
        }

        // step2: refresh ref base table's new added partitions
        needRefreshMvPartitionNames.addAll(listPartitionDiff.getAdds().keySet());
        mvPartitionNameToListMap.putAll(listPartitionDiff.getAdds());

        Map<Table, List<Integer>> tableToPartitionColIds = Maps.newHashMap();
        for (Map.Entry<Table, Column> e : partitionInfos.entrySet()) {
            Table table = e.getKey();
            List<Column> basePartCols = table.getPartitionColumns();
            int coldIdx = basePartCols.indexOf(e.getValue());
            tableToPartitionColIds.put(table, Lists.newArrayList(coldIdx));
        }
        Map<Table, Map<String, Set<String>>> baseToMvNameRef =
                ListPartitionDiffer.generateBaseRefMap(refBaseTablePartitionMap, tableToPartitionColIds, mvPartitionNameToListMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = ListPartitionDiffer
                .generateMvRefMap(mvPartitionNameToListMap, tableToPartitionColIds, refBaseTablePartitionMap);

        mvRefreshInfo.getBasePartToMvPartNames().putAll(baseToMvNameRef);
        mvRefreshInfo.getMvPartToBasePartNames().putAll(mvToBaseNameRef);

        // Step1: collect updated partitions by partition name to range name:
        // - deleted partitions.
        // - added partitions.
        Map<Table, Set<String>> baseChangedPartitionNames = mvRefreshInfo.getBaseTableUpdateInfos().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getToRefreshPartitionNames()));
        for (Map.Entry<Table, Set<String>> entry : baseChangedPartitionNames.entrySet()) {
            entry.getValue().stream().forEach(x ->
                    needRefreshMvPartitionNames.addAll(baseToMvNameRef.get(entry.getKey()).get(x))
            );
        }

        // update mv's to refresh partitions
        mvRefreshInfo.getMvToRefreshPartitionNames().addAll(needRefreshMvPartitionNames);
        return mvRefreshInfo;
    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInLoose(boolean isQueryRewrite) {
        MvUpdateInfo mvUpdateInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL,
                TableProperty.QueryRewriteConsistencyMode.LOOSE);
        Map<Table, Column> partitionTableAndColumn = mv.getRelatedPartitionTableAndColumn();
        ListPartitionDiff listPartitionDiff = null;
        try {
            if (!collectBaseTablePartitionInfos(partitionTableAndColumn, isQueryRewrite, mvUpdateInfo)) {
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
            }
            Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfos = mvUpdateInfo.getBaseTableUpdateInfos();
            Map<Table, Map<String, List<List<String>>>> refBaseTablePartitionMap = baseTableUpdateInfos.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPartitionNameWithLists()));
            Map<String, List<List<String>>> listPartitionMap = mv.getListPartitionMap();
            Table partitionTable = mv.getDirectTableAndPartitionColumn().first;
            listPartitionDiff = SyncPartitionUtils.getListPartitionDiff(refBaseTablePartitionMap.get(partitionTable),
                    listPartitionMap);
        } catch (Exception e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return null;
        }
        if (listPartitionDiff == null) {
            LOG.warn("Materialized view compute partition difference with base table failed, the diff of range partition" +
                    " is null.");
            return null;
        }
        Map<String, List<List<String>>> adds = listPartitionDiff.getAdds();
        for (Map.Entry<String, List<List<String>>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            mvUpdateInfo.getMvToRefreshPartitionNames().add(mvPartitionName);
        }
        return mvUpdateInfo;
    }

    private boolean collectBaseTablePartitionInfos(Map<Table, Column> partitionInfos,
                                                   boolean isQueryRewrite,
                                                   MvUpdateInfo mvUpdateInfo) {
        for (Map.Entry<Table, Column> entry : partitionInfos.entrySet()) {
            Table table = entry.getKey();

            // TODO: merge getPartitionKeyRange into mvBaseTableUpdateInfo
            // step1.2: check ref base table's updated partition names by checking its ref tables recursively.
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo  = getMvBaseTableUpdateInfo(mv, table, true, isQueryRewrite);
            if (mvBaseTableUpdateInfo == null) {
                return false;
            }

            // TODO: no need to list all partitions, just need to list the changed partitions?
            try {
                Map<String, List<List<String>>> partitionKeyLists = PartitionUtil.getPartitionList(table, entry.getValue());
                for (Map.Entry<String, List<List<String>>> e : partitionKeyLists.entrySet()) {
                    mvBaseTableUpdateInfo.addListPartitionKeys(e.getKey(), e.getValue());
                }
            } catch (UserException e) {
                LOG.warn("Materialized view compute partition difference with base table failed.", e);
                return false;
            }
            mvUpdateInfo.getBaseTableUpdateInfos().put(table, mvBaseTableUpdateInfo);
        }
        return true;
    }
}
