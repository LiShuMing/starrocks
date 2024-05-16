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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.scheduler.TableWithPartitions;
import com.starrocks.sql.common.PartitionDiffer;
import com.starrocks.sql.common.RangePartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.MvRefreshArbiter.getMvBaseTableUpdateInfo;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public class MVTimelinessRangePartitionArbiter extends MVTimelinessArbiter {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessRangePartitionArbiter.class);

    public MVTimelinessRangePartitionArbiter(MaterializedView mv) {
        super(mv);
    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInChecked(boolean isQueryRewrite) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Preconditions.checkState(partitionInfo instanceof ExpressionRangePartitionInfo);
        // If non-partition-by table has changed, should refresh all mv partitions
        Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
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
        if (!collectBaseTablePartitionInfos(partitionInfos, partitionExpr,  isQueryRewrite, mvRefreshInfo)) {
            logMVPrepare(mv, "Partitioned mv collect base table infos failed");
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }

        Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfos = mvRefreshInfo.getBaseTableUpdateInfos();
        Map<Table, Map<String, Range<PartitionKey>>> basePartitionNameToRangeMap = baseTableUpdateInfos.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPartitionNameWithRanges()));
        // TODO: prune the partitions based on ttl
        Pair<Table, Column> directTableAndPartitionColumn = mv.getDirectTableAndPartitionColumn();
        Table refBaseTable = directTableAndPartitionColumn.first;

        Map<String, Range<PartitionKey>> refTablePartitionMap = basePartitionNameToRangeMap.get(refBaseTable);
        Map<String, Range<PartitionKey>> mvPartitionNameToRangeMap = mv.getRangePartitionMap();
        RangePartitionDiff rangePartitionDiff = PartitionUtil.getPartitionDiff(partitionExpr,
                refTablePartitionMap, mvPartitionNameToRangeMap, null);

        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();

        // TODO: no needs to refresh the deleted partitions, because the deleted partitions are not in the mv's partition map.
        needRefreshMvPartitionNames.addAll(rangePartitionDiff.getDeletes().keySet());
        // remove ref base table's deleted partitions from `mvPartitionMap`
        for (String deleted : rangePartitionDiff.getDeletes().keySet()) {
            mvPartitionNameToRangeMap.remove(deleted);
        }

        // step2: refresh ref base table's new added partitions
        needRefreshMvPartitionNames.addAll(rangePartitionDiff.getAdds().keySet());
        mvPartitionNameToRangeMap.putAll(rangePartitionDiff.getAdds());

        Map<Table, Expr> tableToPartitionExprMap = mv.getTableToPartitionExprMap();
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = SyncPartitionUtils
                .generateBaseRefMap(basePartitionNameToRangeMap, tableToPartitionExprMap, mvPartitionNameToRangeMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = SyncPartitionUtils
                .generateMvRefMap(mvPartitionNameToRangeMap, tableToPartitionExprMap, basePartitionNameToRangeMap);

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

        if (partitionExpr instanceof FunctionCallExpr) {
            List<TableWithPartitions> baseTableWithPartitions = baseChangedPartitionNames.keySet().stream()
                    .map(x -> new TableWithPartitions(x, baseChangedPartitionNames.get(x)))
                    .collect(Collectors.toList());
            if (mv.isCalcPotentialRefreshPartition(baseTableWithPartitions,
                    basePartitionNameToRangeMap, needRefreshMvPartitionNames, mvPartitionNameToRangeMap)) {
                // because the relation of partitions between materialized view and base partition table is n : m,
                // should calculate the candidate partitions recursively.
                SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                        baseToMvNameRef, mvToBaseNameRef, Sets.newHashSet());
            }
        }

        // update mv's to refresh partitions
        mvRefreshInfo.getMvToRefreshPartitionNames().addAll(needRefreshMvPartitionNames);
        return mvRefreshInfo;
    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInLoose(boolean isQueryRewrite) {
        MvUpdateInfo mvUpdateInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL,
                TableProperty.QueryRewriteConsistencyMode.LOOSE);
        Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
        Map<Table, Column> partitionTableAndColumn = mv.getRelatedPartitionTableAndColumn();
        Map<String, Range<PartitionKey>> mvRangePartitionMap = mv.getRangePartitionMap();
        RangePartitionDiff rangePartitionDiff = null;
        try {
            if (!collectBaseTablePartitionInfos(partitionTableAndColumn, partitionExpr, isQueryRewrite, mvUpdateInfo)) {
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
            }
            Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfos = mvUpdateInfo.getBaseTableUpdateInfos();
            Map<Table, Map<String, Range<PartitionKey>>> refBaseTablePartitionMap = baseTableUpdateInfos.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPartitionNameWithRanges()));
            Table partitionTable = mv.getDirectTableAndPartitionColumn().first;
            PartitionDiffer differ = PartitionDiffer.build(mv, Pair.create(null, null));
            rangePartitionDiff = PartitionUtil.getPartitionDiff(partitionExpr,
                    refBaseTablePartitionMap.get(partitionTable), mvRangePartitionMap, differ);
        } catch (Exception e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return null;
        }

        if (rangePartitionDiff == null) {
            LOG.warn("Materialized view compute partition difference with base table failed, the diff of range partition" +
                    " is null.");
            return null;
        }
        Map<String, Range<PartitionKey>> adds = rangePartitionDiff.getAdds();
        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            mvUpdateInfo.getMvToRefreshPartitionNames().add(mvPartitionName);
        }
        return mvUpdateInfo;
    }

    private boolean collectBaseTablePartitionInfos(Map<Table, Column> partitionInfos,
                                                   Expr partitionExpr,
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
                Map<String, Range<PartitionKey>> partitionKeyRanges =
                        PartitionUtil.getPartitionKeyRange(table, entry.getValue(), partitionExpr);
                mvBaseTableUpdateInfo.getPartitionNameWithRanges().putAll(partitionKeyRanges);
            } catch (UserException e) {
                LOG.warn("Materialized view compute partition difference with base table failed.", e);
                return false;
            }
            mvUpdateInfo.getBaseTableUpdateInfos().put(table, mvBaseTableUpdateInfo);
        }
        return true;
    }
}
