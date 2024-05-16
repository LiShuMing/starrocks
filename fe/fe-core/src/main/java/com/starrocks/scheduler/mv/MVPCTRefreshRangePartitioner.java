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


package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.scheduler.TableWithPartitions;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.common.MvPartitionDiffResult;
import com.starrocks.sql.common.PartitionDiffer;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.SyncPartitionUtils.createRange;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getStr2DateExpr;

public class MVPCTRefreshRangePartitioner extends MVPCTRefreshPartitioner {
    private static final int CREATE_PARTITION_BATCH_SIZE = 64;

    private static final Logger LOG = LogManager.getLogger(MVPCTRefreshRangePartitioner.class);

    public MVPCTRefreshRangePartitioner(MvTaskRunContext mvContext,
                                        TaskRunContext context,
                                        Database db,
                                        MaterializedView mv) {
        super(mvContext, context, db, mv);
    }

    @Override
    public void syncAddOrDropPartitions() {
        Pair<String, String> partitionRange = Pair.create(
                context == null ? null : context.getProperties().get(TaskRun.PARTITION_START),
                context == null ? null : context.getProperties().get(TaskRun.PARTITION_END));
        MvPartitionDiffResult result = PartitionDiffer.computePartitionRangeDiff(db, mv, partitionRange);
        if (result == null) {
            return;
        }
        Map<String, Range<PartitionKey>> deletes = result.rangePartitionDiff.getDeletes();

        // Delete old partitions and then add new partitions because the old and new partitions may overlap
        for (String mvPartitionName : deletes.keySet()) {
            dropPartition(db, mv, mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] delete partitions range [{}]",
                mv.getName(), deletes);

        // Create new added materialized views' ranges
        Map<String, String> partitionProperties = getPartitionProperties(mv);
        DistributionDesc distributionDesc = getDistributionDesc(mv);
        Map<String, Range<PartitionKey>> adds = result.rangePartitionDiff.getAdds();
        addRangePartitions(db, mv, adds, partitionProperties, distributionDesc);
        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            result.mvRangePartitionMap.put(mvPartitionName, addEntry.getValue());
        }
        LOG.info("The process of synchronizing materialized view [{}] add partitions range [{}]",
                mv.getName(), adds);

        // used to get partitions to refresh
        Map<Table, Expr> tableToExprMap = mv.getTableToPartitionExprMap();
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = SyncPartitionUtils
                .generateBaseRefMap(result.refBaseTablePartitionMap, tableToExprMap, result.mvRangePartitionMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = SyncPartitionUtils
                .generateMvRefMap(result.mvRangePartitionMap, tableToExprMap, result.refBaseTablePartitionMap);
        mvContext.setMvRangePartitionMap(result.mvRangePartitionMap);
        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableRangePartitionMap(result.refBaseTablePartitionMap);
        mvContext.setExternalRefBaseTableMVPartitionMap(result.refBaseTableMVPartitionMap);
    }

    @Override
    public Expr generatePartitionPredicate(Table table, Set<String> refBaseTablePartitionNames,
                                           Expr mvPartitionSlotRef) throws AnalysisException {
        List<Range<PartitionKey>> sourceTablePartitionRange = Lists.newArrayList();
        for (String partitionName : refBaseTablePartitionNames) {
            sourceTablePartitionRange.add(mvContext.getRefBaseTableRangePartitionMap()
                    .get(table).get(partitionName));
        }
        sourceTablePartitionRange = MvUtils.mergeRanges(sourceTablePartitionRange);
        // for nested mv, the base table may be another mv, which is partition by str2date(dt, '%Y%m%d')
        // here we should convert date into '%Y%m%d' format
        Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
        Pair<Table, Column> partitionTableAndColumn = mv.getDirectTableAndPartitionColumn();
        boolean isConvertToDate = PartitionUtil.isConvertToDate(partitionExpr, partitionTableAndColumn.second);
        if (isConvertToDate && partitionExpr instanceof FunctionCallExpr
                && !sourceTablePartitionRange.isEmpty() && MvUtils.isDateRange(sourceTablePartitionRange.get(0))) {
            Optional<FunctionCallExpr> functionCallExprOpt = getStr2DateExpr(partitionExpr);
            if (!functionCallExprOpt.isPresent()) {
                LOG.warn("invalid partition expr:{}", partitionExpr);
                return null;
            }
            FunctionCallExpr functionCallExpr = functionCallExprOpt.get();
            Preconditions.checkState(
                    functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE));
            String dateFormat = ((StringLiteral) functionCallExpr.getChild(1)).getStringValue();
            List<Range<PartitionKey>> converted = Lists.newArrayList();
            for (Range<PartitionKey> range : sourceTablePartitionRange) {
                Range<PartitionKey> varcharPartitionKey = MvUtils.convertToVarcharRange(range, dateFormat);
                converted.add(varcharPartitionKey);
            }
            sourceTablePartitionRange = converted;
        }
        List<Expr> partitionPredicates =
                MvUtils.convertRange(mvPartitionSlotRef, sourceTablePartitionRange);
        // range contains the min value could be null value
        Optional<Range<PartitionKey>> nullRange = sourceTablePartitionRange.stream().
                filter(range -> range.lowerEndpoint().isMinValue()).findAny();
        if (nullRange.isPresent()) {
            Expr isNullPredicate = new IsNullPredicate(mvPartitionSlotRef, false);
            partitionPredicates.add(isNullPredicate);
        }

        return Expr.compoundOr(partitionPredicates);
    }

    @Override
    public Set<String> getMVPartitionsToRefresh(PartitionInfo mvPartitionInfo,
                                                Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                String start, String end, boolean force,
                                                Set<String> mvPotentialPartitionNames) throws AnalysisException {
        // range partitioned materialized views
        Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
        Table refBaseTable = mvContext.getRefBaseTable();

        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();
        boolean isAutoRefresh = (mvContext.getTaskType() == Constants.TaskType.PERIODICAL ||
                mvContext.getTaskType() == Constants.TaskType.EVENT_TRIGGERED);
        int partitionTTLNumber = mvContext.getPartitionTTLNumber();
        Set<String> mvRangePartitionNames = getMVPartitionNamesWithTTL(mv, start, end, partitionTTLNumber, isAutoRefresh);

        // check non-ref base tables
        if (needsRefreshBasedOnNonRefTables(snapshotBaseTables, refBaseTable)) {
            if (start == null && end == null) {
                // if non partition table changed, should refresh all partitions of materialized view
                return mvRangePartitionNames;
            } else {
                if (isAutoRefresh) {
                    // If this refresh comes from PERIODIC/EVENT_TRIGGER, it should not respect the config to do the
                    // refresh rather than only checking the ref table's update.
                    // eg:
                    //  MV: SELECT t2.k1 as k1, t2.k2 as k2, t1.k1 as k3, t1.k2 as k4 FROM t1 join t2 on t1.k1=t2.k1;
                    //  RefTable    : t1, Partition: p0,p1,p2
                    //  NonRefTable : t2, Partition: p0,p1,p2
                    // when t2 is updated, it will trigger refresh of ref-table's all partitions.
                    // if `partition_refresh_number` is 1, then:
                    // - The 1th TaskRun: start=null,end=null,  ref-table refresh partition: p0
                    // - The 2th TaskRun: start=p1,end=p2,      ref-table refresh partition: p1
                    // - The 3th TaskRun: start=p2,end=p2,      ref-table refresh partition: p2
                    // if use method below, it will break in the 2th TaskRun because ref-table has not updated in the
                    // specific start and end ranges.
                    return mvRangePartitionNames;
                } else {
                    // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                    // it should be refreshed according to the user-specified range, not all partitions.
                    return getMvPartitionNamesToRefresh(refBaseTable,
                            mvRangePartitionNames, true);
                }
            }
        }

        Map<Table, Column> partitionTablesAndColumn = mv.getRelatedPartitionTableAndColumn();
        if (partitionExpr instanceof SlotRef) {
            // check related partition table
            for (Table table : partitionTablesAndColumn.keySet()) {
                needRefreshMvPartitionNames.addAll(getMvPartitionNamesToRefresh(
                        table, mvRangePartitionNames, force));
            }
        } else if (partitionExpr instanceof FunctionCallExpr) {
            // check related partition table
            for (Table table : partitionTablesAndColumn.keySet()) {
                needRefreshMvPartitionNames.addAll(getMvPartitionNamesToRefresh(
                        table, mvRangePartitionNames, force));
            }

            Map<Table, Set<String>> baseChangedPartitionNames =
                    getBasePartitionNamesByMVPartitionNames(needRefreshMvPartitionNames);
            if (baseChangedPartitionNames.isEmpty()) {
                return needRefreshMvPartitionNames;
            }

            List<TableWithPartitions> baseTableWithPartitions = baseChangedPartitionNames.keySet().stream()
                    .map(x -> new TableWithPartitions(x, baseChangedPartitionNames.get(x)))
                    .collect(Collectors.toList());
            Map<Table, Map<String, Range<PartitionKey>>> refBaseTableRangePartitionMap =
                    mvContext.getRefBaseTableRangePartitionMap();
            Map<String, Range<PartitionKey>> mvRangePartitionMap = mvContext.getMvRangePartitionMap();
            if (mv.isCalcPotentialRefreshPartition(baseTableWithPartitions,
                    refBaseTableRangePartitionMap, needRefreshMvPartitionNames, mvRangePartitionMap)) {
                // because the relation of partitions between materialized view and base partition table is n : m,
                // should calculate the candidate partitions recursively.
                LOG.info("Start calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                        " baseChangedPartitionNames: {}", needRefreshMvPartitionNames, baseChangedPartitionNames);
                SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                        mvContext.getRefBaseTableMVIntersectedPartitions(),
                        mvContext.getMvRefBaseTableIntersectedPartitions(),
                        mvPotentialPartitionNames);
                LOG.info("Finish calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                        " baseChangedPartitionNames: {}", needRefreshMvPartitionNames, baseChangedPartitionNames);
            }
        }
        return needRefreshMvPartitionNames;
    }


    @Override
    public Set<String> getMVPartitionNamesWithTTL(MaterializedView materializedView, String start, String end,
                                                  int partitionTTLNumber, boolean isAutoRefresh) throws AnalysisException {
        int autoRefreshPartitionsLimit = materializedView.getTableProperty().getAutoRefreshPartitionsLimit();
        boolean hasPartitionRange = StringUtils.isNoneEmpty(start) || StringUtils.isNoneEmpty(end);

        if (hasPartitionRange) {
            Set<String> result = Sets.newHashSet();
            Column partitionColumn =
                    ((RangePartitionInfo) materializedView.getPartitionInfo()).getPartitionColumns().get(0);
            Range<PartitionKey> rangeToInclude = createRange(start, end, partitionColumn);
            Map<String, Range<PartitionKey>> rangeMap = materializedView.getValidRangePartitionMap(partitionTTLNumber);
            for (Map.Entry<String, Range<PartitionKey>> entry : rangeMap.entrySet()) {
                Range<PartitionKey> rangeToCheck = entry.getValue();
                int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
                int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
                if (!(lowerCmp >= 0 || upperCmp <= 0)) {
                    result.add(entry.getKey());
                }
            }
            return result;
        }

        int lastPartitionNum;
        if (partitionTTLNumber > 0 && isAutoRefresh && autoRefreshPartitionsLimit > 0) {
            lastPartitionNum = Math.min(partitionTTLNumber, autoRefreshPartitionsLimit);
        } else if (isAutoRefresh && autoRefreshPartitionsLimit > 0) {
            lastPartitionNum = autoRefreshPartitionsLimit;
        } else if (partitionTTLNumber > 0) {
            lastPartitionNum = partitionTTLNumber;
        } else {
            lastPartitionNum = TableProperty.INVALID;
        }

        return materializedView.getValidRangePartitionMap(lastPartitionNum).keySet();
    }

    /**
     * @param mvPartitionNames : the need to refresh materialized view partition names
     * @return : the corresponding ref base table partition names to the materialized view partition names
     */
    protected Map<Table, Set<String>> getBasePartitionNamesByMVPartitionNames(Set<String> mvPartitionNames) {
        Map<Table, Set<String>> result = new HashMap<>();
        Map<String, Map<Table, Set<String>>> mvRefBaseTablePartitionMaps =
                mvContext.getMvRefBaseTableIntersectedPartitions();
        for (String mvPartitionName : mvPartitionNames) {
            if (mvRefBaseTablePartitionMaps == null || !mvRefBaseTablePartitionMaps.containsKey(mvPartitionName)) {
                LOG.warn("Cannot find need refreshed mv table partition from synced partition info: {}",
                        mvPartitionName);
                continue;
            }
            Map<Table, Set<String>> mvRefBaseTablePartitionMap = mvRefBaseTablePartitionMaps.get(mvPartitionName);
            for (Map.Entry<Table, Set<String>> entry : mvRefBaseTablePartitionMap.entrySet()) {
                Table baseTable = entry.getKey();
                Set<String> baseTablePartitions = entry.getValue();
                // If the result already contains the base table name, add all new partitions to the existing set
                // If the result doesn't contain the base table name, put the new set into the map
                result.computeIfAbsent(baseTable, k -> Sets.newHashSet()).addAll(baseTablePartitions);
            }
        }
        return result;
    }

    private void addRangePartitions(Database database, MaterializedView mv,
                                    Map<String, Range<PartitionKey>> adds, Map<String, String> partitionProperties,
                                    DistributionDesc distributionDesc) {
        if (adds.isEmpty()) {
            return;
        }
        List<PartitionDesc> partitionDescs = Lists.newArrayList();

        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            Range<PartitionKey> partitionKeyRange = addEntry.getValue();

            String lowerBound = partitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue();
            String upperBound = partitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue();
            boolean isMaxValue = partitionKeyRange.upperEndpoint().isMaxValue();
            PartitionValue upperPartitionValue;
            if (isMaxValue) {
                upperPartitionValue = PartitionValue.MAX_VALUE;
            } else {
                upperPartitionValue = new PartitionValue(upperBound);
            }
            PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(
                    Collections.singletonList(new PartitionValue(lowerBound)),
                    Collections.singletonList(upperPartitionValue));
            SingleRangePartitionDesc singleRangePartitionDesc =
                    new SingleRangePartitionDesc(false, mvPartitionName, partitionKeyDesc, partitionProperties);
            partitionDescs.add(singleRangePartitionDesc);
        }

        // create partitions in small batch, to avoid create too many partitions at once
        for (List<PartitionDesc> batch : ListUtils.partition(partitionDescs, CREATE_PARTITION_BATCH_SIZE)) {
            RangePartitionDesc rangePartitionDesc =
                    new RangePartitionDesc(mv.getPartitionColumnNames(), batch);
            AddPartitionClause alterPartition = new AddPartitionClause(rangePartitionDesc, distributionDesc,
                    partitionProperties, false);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(
                        database, mv.getName(), alterPartition);
            } catch (Exception e) {
                throw new com.starrocks.sql.common.DmlException("Expression add partition failed: %s, db: %s, table: %s",
                        e, e.getMessage(), database.getFullName(), mv.getName());
            }
            Uninterruptibles.sleepUninterruptibly(Config.mv_create_partition_batch_interval_ms, TimeUnit.MILLISECONDS);
        }
    }
}
