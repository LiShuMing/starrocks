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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ListPartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MVPCTRefreshListPartitioner extends MVPCTRefreshPartitioner {
    private static final Logger LOG = LogManager.getLogger(MVPCTRefreshListPartitioner.class);

    public MVPCTRefreshListPartitioner(MvTaskRunContext mvContext,
                                       TaskRunContext context,
                                       Database db,
                                       MaterializedView mv) {
        super(mvContext, context, db, mv);
    }

    @Override
    public void syncAddOrDropPartitions() {
        Table partitionBaseTable = mvContext.getRefBaseTable();
        Column partitionColumn = mvContext.getRefBaseTablePartitionColumn();

        ListPartitionDiff listPartitionDiff;
        Map<String, List<List<String>>> baseListPartitionMap;

        Map<String, List<List<String>>> listPartitionMap = mv.getListPartitionMap();
        Locker locker = new Locker();
        if (!locker.tryLockDatabase(db, LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database: " + db.getFullName() + " in syncPartitionsForList");
        }
        try {
            baseListPartitionMap = PartitionUtil.getPartitionList(partitionBaseTable, partitionColumn);
            listPartitionDiff = SyncPartitionUtils.getListPartitionDiff(baseListPartitionMap, listPartitionMap);
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        Map<String, List<List<String>>> deletes = listPartitionDiff.getDeletes();

        // We should delete the old partition first and then add the new one,
        // because the old and new partitions may overlap

        for (String mvPartitionName : deletes.keySet()) {
            dropPartition(db, mv, mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] delete partitions range [{}]",
                mv.getName(), deletes);

        Map<String, String> partitionProperties = getPartitionProperties(mv);
        DistributionDesc distributionDesc = getDistributionDesc(mv);
        Map<String, List<List<String>>> adds = listPartitionDiff.getAdds();
        addListPartitions(db, mv, adds, partitionProperties, distributionDesc);

        LOG.info("The process of synchronizing materialized view [{}] add partitions list [{}]",
                mv.getName(), adds);

        // used to get partitions to refresh
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = Maps.newHashMap();
        for (String partitionName : baseListPartitionMap.keySet()) {
            Map<String, Set<String>> basePartitionNameRef = Maps.newHashMap();
            basePartitionNameRef.put(partitionName, Sets.newHashSet(partitionName));
            baseToMvNameRef.put(partitionBaseTable, basePartitionNameRef);
        }
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = Maps.newHashMap();
        for (String partitionName : listPartitionMap.keySet()) {
            Map<Table, Set<String>> mvPartitionNameRef = Maps.newHashMap();
            mvPartitionNameRef.put(partitionBaseTable, Sets.newHashSet(partitionName));
            mvToBaseNameRef.put(partitionName, mvPartitionNameRef);
        }
        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableListPartitionMap(baseListPartitionMap);
    }

    @Override
    public Expr generatePartitionPredicate(Table table, Set<String> refBaseTablePartitionNames,
                                           Expr mvPartitionSlotRef) throws AnalysisException {
        Map<String, List<List<String>>> baseListPartitionMap = mvContext.getRefBaseTableListPartitionMap();
        Type partitionType = mvContext.getRefBaseTablePartitionColumn().getType();
        List<LiteralExpr> sourceTablePartitionList = Lists.newArrayList();
        for (String tablePartitionName : refBaseTablePartitionNames) {
            List<List<String>> values = baseListPartitionMap.get(tablePartitionName);
            for (List<String> value : values) {
                LiteralExpr partitionValue = new PartitionValue(value.get(0)).getValue(partitionType);
                sourceTablePartitionList.add(partitionValue);
            }
        }
        List<Expr> partitionPredicates = MvUtils.convertList(mvPartitionSlotRef, sourceTablePartitionList);
        return Expr.compoundOr(partitionPredicates);
    }

    @Override
    public Set<String> getMVPartitionsToRefresh(PartitionInfo mvPartitionInfo,
                                                Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                String start, String end, boolean force,
                                                Set<String> mvPotentialPartitionNames) {
        // list partitioned materialized view
        Table refBaseTable = mvContext.getRefBaseTable();
        boolean isAutoRefresh = (mvContext.getTaskType() == Constants.TaskType.PERIODICAL ||
                mvContext.getTaskType() == Constants.TaskType.EVENT_TRIGGERED);
        int partitionTTLNumber = mvContext.getPartitionTTLNumber();
        Set<String> mvListPartitionNames = getMVPartitionNamesWithTTL(mv, start, end, partitionTTLNumber, isAutoRefresh);

        // check non-ref base tables
        if (needsRefreshBasedOnNonRefTables(snapshotBaseTables, refBaseTable)) {
            if (start == null && end == null) {
                // if non partition table changed, should refresh all partitions of materialized view
                return mvListPartitionNames;
            } else {
                // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                // it should be refreshed according to the user-specified range, not all partitions.
                return getMvPartitionNamesToRefresh(refBaseTable,
                        mvListPartitionNames, true);
            }
        }

        // check the ref base table
        return getMvPartitionNamesToRefresh(refBaseTable, mvListPartitionNames, force);
    }

    @Override
    public Set<String> getMVPartitionNamesWithTTL(MaterializedView materializedView,
                                                  String start, String end,
                                                  int partitionTTLNumber,
                                                  boolean isAutoRefresh) {
        int autoRefreshPartitionsLimit = materializedView.getTableProperty().getAutoRefreshPartitionsLimit();
        boolean hasPartitionRange = StringUtils.isNoneEmpty(start) || StringUtils.isNoneEmpty(end);

        if (hasPartitionRange) {
            Set<String> result = Sets.newHashSet();

            Map<String, List<List<String>>> listMap = materializedView.getValidListPartitionMap(partitionTTLNumber);
            for (Map.Entry<String, List<List<String>>> entry : listMap.entrySet()) {
                if (entry.getKey().compareTo(start) >= 0 && entry.getKey().compareTo(end) <= 0) {
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

        return materializedView.getValidListPartitionMap(lastPartitionNum).keySet();
    }

    private void addListPartitions(Database database, MaterializedView materializedView,
                                   Map<String, List<List<String>>> adds, Map<String, String> partitionProperties,
                                   DistributionDesc distributionDesc) {
        if (adds.isEmpty()) {
            return;
        }

        // TODO: batch
        for (Map.Entry<String, List<List<String>>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            List<List<String>> partitionKeyList = addEntry.getValue();
            MultiItemListPartitionDesc multiItemListPartitionDesc =
                    new MultiItemListPartitionDesc(false, mvPartitionName, partitionKeyList, partitionProperties);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(
                        database, materializedView.getName(), new AddPartitionClause(
                                multiItemListPartitionDesc, distributionDesc,
                                partitionProperties, false));
            } catch (Exception e) {
                throw new DmlException("add list partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                        database.getFullName(), materializedView.getName());
            }
        }
    }
}
