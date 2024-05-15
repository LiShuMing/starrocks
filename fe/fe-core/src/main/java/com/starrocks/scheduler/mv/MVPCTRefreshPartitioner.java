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

import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.common.DmlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.catalog.MvRefreshArbiter.getMvBaseTableUpdateInfo;
import static com.starrocks.catalog.MvRefreshArbiter.needToRefreshTable;

/**
 * MV Refresh Partitioner for Partitioned Materialized View which provide utility methods associated partitions during mv refresh.
 */
public abstract class MVPCTRefreshPartitioner {
    private static final Logger LOG = LogManager.getLogger(MVPCTRefreshPartitioner.class);

    protected final MvTaskRunContext mvContext;
    protected final TaskRunContext context;
    protected final Database db;
    protected final MaterializedView mv;

    public MVPCTRefreshPartitioner(MvTaskRunContext mvContext,
                                   TaskRunContext context,
                                   Database db,
                                   MaterializedView mv) {
        this.mvContext = mvContext;
        this.context = context;
        this.db = db;
        this.mv = mv;
    }

    /**
     * Sync mv and base tables partitions, add if base tables add partitions, drop partitions if base tables drop or changed
     * partitions.
     */
    public abstract void syncAddOrDropPartitions();

    /**
     * Generate partition predicate for mv refresh according ref base table changed partitions.
     * @param refBaseTable: ref base table to check.
     * @param refBaseTablePartitionNames: ref base table partition names to check.
     * @param mvPartitionSlotRef: mv partition slot ref to generate partition predicate.
     * @return: Return partition predicate for mv refresh.
     * @throws AnalysisException
     */
    public abstract Expr generatePartitionPredicate(Table refBaseTable,
                                                    Set<String> refBaseTablePartitionNames,
                                                    Expr mvPartitionSlotRef) throws AnalysisException;

    /**
     * Get mv partitions to refresh based on the ref base table partitions.
     * @param mvPartitionInfo: mv partition info to check.
     * @param snapshotBaseTables: snapshot base tables to check.
     * @param start: start partition name to check.
     * @param end: end partition name to check.
     * @param force: force to refresh or not.
     * @param mvPotentialPartitionNames: mv potential partition names to check.
     * @return: Return mv partitions to refresh based on the ref base table partitions.
     * @throws AnalysisException
     */
    public abstract Set<String> getMVPartitionsToRefresh(PartitionInfo mvPartitionInfo,
                                                         Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                         String start, String end, boolean force,
                                                         Set<String> mvPotentialPartitionNames) throws AnalysisException;

    /**
     * @param refBaseTable : ref base table to check.
     * @param refBaseTablePartitionNames : ref base table partition names to check.
     * @return : Return mv corresponding partition names to the ref base table partition names.
     */
    protected Set<String> getMVAssociatedPartitionNames(Table refBaseTable,
                                                        Set<String> refBaseTablePartitionNames) {
        Set<String> result = Sets.newHashSet();
        Map<Table, Map<String, Set<String>>> refBaseTableMVPartitionMaps = mvContext.getRefBaseTableMVIntersectedPartitions();
        if (refBaseTableMVPartitionMaps == null || !refBaseTableMVPartitionMaps.containsKey(refBaseTable)) {
            return refBaseTablePartitionNames;
        }
        Map<String, Set<String>> refBaseTableMVPartitionMap = refBaseTableMVPartitionMaps.get(refBaseTable);
        for (String basePartitionName : refBaseTablePartitionNames) {
            if (!refBaseTableMVPartitionMap.containsKey(basePartitionName)) {
                LOG.warn("Cannot find need refreshed ref base table partition from synced partition info: {}",
                        basePartitionName);
                continue;
            }
            result.addAll(refBaseTableMVPartitionMap.get(basePartitionName));
        }
        return result;
    }

    /**
     * Check whether the base table is supported partition refresh or not.
     */
    public static boolean isPartitionRefreshSupported(Table baseTable) {
        return ConnectorPartitionTraits.build(baseTable).supportPartitionRefresh();
    }

    protected Set<String> getMvPartitionNamesToRefresh(Table refBaseTable,
                                                       Set<String> mvRangePartitionNames,
                                                       boolean force) {
        // refresh all mv partitions when the ref base table is not supported partition refresh
        if (force || !isPartitionRefreshSupported(refBaseTable)) {
            return Sets.newHashSet(mvRangePartitionNames);
        }

        // step1: check updated partition names in the ref base table and add it to the refresh candidate
        MvBaseTableUpdateInfo mvBaseTableUpdateInfo = getMvBaseTableUpdateInfo(mv, refBaseTable, false, false);
        if (mvBaseTableUpdateInfo == null) {
            return mvRangePartitionNames;
        }

        // step2: fetch the corresponding materialized view partition names as the need to refresh partitions
        Set<String> updatePartitionNames = mvBaseTableUpdateInfo.getToRefreshPartitionNames();
        Set<String> result = getMVAssociatedPartitionNames(refBaseTable, updatePartitionNames);
        result.retainAll(mvRangePartitionNames);
        return result;
    }

    /**
     * Whether partitioned materialized view needs to be refreshed or not base on the non-ref base tables, it needs refresh when:
     * - its non-ref base table except un-supported base table has updated.
     */
    protected boolean needsRefreshBasedOnNonRefTables(Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                      Table refBaseTable) {
        Map<Table, Column> tableColumnMap = mv.getRelatedPartitionTableAndColumn();
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            if (snapshotTable.getId() == refBaseTable.getId()) {
                continue;
            }
            if (!isPartitionRefreshSupported(snapshotTable)) {
                continue;
            }
            if (tableColumnMap.containsKey(snapshotTable)) {
                continue;
            }
            if (needToRefreshTable(mv, snapshotTable)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Whether non-partitioned materialized view needs to be refreshed or not, it needs refresh when:
     * - its base table is not supported refresh by partition.
     * - its base table has updated.
     */
    public static boolean isNonPartitionedMVNeedToRefresh(Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                          MaterializedView mv) {
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            if (!isPartitionRefreshSupported(snapshotTable)) {
                return true;
            }
            if (needToRefreshTable(mv, snapshotTable)) {
                return true;
            }
        }
        return false;
    }

    protected Map<String, String> getPartitionProperties(MaterializedView materializedView) {
        Map<String, String> partitionProperties = new HashMap<>(4);
        partitionProperties.put("replication_num",
                String.valueOf(materializedView.getDefaultReplicationNum()));
        partitionProperties.put("storage_medium", materializedView.getStorageMedium());
        String storageCooldownTime =
                materializedView.getTableProperty().getProperties().get("storage_cooldown_time");
        if (storageCooldownTime != null
                && !storageCooldownTime.equals(String.valueOf(DataProperty.MAX_COOLDOWN_TIME_MS))) {
            // cast long str to time str e.g.  '1587473111000' -> '2020-04-21 15:00:00'
            String storageCooldownTimeStr = TimeUtils.longToTimeString(Long.parseLong(storageCooldownTime));
            partitionProperties.put("storage_cooldown_time", storageCooldownTimeStr);
        }
        return partitionProperties;
    }

    protected DistributionDesc getDistributionDesc(MaterializedView materializedView) {
        DistributionInfo distributionInfo = materializedView.getDefaultDistributionInfo();
        if (distributionInfo instanceof HashDistributionInfo) {
            List<String> distColumnNames = new ArrayList<>();
            for (Column distributionColumn : (distributionInfo).getDistributionColumns()) {
                distColumnNames.add(distributionColumn.getName());
            }
            return new HashDistributionDesc(distributionInfo.getBucketNum(), distColumnNames);
        } else {
            return new RandomDistributionDesc();
        }
    }

    protected void dropPartition(Database db, MaterializedView materializedView, String mvPartitionName) {
        String dropPartitionName = materializedView.getPartition(mvPartitionName).getName();
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
            throw new DmlException("drop partition failed. database:" + db.getFullName() + " not exist");
        }
        try {
            // check
            Table mv = db.getTable(materializedView.getId());
            if (mv == null) {
                throw new DmlException("drop partition failed. mv:" + materializedView.getName() + " not exist");
            }
            Partition mvPartition = mv.getPartition(dropPartitionName);
            if (mvPartition == null) {
                throw new DmlException("drop partition failed. partition:" + dropPartitionName + " not exist");
            }

            GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(
                    db, materializedView,
                    new DropPartitionClause(false, dropPartitionName, false, true));
        } catch (Exception e) {
            throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                    db.getFullName(), materializedView.getName());
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }
}
