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

import com.google.common.base.Joiner;
import com.google.common.collect.Range;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.starrocks.catalog.MvRefreshArbiter.getPartitionNamesToRefreshForMv;

public class MVPCTRefreshPartitionChecker {

    private static final Logger LOG = LogManager.getLogger(MVPCTRefreshPartitionChecker.class);

    private final MaterializedView mv;
    private final MvTaskRunContext mvTaskRunContext;
    private boolean isEventTriggerMVAvailable = false;

    public MVPCTRefreshPartitionChecker(MaterializedView mv, MvTaskRunContext mvTaskRunContext) {
        this.mv = mv;
        this.mvTaskRunContext = mvTaskRunContext;
    }

    /**
     * Check whether the PCT refresh is available or not.
     */
    public boolean isPCTRefreshAvailable(Collection<TableSnapshotInfo> snapshotBaseTables) {
        // check whether the event trigger mv needs refresh, once it has checked, it will not check again.
        if (!isEventTriggerMVAvailable && !checkEventTriggerMVNeedsRefresh()) {
            return false;
        }
        isEventTriggerMVAvailable = true;

        // Check whether the base table's partition has changed or not.
        if (checkBaseTablePartitionsHaveChanged(mv, snapshotBaseTables)) {
            return false;
        }
        return true;
    }

    /**
     * Make sure event trigger mv needs refresh, otherwise return false.
     * @return: true if it's not event trigger mv or event trigger mv needs refresh, otherwise false.
     */
    private boolean checkEventTriggerMVNeedsRefresh() {
        ExecuteOption executeOption = mvTaskRunContext.getExecuteOption();
        if (!mv.isLoadTriggeredRefresh() || !executeOption.getTaskRunTriggerType().isEventTrigger()) {
            LOG.info("Skip to check whether to refresh since it's not an event trigger", mv.getName());
            return true;
        }

        // for event trigger refresh, we expect the mv is refreshed by event trigger.
        MvUpdateInfo mvUpdateInfo = getPartitionNamesToRefreshForMv(mv, false);
        if (mvUpdateInfo == null || !mvUpdateInfo.isNeedRefresh()) {
            LOG.warn("Event trigger mv {} does not need refresh", mv.getName());
            return false;
        }
        LOG.info("Event trigger mv {} has checked: need to refresh", mv.getName());
        return true;
    }

    /**
     * Check whether the base table's partition has changed or not. Wait to refresh until all mv's base tables
     * don't change again.
     * @return: true if the base table's partition has changed, otherwise false.
     */
    private boolean checkBaseTablePartitionsHaveChanged(MaterializedView mv,
                                                        Collection<TableSnapshotInfo> snapshotBaseTables) {
        List<Database> dbs = MvUtils.collectLockDatabasesOfMV(mv);
        Locker locker = new Locker();
        if (!locker.tryLockDatabases(dbs, LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database: " + Joiner.on(",").join(dbs)
                    + " in checkBaseTablePartitionChange");
        }
        // check snapshotBaseTables and current tables in catalog
        try {
            if (snapshotBaseTables.stream().anyMatch(this::checkBaseTablePartitionHasChanged)) {
                return true;
            }
        } finally {
            locker.unlockDatabases(dbs, LockType.READ);
        }
        return false;
    }

    private boolean checkBaseTablePartitionHasChanged(TableSnapshotInfo snapshotInfo) {
        try {
            BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
            Table snapshotTable = snapshotInfo.getBaseTable();

            Optional<Table> tableOptional = MvUtils.getTableWithIdentifier(baseTableInfo);
            if (tableOptional.isEmpty()) {
                return true;
            }
            Table table = tableOptional.get();
            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapShotOlapTable = (OlapTable) snapshotTable;
                PartitionInfo snapshotPartitionInfo = snapShotOlapTable.getPartitionInfo();
                if (snapshotPartitionInfo instanceof SinglePartitionInfo) {
                    Set<String> partitionNames = ((OlapTable) table).getVisiblePartitionNames();
                    if (!snapShotOlapTable.getVisiblePartitionNames().equals(partitionNames)) {
                        // there is partition rename
                        return true;
                    }
                } else if (snapshotPartitionInfo instanceof ListPartitionInfo) {
                    Map<String, List<List<String>>> snapshotPartitionMap =
                            snapShotOlapTable.getListPartitionMap();
                    Map<String, List<List<String>>> currentPartitionMap =
                            ((OlapTable) table).getListPartitionMap();
                    if (SyncPartitionUtils.hasListPartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                } else {
                    Map<String, Range<PartitionKey>> snapshotPartitionMap =
                            snapShotOlapTable.getRangePartitionMap();
                    Map<String, Range<PartitionKey>> currentPartitionMap =
                            ((OlapTable) table).getRangePartitionMap();
                    if (SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                }
            } else if (ConnectorPartitionTraits.isSupported(snapshotTable.getType())) {
                if (snapshotTable.isUnPartitioned()) {
                    return false;
                } else {
                    PartitionInfo mvPartitionInfo = mv.getPartitionInfo();
                    // TODO: Support list partition later.
                    // do not need to check base partition table changed when mv is not partitioned
                    if (!(mvPartitionInfo instanceof ExpressionRangePartitionInfo)) {
                        return false;
                    }

                    Pair<Table, Column> partitionTableAndColumn = mv.getDirectTableAndPartitionColumn();
                    Column partitionColumn = partitionTableAndColumn.second;
                    // TODO: need to consider(non ref-base table's change)
                    // For Non-partition based base table, it's not necessary to check the partition changed.
                    if (!snapshotTable.equals(partitionTableAndColumn.first)
                            || !snapshotTable.containColumn(partitionColumn.getName())) {
                        return false;
                    }
                    Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.getPartitionKeyRange(
                            snapshotTable, partitionColumn, MaterializedView.getPartitionExpr(mv));
                    Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.getPartitionKeyRange(
                            table, partitionColumn, MaterializedView.getPartitionExpr(mv));
                    if (SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                }
            }
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition change failed", e);
            return true;
        }
        return false;
    }
}
