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
// limitations under the License

package com.starrocks.scheduler.mv.ivm;

import com.google.api.client.util.Sets;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.BaseMVRefreshProcessor;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrDeltaTrait;
import com.starrocks.sql.plan.ExecPlan;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Incremental View Materialization (IVM) based MV refresh processor:
 * - Collect base table changed snapshot infos.
 * - Build an incremental refresh plan based on the changed version ranges of base tables.
 * - Execute the refresh plan to update the materialized view.
 */
public class IVMBasedMVRefreshProcessor extends BaseMVRefreshProcessor {
    // This map is used to store the temporary tvr version range for each base table
    private final Map<BaseTableInfo, TvrVersionRange> tempMvTvrVersionRangeMap = Maps.newConcurrentMap();

    public IVMBasedMVRefreshProcessor(Database db, MaterializedView mv,
                                      MvTaskRunContext mvContext,
                                      IMaterializedViewMetricsEntity mvEntity) {
        super(db, mv, mvContext, mvEntity, IVMBasedMVRefreshProcessor.class);
    }

    public ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) throws Exception {
        try (Timer ignored = Tracers.watchScope("MVRefreshExternalTable")) {
            // collect base table snapshot infos
            Map<Long, BaseTableSnapshotInfo> snapshotBaseTables = collectBaseTableSnapshotInfos(mv);
            // refresh external table meta cache before sync partitions
            Map<BaseTableSnapshotInfo, Set<String>> baseTableCandidatePartitions = snapshotBaseTables.values()
                    .stream()
                    .map(snapshotTable -> Pair.create(snapshotTable, Sets.<String>newHashSet()))
                    .collect(Collectors.toMap(x -> x.first, x -> x.second));
            refreshExternalTable(baseTableCandidatePartitions);
        }
        // collect base table snapshot infos
        try (Timer ignored = Tracers.watchScope("MVRefreshSyncBaseTableSnapshotInfos")) {
            syncPartitions();
        }
        // collect change snapshots
        Map<BaseTableInfo, TvrVersionRange> baseTableChangedVersionRanges = Maps.newHashMap();
        try (Timer ignored = Tracers.watchScope("MVRefreshCheckChangedVersionRanges")) {
            final Map<BaseTableInfo, TvrVersionRange> mvTvrVersionRangeMap =
                    mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
            for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
                TvrVersionRange changedVersionRange =
                        getBaseTableChangedVersionRange(snapshotInfo, mvTvrVersionRangeMap);
                logger.info("Base table: {}, changed version range: {}",
                        snapshotInfo.getBaseTableInfo().getTableName(), changedVersionRange);
                // collect changed version range
                baseTableChangedVersionRanges.put(snapshotInfo.getBaseTableInfo(), changedVersionRange);
                tempMvTvrVersionRangeMap.put(snapshotInfo.getBaseTableInfo(), TvrTableSnapshot.of(changedVersionRange.to));
            }
        }
        boolean isTaskRunSkipped = baseTableChangedVersionRanges.values().stream()
                .allMatch(TvrVersionRange::isEmpty);
        if (isTaskRunSkipped) {
            logger.info("No base table has changed, skip the refresh for materialized view: {}",
                    mv.getName());
            return new ProcessExecPlan(Constants.TaskRunState.SKIPPED, null, null);
        }
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshPrepareRefreshPlan")) {
            insertStmt = prepareRefreshPlan(baseTableChangedVersionRanges);
        }
        return new ProcessExecPlan(Constants.TaskRunState.SUCCESS, mvContext.getExecPlan(), insertStmt);
    }

    @Override
    public Constants.TaskRunState doProcessTaskRun(TaskRunContext taskRunContext,
                                                   MVRefreshExecutor executor) throws Exception {
        try {
            return doProcessTaskRunImpl(executor);
        } catch (Exception e) {
            logger.warn("Failed to process task run for materialized view: {}, error: {}",
                    mv.getName(), e.getMessage(), e);
            throw e;
        }
    }

    private Constants.TaskRunState doProcessTaskRunImpl(MVRefreshExecutor executor) throws Exception {
        final ProcessExecPlan processExecPlan = getProcessExecPlan(mvContext);
        if (processExecPlan.state() == Constants.TaskRunState.SKIPPED) {
            logger.info("Skip the refresh for materialized view: {}, no base table has changed",
                    mv.getName());
            return Constants.TaskRunState.SKIPPED;
        }

        try (Timer ignored = Tracers.watchScope("MVRefreshMaterializedView")) {
            final InsertStmt insertStmt = processExecPlan.insertStmt();
            final ExecPlan execPlan = processExecPlan.execPlan();
            MaterializedView.AsyncRefreshContext mvRefreshContext =
                    mv.getRefreshScheme().getAsyncRefreshContext();
            logger.info("temp tvr version range map: {}", tempMvTvrVersionRangeMap);
            mvRefreshContext.getTempBaseTableInfoTvrDeltaMap().putAll(tempMvTvrVersionRangeMap);
            executor.executePlan(execPlan, insertStmt);
        }
        return Constants.TaskRunState.SUCCESS;
    }

    private TvrTableDelta getBaseTableChangedVersionRange(BaseTableSnapshotInfo snapshotInfo,
                                                          Map<BaseTableInfo, TvrVersionRange> mvTvrVersionRangeMap) {
        final BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
        final Table snapshotTable = snapshotInfo.getBaseTable();

        Optional<Table> optTable = MvUtils.getTableWithIdentifier(baseTableInfo);
        if (optTable.isEmpty()) {
            throw new SemanticException("Base table %s.%s does not exist",
                    baseTableInfo.getDbName(), baseTableInfo.getTableName());
        }
        if (!snapshotTable.isIcebergTable()) {
            throw new SemanticException("Only support Iceberg table for IVMBasedMVRefreshProcessor, " +
                    "but got: " + snapshotTable.getType());
        }
        IcebergTable icebergTable = (IcebergTable) snapshotTable;

        final TvrTableDelta maxTvrDelta = getMaxBaseTableChangedDelta(baseTableInfo, icebergTable, mvTvrVersionRangeMap);
        return getBaseTableChangedDeltaAdaptive(baseTableInfo, icebergTable, maxTvrDelta);
    }

    private TvrTableDelta getMaxBaseTableChangedDelta(BaseTableInfo baseTableInfo,
                                                        IcebergTable icebergTable,
                                                      Map<BaseTableInfo, TvrVersionRange> mvTvrVersionRangeMap) {
        // For now, we always refresh the latest snapshot from the last refresh.
        // current tvr snapshot
        TvrVersionRange currentTvrSnapshot = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getCurrentTvrSnapshot(baseTableInfo.getDbName(), icebergTable);
        if (currentTvrSnapshot == null || !(currentTvrSnapshot instanceof TvrTableSnapshot)) {
            logger.warn("Current tvr snapshot is null for base table: {}, db: {}",
                    baseTableInfo.getTableName(), baseTableInfo.getDbName());
            throw new SemanticException("Current tvr snapshot is null for base table: %s.%s",
                    baseTableInfo.getDbName(), baseTableInfo.getTableName());
        }
        if (currentTvrSnapshot.to.isMax()) {
            // if the current tvr snapshot is max, it means the table is empty or not ready for refresh
            logger.info("Base table {} is empty or not ready for refresh, skip the refresh",
                    baseTableInfo.getTableName());
            return TvrTableDelta.emptyDelta();
        }
        TvrVersion currentVersion = currentTvrSnapshot.to;
        if (!mvTvrVersionRangeMap.containsKey(baseTableInfo)) {
            // mv has not refreshed yet, so we need to refresh it
            logger.info("Materialized view {} does not have a tvr version range for base table: {}, "
                            + "current tvr snapshot: {}, so we need to refresh it",
                    mv.getName(), baseTableInfo.getTableName(), currentTvrSnapshot);
            return TvrTableDelta.of(TvrVersion.MIN, currentVersion);
        }

        TvrVersionRange beforeTvrVersionRange = mvTvrVersionRangeMap.get(baseTableInfo);
        logger.info("Base table: {}, before tvr version range: {}, current tvr snapshot: {}",
                baseTableInfo.getTableName(), beforeTvrVersionRange, currentTvrSnapshot);
        if (beforeTvrVersionRange == null || !(beforeTvrVersionRange instanceof TvrTableSnapshot)) {
            throw new SemanticException("Materialized view " + mv.getName()
                    + " does not have a valid tvr version range for base table: " + baseTableInfo.getTableName());
        }
        TvrVersion beforeVersion = beforeTvrVersionRange.to;
        if (beforeVersion.equals(currentVersion)) {
            // no change, so we can skip the refresh
            logger.info("Base table {} has not changed", baseTableInfo.getTableName());
            return TvrTableDelta.of(beforeVersion, currentVersion);
        } else if (beforeVersion.isAfter(currentVersion)) {
            // if the before tvr snapshot's to is after the current tvr snapshot's to, throw exception?
            // how to handle this!
            logger.info("Base table {} has a before version {} that is after the current version {}, "
                            + "this should not happen, skip the refresh",
                    baseTableInfo.getTableName(), beforeVersion, currentVersion);
        }
        return TvrTableDelta.of(beforeVersion, currentVersion);
    }

    // TODO: We may introduce a smarter way to determine which incremental snapshot to refresh later.
    private TvrTableDelta getBaseTableChangedDeltaAdaptive(BaseTableInfo baseTableInfo,
                                                           IcebergTable icebergTable,
                                                           TvrTableDelta maxTvrDelta) {
        List<TvrDeltaTrait> tableDeltaTraits = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .listVersionRangesBetween(baseTableInfo.getDbName(), icebergTable,
                        maxTvrDelta.fromSnapshot(), maxTvrDelta.toSnapshot());
        if (tableDeltaTraits.isEmpty()) {
            logger.warn("No tvr delta traits found for base table: {}, db: {}", baseTableInfo.getTableName(),
                    baseTableInfo.getDbName());
            return TvrTableDelta.emptyDelta();
        }
        TvrTableSnapshot fromSnapshot = maxTvrDelta.fromSnapshot();
        long addedRows = 0;
        TvrTableSnapshot toSnapshot = maxTvrDelta.toSnapshot();
        for (TvrDeltaTrait deltaTrait : tableDeltaTraits) {
            // TODO: We may need to handle the case where the deltaTrait is not append-only.
            if (!deltaTrait.isAppendOnly()) {
                throw new SemanticException("TvrDeltaTrait is not append-only for base table: %s.%s",
                        baseTableInfo.getDbName(), baseTableInfo.getTableName());
            }
            addedRows += deltaTrait.getTvrDeltaStats().getChangedRows();
            if (addedRows >= Config.mv_max_rows_per_refresh) {
                toSnapshot = deltaTrait.getTvrDelta().toSnapshot();
                break;
            }
        }
        TvrTableDelta result = TvrTableDelta.of(fromSnapshot.to, toSnapshot.to);
        logger.info("Base table: {}, db: {}, max tvr delta: {}, adaptive tvr delta: {}",
                baseTableInfo.getTableName(), baseTableInfo.getDbName(), maxTvrDelta, result);
        return result;
    }

    private InsertStmt prepareRefreshPlan(Map<BaseTableInfo, TvrVersionRange> baseTableChangedVersionRanges)
            throws AnalysisException, LockTimeoutException {
        ConnectContext ctx = mvContext.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase())
                .setWarehouse(ctx.getCurrentWarehouseName())
                .setCNGroup(ctx.getCurrentComputeResourceName());

        // set tvr target mvid
        ctx.getSessionVariable().setEnableIVMRefresh(true);
        ctx.getSessionVariable().setTvrTargetMvid(GsonUtils.GSON.toJson(mv.getMvId()));

        final Set<Table> baseTables = snapshotBaseTables.values()
                .stream()
                .map(BaseTableSnapshotInfo::getBaseTable)
                .collect(Collectors.toSet());
        changeDefaultConnectContextIfNeeded(ctx, baseTables);

        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshParser")) {
            // generate insert statement from defined query
            insertStmt = generateInsertAst(ctx, Sets.newHashSet(), true);
        }

        PlannerMetaLocker locker = new PlannerMetaLocker(ctx, insertStmt);
        if (!locker.tryLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database in prepareRefreshPlan");
        }
        try (ConnectContext.ScopeGuard guard = ctx.bindScope()) {
            // analyze the insert statement
            try (Timer ignored = Tracers.watchScope("MVRefreshAnalyzer")) {
                analyzeInsertStmt(insertStmt);
                // build the insert plan
                insertStmt = buildInsertPlan(insertStmt, baseTableChangedVersionRanges);
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
            }
        } finally {
            locker.unlock();
        }

        try (Timer ignored = Tracers.watchScope("MVRefreshPlanner")) {
            ExecPlan execPlan = StatementPlanner.plan(insertStmt, ctx);
            mvContext.setExecPlan(execPlan);
        }
        return insertStmt;
    }

    private void analyzeInsertStmt(InsertStmt insertStmt) throws AnalysisException {
        ConnectContext ctx = mvContext.getCtx();
        Analyzer.analyze(insertStmt, ctx);
    }

    private InsertStmt buildInsertPlan(InsertStmt insertStmt,
                                       Map<BaseTableInfo, TvrVersionRange> tvrVersionRangeMap) throws AnalysisException {
        QueryStatement queryStatement = insertStmt.getQueryStatement();
        Multimap<String, TableRelation> tableRelations = AnalyzerUtils.collectAllTableRelation(queryStatement);
        Map<String, TvrVersionRange> baseTableNameToTvrVersionRangeMap = tvrVersionRangeMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getTableName(), Map.Entry::getValue));
        for (Map.Entry<String, TableRelation> entry : tableRelations.entries()) {
            TableRelation tableRelation = entry.getValue();
            Table table = tableRelation.getTable();
            if (!baseTableNameToTvrVersionRangeMap.containsKey(table.getName())) {
                throw new SemanticException("Base table %s.%s is not found in the changed version ranges",
                        tableRelation.getName().getDb(), tableRelation.getName().getTbl());
            }
            TvrVersionRange tvrVersionRange = baseTableNameToTvrVersionRangeMap.get(table.getName());
            tableRelation.setTvrVersionRange(tvrVersionRange);
        }
        return insertStmt;
    }

    @Override
    protected BaseTableSnapshotInfo buildBaseTableSnapshotInfo(BaseTableInfo baseTableInfo, Table table) {
        return new TvrTableSnapshotInfo(baseTableInfo, table);
    }
}
