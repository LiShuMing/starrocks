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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockParams;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.HivePartitionDataInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.TableUpdateArbitrator;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.scheduler.TaskRun.MV_ID;
import static com.starrocks.scheduler.TaskRun.MV_UNCOPYABLE_PROPERTIES;

/**
 * Core logic of mv refresh task run
 * PartitionBasedMvRefreshProcessor is not thread safe for concurrent runs of the same materialized view
 */
public class PartitionBasedMvRefreshProcessor extends BaseMVRefreshProcessor {
    private Set<String> mvToRefreshedPartitions = null;
    private Map<String, Set<String>> refTablePartitionNames = null;
    private Map<BaseTableSnapshotInfo, Set<String>> refTableRefreshPartitions = null;

    public PartitionBasedMvRefreshProcessor(Database db, MaterializedView mv,
                                            MvTaskRunContext mvContext,
                                            IMaterializedViewMetricsEntity mvEntity) {
        super(db, mv, mvContext, mvEntity, PartitionBasedMvRefreshProcessor.class);
    }

    // Core logics:
    // 1. prepare to check some conditions
    // 2. sync partitions with base tables(add or drop partitions, which will be optimized  by dynamic partition creation later)
    // 3. decide which partitions of mv to refresh and the corresponding base tables' source partitions
    // 4. construct the refresh sql and execute it
    // 5. update the source table version map if refresh task completes successfully
    @Override
    public Constants.TaskRunState doProcessTaskRun(TaskRunContext taskRunContext,
                                                   MVRefreshExecutor executor) throws Exception {
        long startRefreshTs = System.currentTimeMillis();
        // log mv basic info, it may throw exception if mv is invalid since base table has dropped
        try {
            logger.debug("refBaseTablePartitionExprMap:{},refBaseTablePartitionSlotMap:{}, " +
                            "refBaseTablePartitionColumnMap:{},baseTableInfos:{}",
                    mv.getRefBaseTablePartitionExprs(), mv.getRefBaseTablePartitionSlots(),
                    mv.getRefBaseTablePartitionColumns(), MvUtils.formatBaseTableInfos(mv.getBaseTableInfos()));
        } catch (Throwable e) {
            logger.warn("Log mv basic info failed:", e);
        }

        // refresh materialized view
        Constants.TaskRunState result = doRefreshMaterializedView(taskRunContext, executor);

        // do not generate next task run if the current task run is killed
        if (mvContext.hasNextBatchPartition() && !mvContext.getTaskRun().isKilled()) {
            generateNextTaskRun();
        }

        long refreshDurationMs = System.currentTimeMillis() - startRefreshTs;
        logger.info("refresh mv success, cost time(ms): {}", DebugUtil.DECIMAL_FORMAT_SCALE_3.format(refreshDurationMs));
        mvEntity.updateRefreshDuration(refreshDurationMs);
        return result;
    }

    private void logMvToRefreshInfoIntoTaskRun(Set<String> finalMvToRefreshedPartitions,
                                               Map<String, Set<String>> finalRefTablePartitionNames) {
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setMvPartitionsToRefresh(finalMvToRefreshedPartitions);
            extraMessage.setRefBasePartitionsToRefreshMap(finalRefTablePartitionNames);
        });
    }

    @Override
    public int getRetryTimes(ConnectContext connectContext) {
        int maxRefreshMaterializedViewRetryNum = 1;
        if (connectContext != null && connectContext.getSessionVariable() != null) {
            maxRefreshMaterializedViewRetryNum =
                    connectContext.getSessionVariable().getQueryDebugOptions().getMaxRefreshMaterializedViewRetryNum();
            if (maxRefreshMaterializedViewRetryNum <= 0) {
                maxRefreshMaterializedViewRetryNum = 1;
            }
        }
        maxRefreshMaterializedViewRetryNum = Math.max(Config.max_mv_refresh_failure_retry_times,
                maxRefreshMaterializedViewRetryNum);
        return maxRefreshMaterializedViewRetryNum;
    }

    @Override
    public ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) throws Exception {
        // 0. Compute the base-table partitions to check for external table
        // The candidate partition info is used to refresh the external table
        Map<BaseTableSnapshotInfo, Set<String>> baseTableCandidatePartitions = Maps.newHashMap();
        if (Config.enable_materialized_view_external_table_precise_refresh) {
            try (Timer ignored = Tracers.watchScope("MVRefreshComputeCandidatePartitions")) {
                if (!syncPartitions()) {
                    throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                            mv.getName()));
                }
                Set<String> mvCandidatePartition = getMVToRefreshedPartitions(taskRunContext, true);
                baseTableCandidatePartitions = getRefTableRefreshPartitions(mvCandidatePartition);
            } catch (Exception e) {
                logger.warn("failed to compute candidate partitions in sync partitions", DebugUtil.getRootStackTrace(e));
                // Since at here we sync partitions before the refreshExternalTable, the situation may happen that
                // the base-table not exists before refreshExternalTable, so we just need to swallow this exception
                if (e.getMessage() == null || !e.getMessage().contains("not exist")) {
                    throw e;
                }
            }
        }

        // 1. Refresh the partition information of these base-table partitions, and create mv partitions if needed
        try (Timer ignored = Tracers.watchScope("MVRefreshSyncAndCheckPartitions")) {
            if (!syncAndCheckPartitions(baseTableCandidatePartitions)) {
                throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                        mv.getName()));
            }
        }

        try (Timer ignored = Tracers.watchScope("MVRefreshCheckMVToRefreshPartitions")) {
            mvToRefreshedPartitions = getMVToRefreshedPartitions(taskRunContext, false);
            if (CollectionUtils.isEmpty(mvToRefreshedPartitions)) {
                return new ProcessExecPlan(Constants.TaskRunState.SKIPPED, null, null);
            }
            // ref table of mv : refreshed partition names
            refTableRefreshPartitions = getRefTableRefreshPartitions(mvToRefreshedPartitions);
            // ref table of mv : refreshed partition names
            refTablePartitionNames = refTableRefreshPartitions.entrySet().stream()
                    .collect(Collectors.toMap(x -> x.getKey().getName(), Map.Entry::getValue));
            logger.info("mvToRefreshedPartitions:{}, refTableRefreshPartitions:{}",
                    mvToRefreshedPartitions, refTableRefreshPartitions);
            // add a message into information_schema
            logMvToRefreshInfoIntoTaskRun(mvToRefreshedPartitions, refTablePartitionNames);
            updateBaseTablePartitionSnapshotInfos(refTableRefreshPartitions);
        }

        ///// 2. execute the ExecPlan of insert stmt
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshPrepareRefreshPlan")) {
            insertStmt = prepareRefreshPlan(mvToRefreshedPartitions, refTablePartitionNames);
        }
        return new ProcessExecPlan(Constants.TaskRunState.SUCCESS, mvContext.getExecPlan(), insertStmt);
    }

    private Constants.TaskRunState doRefreshMaterializedView(TaskRunContext context,
                                                             MVRefreshExecutor executor) throws Exception {
        final ProcessExecPlan processExecPlan = getProcessExecPlan(context);
        if (processExecPlan.state() == Constants.TaskRunState.SKIPPED) {
            logger.info("MV {} refresh task skipped, no partitions to refresh", mv.getName());
            return Constants.TaskRunState.SKIPPED;
        }

        final ExecPlan mvExecPlan = processExecPlan.execPlan();
        final InsertStmt insertStmt = processExecPlan.insertStmt();
        try (Timer ignored = Tracers.watchScope("MVRefreshMaterializedView")) {
            executor.executePlan(mvExecPlan, insertStmt);
        }

        ///// 3. insert execute successfully, update the meta of mv according to ExecPlan
        try (Timer ignored = Tracers.watchScope("MVRefreshUpdateMeta")) {
            updateMeta(mvToRefreshedPartitions, mvExecPlan, refTableRefreshPartitions);
        }

        return Constants.TaskRunState.SUCCESS;
    }

    /**
     * Sync partitions of base tables and check whether they are changing anymore
     */
    protected boolean syncAndCheckPartitions(Map<BaseTableSnapshotInfo, Set<String>> baseTableCandidatePartitions)
            throws AnalysisException, LockTimeoutException {
        // collect partition infos of ref base tables
        int retryNum = 0;
        boolean checked = false;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (!checked && retryNum++ < Config.max_mv_check_base_table_change_retry_times) {
            mvEntity.increaseRefreshRetryMetaCount(1L);
            try (Timer ignored = Tracers.watchScope("MVRefreshExternalTable")) {
                // refresh external table meta cache before sync partitions
                refreshExternalTable(baseTableCandidatePartitions);
            }

            if (!Config.enable_materialized_view_external_table_precise_refresh || retryNum > 1) {
                try (Timer ignored = Tracers.watchScope("MVRefreshSyncPartitions")) {
                    // sync partitions between mv and base tables out of lock
                    // do it outside lock because it is a time-cost operation
                    if (!syncPartitions()) {
                        logger.warn("Sync partitions failed.");
                        return false;
                    }
                }
            }

            try (Timer ignored = Tracers.watchScope("MVRefreshCheckBaseTableChange")) {
                // check whether there are partition changes for base tables, eg: partition rename
                // retry to sync partitions if any base table changed the partition infos
                if (checkBaseTablePartitionChange(mv)) {
                    logger.info("materialized view base partition has changed. retry to sync partitions, retryNum:{}", retryNum);
                    // sleep 100ms
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    continue;
                }
            }
            checked = true;
        }
        Tracers.record("MVRefreshSyncPartitionsRetryTimes", String.valueOf(retryNum));
        logger.info("sync and check mv partition changing after {} times: {}, costs: {} ms",
                retryNum, checked, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return checked;
    }


    /**
     * Check whether the base table's partition has changed or not. Wait to refresh until all mv's base tables
     * don't change again.
     * @return: true if the base table's partition has changed, otherwise false.
     */
    private boolean checkBaseTablePartitionChange(MaterializedView mv) throws LockTimeoutException {
        LockParams lockParams = collectDatabases(mv);
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(lockParams,
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            logger.warn("failed to lock database: {} in checkBaseTablePartitionChange", lockParams);
            throw new LockTimeoutException("Failed to lock database: " + lockParams
                    + " in checkBaseTablePartitionChange");
        }
        // check snapshotBaseTables and current tables in catalog
        try {
            return snapshotBaseTables.values().stream().anyMatch(this::checkBaseTablePartitionHasChanged);
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
        }
    }

    private boolean checkBaseTablePartitionHasChanged(BaseTableSnapshotInfo snapshotInfo) {
        try {
            BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
            Table snapshotTable = snapshotInfo.getBaseTable();

            Optional<Table> optTable = MvUtils.getTableWithIdentifier(baseTableInfo);
            if (optTable.isEmpty()) {
                return true;
            }
            Table table = optTable.get();
            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapShotOlapTable = (OlapTable) snapshotTable;
                PartitionInfo snapshotPartitionInfo = snapShotOlapTable.getPartitionInfo();
                if (snapshotPartitionInfo.isUnPartitioned()) {
                    Set<String> partitionNames = ((OlapTable) table).getVisiblePartitionNames();
                    if (!snapShotOlapTable.getVisiblePartitionNames().equals(partitionNames)) {
                        // there is partition rename
                        return true;
                    }
                } else if (snapshotPartitionInfo.isListPartition()) {
                    OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                    Map<String, PListCell> snapshotPartitionMap = snapshotOlapTable.getListPartitionItems();
                    Map<String, PListCell> currentPartitionMap = snapshotOlapTable.getListPartitionItems();
                    if (ListPartitionDiffer.hasListPartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                } else {
                    Map<String, Range<PartitionKey>> snapshotPartitionMap = snapShotOlapTable.getRangePartitionMap();
                    Map<String, Range<PartitionKey>> currentPartitionMap = ((OlapTable) table).getRangePartitionMap();
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
                    if (!(mvPartitionInfo.isRangePartition())) {
                        return false;
                    }
                    Map<Table, List<Column>> partitionTableAndColumns = mv.getRefBaseTablePartitionColumns();
                    // For Non-partition based base table, it's not necessary to check the partition changed.
                    if (!partitionTableAndColumns.containsKey(snapshotTable)) {
                        return false;
                    }
                    List<Column> partitionColumns = partitionTableAndColumns.get(snapshotTable);
                    Preconditions.checkArgument(partitionColumns.size() == 1,
                            "Only support one partition column in range partition");
                    Column partitionColumn = partitionColumns.get(0);
                    Optional<Expr> rangePartitionExprOpt = mv.getRangePartitionFirstExpr();
                    if (rangePartitionExprOpt.isEmpty()) {
                        return false;
                    }
                    Expr rangePartitionExpr = rangePartitionExprOpt.get();
                    Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.getPartitionKeyRange(
                            snapshotTable, partitionColumn, rangePartitionExpr);
                    Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.getPartitionKeyRange(
                            table, partitionColumn, rangePartitionExpr);
                    if (SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                }
            }
        } catch (StarRocksException e) {
            logger.warn("Materialized view compute partition change failed", DebugUtil.getRootStackTrace(e));
            return true;
        }
        return false;
    }

    /**
     * @param mvToRefreshedPartitions :  to-refreshed mv partition names
     * @return : return to-refreshed base table's table name and partition names mapping
     */
    @VisibleForTesting
    public Map<BaseTableSnapshotInfo, Set<String>> getRefTableRefreshPartitions(Set<String> mvToRefreshedPartitions) {
        Map<BaseTableSnapshotInfo, Set<String>> refTableAndPartitionNames = Maps.newHashMap();
        Map<String, Map<Table, Set<String>>> mvToBaseNameRefs = mvContext.getMvRefBaseTableIntersectedPartitions();
        if (mvToBaseNameRefs == null || mvToBaseNameRefs.isEmpty()) {
            return refTableAndPartitionNames;
        }
        for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            Set<String> needRefreshTablePartitionNames = null;
            for (String mvPartitionName : mvToRefreshedPartitions) {
                if (!mvToBaseNameRefs.containsKey(mvPartitionName)) {
                    continue;
                }
                Map<Table, Set<String>> mvToBaseNameRef = mvToBaseNameRefs.get(mvPartitionName);
                if (mvToBaseNameRef.containsKey(snapshotTable)) {
                    if (needRefreshTablePartitionNames == null) {
                        needRefreshTablePartitionNames = Sets.newHashSet();
                    }
                    // The table in this map has related partition with mv
                    // It's ok to add empty set for a table, means no partition corresponding to this mv partition
                    needRefreshTablePartitionNames.addAll(mvToBaseNameRef.get(snapshotTable));
                } else {
                    logger.info("ref-base-table {} is not found in `mvRefBaseTableIntersectedPartitions` " +
                            "because of empty update", snapshotTable.getName());
                }
            }
            if (needRefreshTablePartitionNames != null) {
                refTableAndPartitionNames.put(snapshotInfo, needRefreshTablePartitionNames);
            }
        }
        return refTableAndPartitionNames;
    }

    public Set<String> getMVToRefreshedPartitions(TaskRunContext context,
                                                  boolean tentative)
            throws AnalysisException, LockTimeoutException {
        MaterializedView.PartitionRefreshStrategy partitionRefreshStrategy = MaterializedView.PartitionRefreshStrategy.valueOf(
                mv.getTableProperty().getPartitionRefreshStrategy().trim().toUpperCase());
        boolean isForce = partitionRefreshStrategy == MaterializedView.PartitionRefreshStrategy.FORCE || tentative;
        final MVRefreshParams mvRefreshParams = new MVRefreshParams(mv.getPartitionInfo(), context.getProperties(), isForce);

        final Set<String> mvPotentialPartitionNames = Sets.newHashSet();
        Set<String> mvToRefreshedPartitions = mvRefreshPartitioner.getMVToRefreshedPartitions(
                snapshotBaseTables, mvRefreshParams, partitionRefreshStrategy, mvPotentialPartitionNames, tentative);
        // update mv extra message
        if (!tentative) {
            updateTaskRunStatus(status -> {
                MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
                extraMessage.setForceRefresh(mvRefreshParams.isForce());
                extraMessage.setPartitionStart(mvRefreshParams.getRangeStart());
                extraMessage.setPartitionEnd(mvRefreshParams.getRangeEnd());
            });
        }
        return mvToRefreshedPartitions;
    }

    /**
     * Prepare the statement and plan for mv refreshing, considering the partitions of ref table
     */
    private InsertStmt prepareRefreshPlan(Set<String> mvToRefreshedPartitions, Map<String, Set<String>> refTablePartitionNames)
            throws AnalysisException, LockTimeoutException {
        // 1. Prepare context
        ConnectContext ctx = mvContext.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase())
                .setWarehouse(ctx.getCurrentWarehouseName())
                .setCNGroup(ctx.getCurrentComputeResourceName());

        // 2. Prepare variables
        final Set<Table> baseTables = snapshotBaseTables.values()
                .stream()
                .map(BaseTableSnapshotInfo::getBaseTable)
                .collect(Collectors.toSet());
        changeDefaultConnectContextIfNeeded(ctx, baseTables);

        // 3. AST
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshParser")) {
            insertStmt = generateInsertAst(ctx, mvToRefreshedPartitions, false);
        }

        PlannerMetaLocker locker = new PlannerMetaLocker(ctx, insertStmt);
        ExecPlan execPlan = null;
        if (!locker.tryLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database in prepareRefreshPlan");
        }

        MVPCTRefreshPlanBuilder planBuilder = new MVPCTRefreshPlanBuilder(db, mv, mvContext, mvRefreshPartitioner);
        try {
            // 4. Analyze and prepare a partition & Rebuild insert statement by
            // considering to-refresh partitions of ref tables/ mv
            try (Timer ignored = Tracers.watchScope("MVRefreshAnalyzer")) {
                insertStmt = planBuilder.analyzeAndBuildInsertPlan(insertStmt,
                        mvToRefreshedPartitions, refTablePartitionNames, ctx);
                // Must set execution id before StatementPlanner.plan
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
            }

            // 5. generate insert stmt's exec plan, make thread local ctx existed
            try (ConnectContext.ScopeGuard guard = ctx.bindScope(); Timer ignored = Tracers.watchScope("MVRefreshPlanner")) {
                execPlan = StatementPlanner.planInsertStmt(locker, insertStmt, ctx);
            }
        } finally {
            locker.unlock();
        }

        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage message = status.getMvTaskRunExtraMessage();
            if (message == null) {
                return;
            }
            Map<String, String> planBuildMessage = planBuilder.getPlanBuilderMessage();
            logger.info("MV Refresh PlanBuilderMessage: {}", planBuildMessage);
            message.setPlanBuilderMessage(planBuildMessage);
        });

        QueryDebugOptions debugOptions = ctx.getSessionVariable().getQueryDebugOptions();
        // log the final mv refresh plan for each refresh for better trace and debug
        if (logger.isDebugEnabled() || debugOptions.isEnableQueryTraceLog()) {
            logger.info("MV Refresh Final Plan\nMV PartitionsToRefresh: {}\nBase PartitionsToScan: {}\n" +
                            "Insert Plan:\n{}",
                    String.join(",", mvToRefreshedPartitions), refTablePartitionNames,
                    execPlan != null ? execPlan.getExplainString(StatementBase.ExplainLevel.VERBOSE) : "");
        } else {
            logger.info("MV Refresh Final Plan, MV PartitionsToRefresh: {}, Base PartitionsToScan: {}",
                    String.join(",", mvToRefreshedPartitions), refTablePartitionNames);
        }
        mvContext.setExecPlan(execPlan);
        return insertStmt;
    }

    private void generateNextTaskRun() {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Map<String, String> properties = mvContext.getProperties();
        long mvId = Long.parseLong(properties.get(MV_ID));
        String taskName = TaskBuilder.getMvTaskName(mvId);
        Map<String, String> newProperties = Maps.newHashMap();
        for (Map.Entry<String, String> proEntry : properties.entrySet()) {
            // skip uncopyable properties: force/partition_values/... which only can be set specifically.
            if (proEntry.getKey() == null || proEntry.getValue() == null
                    || MV_UNCOPYABLE_PROPERTIES.contains(proEntry.getKey())) {
                continue;
            }
            newProperties.put(proEntry.getKey(), proEntry.getValue());
        }
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isListPartition()) {
            //TODO: partition values may be too long, need to be optimized later.
            newProperties.put(TaskRun.PARTITION_VALUES, mvContext.getNextPartitionValues());
        } else {
            newProperties.put(TaskRun.PARTITION_START, mvContext.getNextPartitionStart());
            newProperties.put(TaskRun.PARTITION_END, mvContext.getNextPartitionEnd());
        }
        if (mvContext.getStatus() != null) {
            newProperties.put(TaskRun.START_TASK_RUN_ID, mvContext.getStatus().getStartTaskRunId());
        }
        // warehouse
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            newProperties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
        }
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setNextPartitionStart(mvContext.getNextPartitionStart());
            extraMessage.setNextPartitionEnd(mvContext.getNextPartitionEnd());
            extraMessage.setNextPartitionValues(mvContext.getNextPartitionValues());
        });

        // Partition refreshing task run should have the HIGHER priority, and be scheduled before other tasks
        // Otherwise this round of partition refreshing would be staved and never got finished
        ExecuteOption executeOption = mvContext.getExecuteOption();
        int priority = executeOption.getPriority() > Constants.TaskRunPriority.LOWEST.value() ?
                executeOption.getPriority() : Constants.TaskRunPriority.HIGHER.value();
        ExecuteOption option = new ExecuteOption(priority, true, newProperties);
        logger.info("[MV] Generate a task to refresh next batches of partitions for MV {}-{}, start={}, end={}, " +
                        "priority={}, properties={}", mv.getName(), mv.getId(),
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(), priority, properties);

        if (properties.containsKey(TaskRun.IS_TEST) && properties.get(TaskRun.IS_TEST).equalsIgnoreCase("true")) {
            // for testing
            TaskRun taskRun = TaskRunBuilder
                    .newBuilder(taskManager.getTask(taskName))
                    .properties(option.getTaskRunProperties())
                    .setExecuteOption(option)
                    .build();
            nextTaskRun = taskRun;
        } else {
            taskManager.executeTask(taskName, option);
        }
    }

    /**
     * After mv is refreshed, update materialized view's meta info to record history refreshes.
     *
     * @param refTableAndPartitionNames : refreshed base table and its partition names mapping.
     */
    private void updateMeta(Set<String> mvRefreshedPartitions,
                            ExecPlan execPlan,
                            Map<BaseTableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        // check
        Table mv = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), this.mv.getId());
        if (mv == null) {
            throw new DmlException("update meta failed. materialized view:" + this.mv.getName() + " not exist");
        }
        // check
        if (mvRefreshedPartitions == null || refTableAndPartitionNames == null) {
            logger.info("no partitions to refresh, mvRefreshedPartitions:{}, refTableAndPartitionNames:{}",
                    mvRefreshedPartitions, refTableAndPartitionNames);
            return;
        }

        // update mv's version info
        Set<Long> refBaseTableIds = refTableAndPartitionNames.keySet().stream()
                .map(t -> t.getId())
                .collect(Collectors.toSet());

        Locker locker = new Locker();
        // update the meta if succeed
        if (!locker.lockDatabaseAndCheckExist(db, this.mv, LockType.WRITE)) {
            logger.warn("failed to lock database: {} in updateMeta for mv refresh", db.getFullName());
            throw new DmlException("update meta failed. database:" + db.getFullName() + " not exist");
        }

        MVVersionManager mvVersionManager = new MVVersionManager(this.mv, mvContext);
        try {
            mvVersionManager.updateMVVersionInfo(snapshotBaseTables, mvRefreshedPartitions,
                    refBaseTableIds, refTableAndPartitionNames);
        } catch (Exception e) {
            logger.warn("update final meta failed after mv refreshed:", DebugUtil.getRootStackTrace(e));
            throw e;
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), this.mv.getId(), LockType.WRITE);
        }

        // update mv status message
        updateTaskRunStatus(status -> {
            try {
                MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
                Map<String, Set<String>> baseTableRefreshedPartitionsByExecPlan =
                        MVTraceUtils.getBaseTableRefreshedPartitionsByExecPlan(this.mv, execPlan);
                extraMessage.setBasePartitionsToRefreshMap(baseTableRefreshedPartitionsByExecPlan);
            } catch (Exception e) {
                // just log warn and no throw exceptions for an updating task runs message.
                logger.warn("update task run messages failed:", DebugUtil.getRootStackTrace(e));
            }
        });
    }

    @VisibleForTesting
    public void updateBaseTablePartitionSnapshotInfos(Map<BaseTableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        // NOTE: For each task run, ref-base table's incremental partition and all non-ref base tables' partitions
        // are refreshed, so we need record it into materialized view.
        // NOTE: We don't use the pruned partition infos from ExecPlan because the optimized partition infos are not
        // exact to describe which partitions are refreshed.
        Map<BaseTableSnapshotInfo, Set<String>> baseTableAndPartitionNames = Maps.newHashMap();
        for (Map.Entry<BaseTableSnapshotInfo, Set<String>> e : refTableAndPartitionNames.entrySet()) {
            PCTTableSnapshotInfo snapshotInfo = (PCTTableSnapshotInfo) e.getKey();
            Table baseTable = snapshotInfo.getBaseTable();
            Set<String> realPartitionNames = e.getValue().stream()
                    .flatMap(name -> mvContext.getExternalTableRealPartitionName(baseTable, name).stream())
                    .collect(Collectors.toSet());
            baseTableAndPartitionNames.put(snapshotInfo, realPartitionNames);
        }

        // Update all base tables' version map to track all tables' changes used in query rewrite.
        Map<BaseTableSnapshotInfo, Set<String>> nonRefTableAndPartitionNames = getNonRefTableRefreshPartitions();
        if (!nonRefTableAndPartitionNames.isEmpty()) {
            baseTableAndPartitionNames.putAll(nonRefTableAndPartitionNames);
        }

        for (Map.Entry<BaseTableSnapshotInfo, Set<String>> e : baseTableAndPartitionNames.entrySet()) {
            PCTTableSnapshotInfo snapshotInfo = (PCTTableSnapshotInfo) e.getKey();
            Set<String> refreshedPartitionNames = e.getValue();
            Map<String, MaterializedView.BasePartitionInfo> refreshedPartitionInfos =
                    getRefreshedPartitionInfos(snapshotInfo, refreshedPartitionNames);
            snapshotInfo.setRefreshedPartitionInfos(refreshedPartitionInfos);
        }
    }


    /**
     * Return all non-ref base table and refreshed partitions.
     */
    private Map<BaseTableSnapshotInfo, Set<String>> getNonRefTableRefreshPartitions() {
        Map<BaseTableSnapshotInfo, Set<String>> tableNamePartitionNames = Maps.newHashMap();
        Map<Table, Map<String, Set<String>>> baseTableToMvNameRefs = mvContext.getRefBaseTableMVIntersectedPartitions();
        for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table table = snapshotInfo.getBaseTable();
            if (baseTableToMvNameRefs != null && baseTableToMvNameRefs.containsKey(table)) {
                // do nothing
            } else {
                if (table.isNativeTableOrMaterializedView()) {
                    tableNamePartitionNames.put(snapshotInfo, ((OlapTable) table).getVisiblePartitionNames());
                } else if (table.isView()) {
                    // do nothing
                } else {
                    tableNamePartitionNames.put(snapshotInfo, Sets.newHashSet(PartitionUtil.getPartitionNames(table)));
                }
            }
        }
        return tableNamePartitionNames;
    }

    /**
     * Collect base table and its refreshed partition infos based on refreshed table infos.
     */
    private Map<String, MaterializedView.BasePartitionInfo> getRefreshedPartitionInfos(
            PCTTableSnapshotInfo snapshotInfo, Set<String> refreshedPartitionNames) {
        Table baseTable = snapshotInfo.getBaseTable();
        BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
        if (baseTable.isNativeTableOrMaterializedView()) {
            Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
            OlapTable olapTable = (OlapTable) baseTable;
            for (String partitionName : refreshedPartitionNames) {
                Partition partition = olapTable.getPartition(partitionName);
                // it's ok to skip because only existed partitions are updated in the version map.
                if (partition == null) {
                    logger.warn("partition {} not found in base table {}, refreshedPartitionNames:{}",
                            partitionName, baseTable.getName(), refreshedPartitionNames);
                    continue;
                }
                MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                        partition.getId(),
                        partition.getDefaultPhysicalPartition().getVisibleVersion(),
                        partition.getDefaultPhysicalPartition().getVisibleVersionTime());
                partitionInfos.put(partition.getName(), basePartitionInfo);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Collect olap base table {}'s refreshed partition infos: {}", baseTable.getName(), partitionInfos);
            }
            return partitionInfos;
        } else if (MVPCTRefreshPartitioner.isPartitionRefreshSupported(baseTable)) {
            return getSelectedPartitionInfos(baseTable, Lists.newArrayList(refreshedPartitionNames), baseTableInfo);
        } else {
            // FIXME: base table does not support partition-level refresh and does not update the meta
            //  in materialized view.
            logger.warn("refresh mv with non-supported-partition-level refresh base table {}", baseTable.getName());
            return Maps.newHashMap();
        }
    }

    /**
     * @param table                  : input table to collect refresh partition infos
     * @param selectedPartitionNames : input table refreshed partition names
     * @param baseTableInfo          : input table's base table info
     * @return : return the given table's refresh partition infos
     */
    private Map<String, MaterializedView.BasePartitionInfo> getSelectedPartitionInfos(Table table,
                                                                                      List<String> selectedPartitionNames,
                                                                                      BaseTableInfo baseTableInfo) {
        // sort selectedPartitionNames before the for loop, otherwise the order of partition names may be
        // different in selectedPartitionNames and partitions and will lead to infinite partition refresh.
        Collections.sort(selectedPartitionNames);
        Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
        List<com.starrocks.connector.PartitionInfo> partitions = GlobalStateMgr.
                getCurrentState().getMetadataMgr().getPartitions(baseTableInfo.getCatalogName(), table,
                        selectedPartitionNames);
        for (int index = 0; index < selectedPartitionNames.size(); ++index) {
            long modifiedTime = partitions.get(index).getModifiedTime();
            String partitionName = selectedPartitionNames.get(index);
            MaterializedView.BasePartitionInfo basePartitionInfo =
                    new MaterializedView.BasePartitionInfo(-1, modifiedTime, modifiedTime);
            TableUpdateArbitrator.UpdateContext updateContext = new TableUpdateArbitrator.UpdateContext(
                    table,
                    -1,
                    Lists.newArrayList(partitionName));
            if (table instanceof HiveTable
                    && ((HiveTable) table).getHiveTableType() == HiveTable.HiveTableType.EXTERNAL_TABLE) {
                TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
                if (arbitrator != null) {
                    Map<String, Optional<HivePartitionDataInfo>> partitionDataInfos = arbitrator.getPartitionDataInfos();
                    Preconditions.checkState(partitionDataInfos.size() == 1);
                    if (partitionDataInfos.get(partitionName).isPresent()) {
                        HivePartitionDataInfo hivePartitionDataInfo = partitionDataInfos.get(partitionName).get();
                        basePartitionInfo.setExtLastFileModifiedTime(hivePartitionDataInfo.getLastFileModifiedTime());
                        basePartitionInfo.setFileNumber(hivePartitionDataInfo.getFileNumber());
                    }
                }
            }
            partitionInfos.put(partitionName, basePartitionInfo);
        }
        return partitionInfos;
    }

    @VisibleForTesting
    public Map<Long, BaseTableSnapshotInfo> getSnapshotBaseTables() {
        return snapshotBaseTables;
    }

    @Override
    protected BaseTableSnapshotInfo buildBaseTableSnapshotInfo(BaseTableInfo baseTableInfo, Table table) {
        return new PCTTableSnapshotInfo(baseTableInfo, table);
    }

    public MVPCTRefreshPartitioner getMvRefreshPartitioner() {
        return mvRefreshPartitioner;
    }
}