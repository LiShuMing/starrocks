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

package com.starrocks.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.concurrent.lock.LockParams;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVPCTMetaRepairer;
import com.starrocks.scheduler.mv.MVTraceUtils;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.scheduler.TaskRun.MV_ID;

public abstract class BaseMVRefreshProcessor extends  BaseTaskRunProcessor {
    protected static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);
    // session.enable_spill
    protected static final String MV_SESSION_ENABLE_SPILL =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.ENABLE_SPILL;
    // session.query_timeout. Deprecated, only for compatibility with old version
    protected static final String MV_SESSION_QUERY_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.QUERY_TIMEOUT;
    // session.insert_timeout
    protected static final String MV_SESSION_INSERT_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.INSERT_TIMEOUT;

    protected Database db;
    protected MaterializedView mv;
    protected Logger logger;
    protected MvTaskRunContext mvContext;
    protected long oldTransactionVisibleWaitTimeout;
    protected IMaterializedViewMetricsEntity mvEntity;
    protected Map<Long, BaseTableSnapshotInfo> snapshotBaseTables = Maps.newHashMap();

    // only trigger to post process when mv has been refreshed successfully
    protected Constants.TaskRunState taskRunState = Constants.TaskRunState.FAILED;
    // for testing
    protected TaskRun nextTaskRun = null;
    // runtime profile
    @VisibleForTesting
    protected RuntimeProfile runtimeProfile;


    @VisibleForTesting
    public RuntimeProfile getRuntimeProfile() {
        return runtimeProfile;
    }

    public TaskRun getNextTaskRun() {
        return nextTaskRun;
    }

    @VisibleForTesting
    public MvTaskRunContext getMvContext() {
        return mvContext;
    }

    @VisibleForTesting
    public void setMvContext(MvTaskRunContext mvContext) {
        this.mvContext = mvContext;
    }

    protected abstract Constants.TaskRunState doProcessTaskRun(TaskRunContext context) throws Exception;

    protected abstract boolean syncPartitions() throws AnalysisException, LockTimeoutException;

    protected abstract BaseTableSnapshotInfo buildBaseTableSnapshotInfo(BaseTableInfo baseTableInfo, Table table);

    @VisibleForTesting
    @Override
    public void prepare(TaskRunContext context) throws Exception {
        // NOTE: mvId is set in Task's properties when creating
        final Map<String, String> properties = context.getProperties();
        final long mvId = Long.parseLong(properties.get(MV_ID));
        this.db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(context.ctx.getDatabase());
        if (this.db == null) {
            throw new DmlException("database " + context.ctx.getDatabase() + " do not exist.");
        }

        final Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId);
        if (table == null || !(table instanceof MaterializedView)) {
            throw new DmlException(String.format("materialized view:%s in database:%s do not exist when refreshing",
                    mvId, context.ctx.getDatabase()));
        }
        this.mv = (MaterializedView) table;

        // reinit the logger
        this.logger = MVTraceUtils.getLogger(mv, BaseMVRefreshProcessor.class);
        // try to activate the mv before refresh
        if (!mv.isActive()) {
            MVActiveChecker.tryToActivate(mv);
            logger.info("Activated the MV before refreshing: {}", mv.getName());
        }

        // metrics entity
        this.mvEntity = MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
        if (!mv.isActive()) {
            String errorMsg = String.format("Materialized view: %s/%d is not active due to %s.",
                    mv.getName(), mvId, mv.getInactiveReason());
            logger.warn(errorMsg);
            mvEntity.increaseRefreshJobStatus(Constants.TaskRunState.FAILED);
            throw new DmlException(errorMsg);
        }

        // wait util transaction is visible for mv refresh task
        // because mv will update base tables' visible version after insert, the mv's visible version
        // should keep up with the base tables, or it will return outdated result.
        this.oldTransactionVisibleWaitTimeout = context.ctx.getSessionVariable().getTransactionVisibleWaitTimeout();
        context.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(Long.MAX_VALUE / 1000);

        // Initialize status's job id which is used to track a batch of task runs.
        final String jobId = properties.containsKey(TaskRun.START_TASK_RUN_ID) ?
                properties.get(TaskRun.START_TASK_RUN_ID) : context.getTaskRunId();
        if (context.status != null) {
            context.status.setStartTaskRunId(jobId);
        }

        // prepare mv context
        this.mvContext = new MvTaskRunContext(context);
        // prepare partition ttl number
        int partitionTTLNumber = mv.getTableProperty().getPartitionTTLNumber();
        this.mvContext.setPartitionTTLNumber(partitionTTLNumber);

        logger.info("finish prepare refresh mv:{}, jobId: {}", mvId, jobId);
    }

    @Override
    public Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception {
        // init to collect the base timer for refresh profile
        final QueryDebugOptions queryDebugOptions = context.getCtx().getSessionVariable().getQueryDebugOptions();
        final Tracers.Mode mvRefreshTraceMode = queryDebugOptions.getMvRefreshTraceMode();
        final Tracers.Module mvRefreshTraceModule = queryDebugOptions.getMvRefreshTraceModule();
        Tracers.init(mvRefreshTraceMode, mvRefreshTraceModule, true, false);

        final ConnectContext connectContext = context.getCtx();
        final QueryMaterializationContext queryMVContext = new QueryMaterializationContext();
        connectContext.setQueryMVContext(queryMVContext);

        try {
            // do refresh
            try (Timer ignored = Tracers.watchScope("MVRefreshDoWholeRefresh")) {
                // refresh mv
                Preconditions.checkState(mv != null);
                mvEntity = MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
                this.taskRunState = doProcessTaskRun(context);
                // update metrics
                mvEntity.increaseRefreshJobStatus(taskRunState);
                connectContext.getState().setOk();
                return taskRunState;
            }
        } catch (Exception e) {
            if (mvEntity != null) {
                mvEntity.increaseRefreshJobStatus(Constants.TaskRunState.FAILED);
            }
            connectContext.getState().setError(e.getMessage());
            throw e;
        } finally {
            try {
                // If mv's not active, mvContext may be null.
                if (mvContext != null && mvContext.ctx != null) {
                    mvContext.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(oldTransactionVisibleWaitTimeout);
                }

                // reset query mv context to avoid affecting other tasks
                queryMVContext.clear();
                connectContext.setQueryMVContext(null);

                if (FeConstants.runningUnitTest) {
                    runtimeProfile = new RuntimeProfile();
                    Tracers.toRuntimeProfile(runtimeProfile);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("refresh mv trace logs: {}", Tracers.getTrace(mvRefreshTraceMode));
                }
            } catch (Exception e) {
                logger.error("Failed to close Tracers", e);
            }
        }
    }

    private String getPostRun(ConnectContext ctx, MaterializedView mv) {
        // check whether it's enabled to analyze MV task after task run for each task run,
        // so the analyze_for_mv can be set in session variable dynamically
        if (mv == null) {
            return "";
        }
        return TaskBuilder.getAnalyzeMVStmt(ctx, mv.getName());
    }

    @Override
    public void postTaskRun(TaskRunContext context) throws Exception {
        if (taskRunState != Constants.TaskRunState.SUCCESS) {
            return;
        }
        // recreate post run context for each task run
        final ConnectContext ctx = context.getCtx();
        final String postRun = getPostRun(ctx, mv);
        // visible for tests
        if (mvContext != null) {
            mvContext.setPostRun(postRun);
        }
        context.setPostRun(postRun);
        if (StringUtils.isNotEmpty(postRun)) {
            ctx.executeSql(postRun);
        }
    }

    /**
     * Change default connect context when for mv refresh this is because:
     * - MV Refresh may take much resource to load base tables' data into the final materialized view.
     * - Those changes are set by default and also able to be changed by users for their needs.
     *
     * @param mvConnectCtx
     */
    protected void changeDefaultConnectContextIfNeeded(ConnectContext mvConnectCtx,
                                                       Set<Table> baseTables) {
        // add resource group if resource group is enabled
        final TableProperty mvProperty = mv.getTableProperty();
        final SessionVariable mvSessionVariable = mvConnectCtx.getSessionVariable();
        if (mvSessionVariable.isEnableResourceGroup()) {
            String rg = ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME;
            if (mvProperty != null && !Strings.isNullOrEmpty(mvProperty.getResourceGroup())) {
                rg = mvProperty.getResourceGroup();
            }
            mvSessionVariable.setResourceGroup(rg);
        }

        // enable spill by default for mv if spill is not set by default and
        // `session.enable_spill` session variable is not set.
        if (Config.enable_materialized_view_spill &&
                !mvSessionVariable.isEnableSpill() &&
                !mvProperty.getProperties().containsKey(MV_SESSION_ENABLE_SPILL)) {
            mvSessionVariable.setEnableSpill(true);
        }

        if (!mvProperty.getProperties().containsKey(MV_SESSION_INSERT_TIMEOUT)
                && mvProperty.getProperties().containsKey(MV_SESSION_QUERY_TIMEOUT)) {
            // for compatibility
            mvProperty.getProperties().put(MV_SESSION_INSERT_TIMEOUT, mvProperty.getProperties().get(MV_SESSION_QUERY_TIMEOUT));
        }

        // set insert_max_filter_ratio by default
        if (!isMVPropertyContains(SessionVariable.INSERT_MAX_FILTER_RATIO)) {
            mvSessionVariable.setInsertMaxFilterRatio(Config.mv_refresh_fail_on_filter_data ? 0 : 1);
        }
        // enable profile by default for mv refresh task
        if (!isMVPropertyContains(SessionVariable.ENABLE_PROFILE) && !mvSessionVariable.isEnableProfile()) {
            mvSessionVariable.setEnableProfile(Config.enable_mv_refresh_collect_profile);
        }
        // set the default new_planner_optimize_timeout for mv refresh
        if (!isMVPropertyContains(SessionVariable.NEW_PLANNER_OPTIMIZER_TIMEOUT)) {
            mvSessionVariable.setOptimizerExecuteTimeout(Config.mv_refresh_default_planner_optimize_timeout);
        }
        // set enable_materialized_view_rewrite by default
        if (!isMVPropertyContains(SessionVariable.ENABLE_MATERIALIZED_VIEW_REWRITE) && Config.enable_mv_refresh_query_rewrite) {
            // Only enable mv rewrite when there are more than one related mvs that can be rewritten by other mvs.
            if (isEnableMVRefreshQueryRewrite(mvConnectCtx, baseTables)) {
                mvSessionVariable.setEnableMaterializedViewRewrite(Config.enable_mv_refresh_query_rewrite);
                mvSessionVariable.setEnableMaterializedViewRewriteForInsert(Config.enable_mv_refresh_query_rewrite);
            }
        }
        // set nested_mv_rewrite_max_level by default, only rewrite one level
        if (!isMVPropertyContains(SessionVariable.NESTED_MV_REWRITE_MAX_LEVEL)) {
            mvSessionVariable.setNestedMvRewriteMaxLevel(1);
        }
        // always exclude the current mv name from rewrite
        mvSessionVariable.setQueryExcludingMVNames(mv.getName());
        mvConnectCtx.setUseConnectorMetadataCache(Optional.of(true));
    }

    private boolean isMVPropertyContains(String key) {
        final String mvKey = PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + key;
        return mv.getTableProperty().getProperties().containsKey(mvKey);
    }

    private boolean isEnableMVRefreshQueryRewrite(ConnectContext ctx,
                                                  Set<Table> baseTables) {
        return MvUtils.getRelatedMvs(ctx, 1, baseTables).size() > 1;
    }

    /**
     * Build an AST for insert stmt
     * @param ctx: connect context
     * @param materializedViewPartitions: the partitions to be refreshed
     */
    protected InsertStmt generateInsertAst(ConnectContext ctx,
                                           Set<String> materializedViewPartitions) {
        final String definition = mvContext.getDefinition();
        final InsertStmt insertStmt =
                (InsertStmt) SqlParser.parse(definition, ctx.getSessionVariable()).get(0);
        // set target partitions
        if (CollectionUtils.isNotEmpty(materializedViewPartitions)) {
            insertStmt.setTargetPartitionNames(new PartitionNames(false, new ArrayList<>(materializedViewPartitions)));
        }
        // insert overwrite mv must set system = true
        insertStmt.setSystem(true);
        // if mv has set sort keys, materialized view's output columns
        // may be different from the defined query's output.
        // so set materialized view's defined outputs as target columns.
        final List<Integer> queryOutputIndexes = mv.getQueryOutputIndices();
        final List<Column> baseSchema = mv.getBaseSchemaWithoutGeneratedColumn();
        if (queryOutputIndexes != null && baseSchema.size() == queryOutputIndexes.size()) {
            final List<String> targetColumnNames = queryOutputIndexes.stream()
                    .map(baseSchema::get)
                    .map(Column::getName)
                    .map(String::toLowerCase) // case insensitive
                    .collect(Collectors.toList());
            insertStmt.setTargetColumnNames(targetColumnNames);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("generate insert-overwrite statement, materialized view's target partition names:{}, " +
                            "mv's target columns: {}, definition:{}",
                    Joiner.on(",").join(materializedViewPartitions),
                    insertStmt.getTargetColumnNames() == null ? "" : Joiner.on(",").join(insertStmt.getTargetColumnNames()),
                    definition);
        }
        return insertStmt;
    }

    /**
     * Update task run status's extra message to add more information for information_schema if possible.
     * @param action: a consumer to update the task run status
     */
    protected void updateTaskRunStatus(Consumer<TaskRunStatus> action) {
        if (this.mvContext == null || this.mvContext.status == null) {
            return;
        }
        action.accept(this.mvContext.status);
    }

    protected void refreshExternalTable(Map<BaseTableSnapshotInfo, Set<String>> baseTableCandidatePartitions) {
        final List<Pair<Table, BaseTableInfo>> toRepairTables = new ArrayList<>();
        // use it if refresh external table fails
        final ConnectContext connectContext = mvContext.getCtx();
        final List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            final Optional<Database> dbOpt =
                    GlobalStateMgr.getCurrentState().getMetadataMgr().getDatabase(connectContext, baseTableInfo);
            if (dbOpt.isEmpty()) {
                logger.warn("database {} do not exist in refreshing materialized view", baseTableInfo.getDbInfoStr());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }

            final Optional<Table> optTable = MvUtils.getTable(baseTableInfo);
            if (optTable.isEmpty()) {
                logger.warn("table {} do not exist when refreshing materialized view", baseTableInfo.getTableInfoStr());
                mv.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            // refresh old table
            final Table table = optTable.get();
            // if table is native table or materialized view or connector view or external table, no need to refresh
            if (table.isNativeTableOrMaterializedView() || table.isView()
                    || MaterializedViewAnalyzer.isExternalTableFromResource(table)) {
                logger.debug("No need to refresh table:{} because it is native table or mv or connector view",
                        baseTableInfo.getTableInfoStr());
                continue;
            }
            final BaseTableSnapshotInfo snapshotInfo = buildBaseTableSnapshotInfo(baseTableInfo, table);
            final Set<String> basePartitions = baseTableCandidatePartitions.get(snapshotInfo);
            if (CollectionUtils.isNotEmpty(basePartitions)) {
                // only refresh referenced partitions, to reduce metadata overhead
                final List<String> realPartitionNames = basePartitions.stream()
                        .flatMap(name -> mvContext.getExternalTableRealPartitionName(table, name).stream())
                        .collect(Collectors.toList());
                connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, realPartitionNames, false);
            } else {
                // refresh the whole table, which may be costly in extreme case
                connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, Lists.newArrayList(), true);
            }
            // should clear query cache
            connectContext.getGlobalStateMgr().getMetadataMgr().removeQueryMetadata();

            // check new table
            final Optional<Table> optNewTable = MvUtils.getTable(baseTableInfo);
            if (optNewTable.isEmpty()) {
                logger.warn("table {} does not exist after refreshing materialized view", baseTableInfo.getTableInfoStr());
                mv.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            // only collect to-repair tables when the table is not the same as the old one by checking the table identifier
            final Table newTable = optNewTable.get();
            if (!baseTableInfo.getTableIdentifier().equals(table.getTableIdentifier())) {
                toRepairTables.add(Pair.create(newTable, baseTableInfo));
            }
        }

        // do repair if needed
        if (!toRepairTables.isEmpty()) {
            MVPCTMetaRepairer.repairMetaIfNeeded(db, mv, toRepairTables);
        }
    }

    /**
     * Collect all deduplicated databases of the materialized view's base tables.
     * @param mv: the mv to check
     * @return: the deduplicated databases of the materialized view's base tables,
     * throw exception if the database does not exist.
     */
    protected LockParams collectDatabases(MaterializedView mv) {
        final LockParams lockParams = new LockParams();
        final ConnectContext connectContext = mvContext.getCtx();
        for (BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
            Optional<Database> dbOpt = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getDatabase(connectContext, baseTableInfo);
            if (dbOpt.isEmpty()) {
                logger.warn("database {} do not exist", baseTableInfo.getDbInfoStr());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }
            Database db = dbOpt.get();
            lockParams.add(db, baseTableInfo.getTableId());
        }
        return lockParams;
    }

    /**
     * Collect all base table snapshot infos for the mv which the snapshot infos are kept and used in the final
     * update meta phase.
     * 1. deep copy of the base table's metadata may be time costing, we can optimize it later.
     * 2. no needs to lock the base table's metadata since the metadata is not changed during the refresh process.
     * @param mv the mv to collect
     * @return the base table and its snapshot info map
     */
    @VisibleForTesting
    public Map<Long, BaseTableSnapshotInfo> collectBaseTableSnapshotInfos(MaterializedView mv)
            throws LockTimeoutException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final Map<Long, BaseTableSnapshotInfo> tables = Maps.newHashMap();
        final List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();

        final LockParams lockParams = collectDatabases(mv);
        final Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(lockParams, LockType.READ, Config.mv_refresh_try_lock_timeout_ms,
                TimeUnit.MILLISECONDS)) {
            logger.warn("failed to lock database: {} in collectBaseTableSnapshotInfos for mv refresh", lockParams);
            throw new LockTimeoutException("Failed to lock database: " + lockParams + " in collectBaseTableSnapshotInfos");
        }
        try {
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                final Optional<Table> tableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
                if (tableOpt.isEmpty()) {
                    logger.warn("table {} doesn't exist", baseTableInfo.getTableInfoStr());
                    throw new DmlException("Materialized view base table: %s not exist.",
                            baseTableInfo.getTableInfoStr());
                }

                // NOTE: DeepCopy.copyWithGson is very time costing, use `copyOnlyForQuery` to reduce the cost.
                // TODO: Implement a `SnapshotTable` later which can use the copied table or transfer to the real table.
                final Table table = tableOpt.get();
                if (table.isNativeTableOrMaterializedView()) {
                    OlapTable copied = null;
                    if (table.isOlapOrCloudNativeTable()) {
                        copied = new OlapTable();
                    } else {
                        copied = new MaterializedView();
                    }
                    final OlapTable olapTable = (OlapTable) table;
                    olapTable.copyOnlyForQuery(copied);
                    tables.put(table.getId(), buildBaseTableSnapshotInfo(baseTableInfo, copied));
                } else if (table.isView()) {
                    // skip to collect snapshots for views
                } else {
                    // for other table types, use the table directly which needs to lock if visits the table metadata.
                    tables.put(table.getId(), buildBaseTableSnapshotInfo(baseTableInfo, table));
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
        }
        logger.info("collect base table snapshot infos cost: {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return tables;
    }

}
