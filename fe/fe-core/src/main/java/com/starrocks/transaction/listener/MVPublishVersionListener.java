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


package com.starrocks.transaction.listener;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunTriggerType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.transaction.PublishVersionListener;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Set;

public class MVPublishVersionListener implements PublishVersionListener {
    private static final Logger LOG = LogManager.getLogger(MVPublishVersionListener.class);

    public static final MVPublishVersionListener INSTANCE = new MVPublishVersionListener();

    @Override
    public String getName() {
        return "MVPublishVersionListener";
    }

    @Override
    public void onFinish(TransactionState transactionState) {
        try {
            refreshMvIfNecessary(transactionState);
        } catch (Exception t) {
            LOG.warn("refresh mv after publish version failed:", DebugUtil.getStackTrace(t));
        }
    }

    /**
     * Refresh the materialized view if it should be triggered after base table was loaded.
     *
     * @param transactionState: the transaction state which contains updated table info.
     * @throws DdlException
     * @throws MetaNotFoundException
     */
    private void refreshMvIfNecessary(TransactionState transactionState) throws DdlException {
        // Refresh materialized view when base table update transaction has been visible
        long dbId = transactionState.getDbId();
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        if (localMetastore == null) {
            LOG.warn("local metastore is null when event trigger refresh.");
            return;
        }
        Database db = localMetastore.getDb(dbId);
        if (db == null) {
            LOG.warn("transaction db is null when event trigger refresh.");
            return;
        }
        for (long tableId : transactionState.getTableIdList()) {
            Table table = db.getTable(tableId);
            if (table == null) {
                LOG.warn("failed to get transaction tableId {} when pending refresh.", tableId);
                return;
            }

            Set<MvId> relatedMvs = table.getRelatedMaterializedViews();
            for (MvId mvId : relatedMvs) {
                long mvDbId = mvId.getDbId();
                Database mvDb = localMetastore.getDb(mvDbId);
                if (mvDb == null) {
                    LOG.warn("failed to get mv db {} when pending refresh.", mvId);
                    continue;
                }
                long mvTblId = mvId.getId();
                MaterializedView mv = (MaterializedView) mvDb.getTable(mvTblId);
                if (mv == null) {
                    LOG.warn("materialized view {} does not exists.", mvId);
                    continue;
                }
                Locker locker = new Locker();
                locker.lockTablesWithIntensiveDbLock(mvDb, Lists.newArrayList(mvTblId), LockType.READ);
                try {
                    if (mv.shouldTriggeredRefreshBy(db.getFullName(), table.getName())) {
                        LOG.info("Trigger auto materialized view refresh because of base table {} has changed, " +
                                "db:{}, mv:{}", table.getName(), mvDb.getFullName(), mv.getName());
                        HashMap<String, String> taskRunProperties = new HashMap<>();
                        taskRunProperties.put(TaskRun.PARTITION_START, null);
                        taskRunProperties.put(TaskRun.PARTITION_END, null);
                        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(false));
                        ExecuteOption executeOption = new ExecuteOption(
                                Constants.TaskRunPriority.NORMAL.value(), true, taskRunProperties);
                        executeOption.setManual(false);
                        executeOption.setSync(false);
                        executeOption.setTaskRunTriggerType(TaskRunTriggerType.EVENT_TRIGGER);
                        localMetastore.executeRefreshMvTask(mvDb.getFullName(), mv, executeOption);
                    }
                } finally {
                    locker.unLockTablesWithIntensiveDbLock(mvDb, Lists.newArrayList(mvId.getId()), LockType.READ);
                }
            }
        }
    }
}