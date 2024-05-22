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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.transaction.PublishVersionListener;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StatsPublishVersionListener implements PublishVersionListener {
    public static final StatsPublishVersionListener INSTANCE = new StatsPublishVersionListener();

    private static final Logger LOG = LogManager.getLogger(StatsPublishVersionListener.class);

    @Override
    public String getName() {
        return "StatsPublishVersionListener";
    }

    @Override
    public void onFinish(TransactionState transactionState) {
        long dbId = transactionState.getDbId();
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        if (localMetastore == null) {
            LOG.warn("local metastore is null when transaction finish.");
            return;
        }
        Database db = localMetastore.getDb(dbId);
        if (db == null) {
            LOG.warn("transaction db is null when transaction finish.");
            return;
        }
        try {
            collectStatisticsForStreamLoadOnFirstLoad(transactionState, db);
        } catch (Exception t) {
            LOG.warn("refresh mv after publish version failed:", DebugUtil.getStackTrace(t));
        }
    }

    private void collectStatisticsForStreamLoadOnFirstLoad(TransactionState txnState, Database db) {
        TransactionState.LoadJobSourceType sourceType = txnState.getSourceType();
        if (!TransactionState.LoadJobSourceType.FRONTEND_STREAMING.equals(sourceType)
                && !TransactionState.LoadJobSourceType.BACKEND_STREAMING.equals(sourceType)) {
            return;
        }
        List<Table> tables = txnState.getIdToTableCommitInfos().values().stream()
                .map(x -> x.getTableId())
                .distinct()
                .map(db::getTable)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        for (Table table : tables) {
            StatisticUtils.triggerCollectionOnFirstLoad(txnState, db, table, false);
        }
    }
}
