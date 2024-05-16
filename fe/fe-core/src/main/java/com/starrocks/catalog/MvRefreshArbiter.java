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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.catalog.mv.MVTimelinessListPartitionArbiter;
import com.starrocks.catalog.mv.MVTimelinessNonPartitionArbiter;
import com.starrocks.catalog.mv.MVTimelinessRangePartitionArbiter;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.common.UnsupportedException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.connector.PartitionUtil.getMVPartitionNameWithList;
import static com.starrocks.connector.PartitionUtil.getMVPartitionNameWithRange;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

/**
* The arbiter of materialized view refresh. All implementations of refresh strategies should be here.
*/
public class MvRefreshArbiter {
    private static final Logger LOG = LogManager.getLogger(MvRefreshArbiter.class);

    public static boolean needsToRefreshTable(MaterializedView mv, Table table) {
        MvBaseTableUpdateInfo mvBaseTableUpdateInfo = getMvBaseTableUpdateInfo(mv, table, true, false);
        if (mvBaseTableUpdateInfo == null) {
            return true;
        }
        return CollectionUtils.isNotEmpty(mvBaseTableUpdateInfo.getToRefreshPartitionNames());
    }

    /**
     * Once the materialized view's base tables have updated, we need to check correspond materialized views' partitions
     * to be refreshed.
     *
     * @return : Collect all need refreshed partitions of materialized view.
     * @isQueryRewrite : Mark whether this caller is query rewrite or not, when it's true we can use staleness to shortcut
     * the update check.
     */
    public static MvUpdateInfo getMVTimelinessUpdateInfo(MaterializedView mv, boolean isQueryRewrite) {
        // Skip check for sync materialized view.
        if (mv.getRefreshScheme().isSync()) {
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);
        }

        // check mv's query rewrite consistency mode property only in query rewrite.
        TableProperty tableProperty = mv.getTableProperty();
        TableProperty.QueryRewriteConsistencyMode mvConsistencyRewriteMode = tableProperty != null ?
                tableProperty.getQueryRewriteConsistencyMode() : TableProperty.QueryRewriteConsistencyMode.CHECKED;
        if (isQueryRewrite) {
            switch (mvConsistencyRewriteMode) {
                case DISABLE:
                    return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
                case NOCHECK:
                    return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);
                case LOOSE:
                case CHECKED:
                default:
                    break;
            }
        }

        logMVPrepare(mv, "MV refresh arbiter start to get partition names to refresh, query rewrite mode: {}",
                mvConsistencyRewriteMode);
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            MVTimelinessArbiter timelinessArbiter = new MVTimelinessNonPartitionArbiter(mv);
            return timelinessArbiter.getMVTimelinessUpdateInfo(isQueryRewrite, mvConsistencyRewriteMode);
        } else if (partitionInfo.isRangePartitionedV1()) {
            MVTimelinessArbiter timelinessArbiter = new MVTimelinessRangePartitionArbiter(mv);
            return timelinessArbiter.getMVTimelinessUpdateInfo(isQueryRewrite, mvConsistencyRewriteMode);
        } else if (partitionInfo.isListPartition()) {
            MVTimelinessArbiter timelinessArbiter = new MVTimelinessListPartitionArbiter(mv);
            return timelinessArbiter.getMVTimelinessUpdateInfo(isQueryRewrite, mvConsistencyRewriteMode);
        } else {
            throw UnsupportedException.unsupportedException("unsupported partition info type:"
                    + partitionInfo.getClass().getName());
        }
    }

    /**
     * Get to refresh partition info of the specific table.
     * @param baseTable: the table to check
     * @param withMv: whether to check the materialized view if it's a materialized view
     * @param isQueryRewrite: whether this caller is query rewrite or not
     * @return MvBaseTableUpdateInfo: the update info of the base table
     */
    public static MvBaseTableUpdateInfo getMvBaseTableUpdateInfo(MaterializedView mv,
                                                                 Table baseTable,
                                                                 boolean withMv,
                                                                 boolean isQueryRewrite) {
        MvBaseTableUpdateInfo mvBaseTableUpdateInfo = new MvBaseTableUpdateInfo();
        if (baseTable.isView()) {
            // do nothing
            return mvBaseTableUpdateInfo;
        } else if (baseTable.isNativeTableOrMaterializedView()) {
            OlapTable olapBaseTable = (OlapTable) baseTable;
            Set<String> baseTableUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfOlapTable(olapBaseTable, isQueryRewrite);

            // recursive check its children
            if (withMv && baseTable.isMaterializedView()) {
                MvUpdateInfo mvUpdateInfo = getMVTimelinessUpdateInfo((MaterializedView) baseTable, isQueryRewrite);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    return null;
                }
                baseTableUpdatedPartitionNames.addAll(mvUpdateInfo.getMvToRefreshPartitionNames());
            }
            // update base table's partition info
            mvBaseTableUpdateInfo.getToRefreshPartitionNames().addAll(baseTableUpdatedPartitionNames);
        } else {
            Set<String> updatePartitionNames = mv.getUpdatedPartitionNamesOfExternalTable(baseTable, isQueryRewrite);
            if (updatePartitionNames == null) {
                return null;
            }
            Map<Table, Column> partitionTableAndColumns = mv.getRelatedPartitionTableAndColumn();
            if (!partitionTableAndColumns.containsKey(baseTable)) {
                // ATTENTION: This partition value is not formatted to mv partition type.
                mvBaseTableUpdateInfo.getToRefreshPartitionNames().addAll(updatePartitionNames);
                return mvBaseTableUpdateInfo;
            }

            try {
                List<String> updatedPartitionNamesList = Lists.newArrayList(updatePartitionNames);
                Column partitionColumn = partitionTableAndColumns.get(baseTable);
                Expr partitionExpr = MaterializedView.getPartitionExpr(mv);
                PartitionInfo mvPartitionInfo = mv.getPartitionInfo();
                if (mvPartitionInfo.isListPartition()) {
                    Map<String, List<List<String>>> partitionNameWithRange = getMVPartitionNameWithList(baseTable,
                            partitionColumn, updatedPartitionNamesList);
                    for (Map.Entry<String, List<List<String>>> e : partitionNameWithRange.entrySet()) {
                        mvBaseTableUpdateInfo.addListPartitionKeys(e.getKey(), e.getValue());
                    }
                    mvBaseTableUpdateInfo.addToRefreshPartitionNames(partitionNameWithRange.keySet());
                } else if (mvPartitionInfo.isRangePartition()) {
                    Map<String, Range<PartitionKey>> partitionNameWithRange = getMVPartitionNameWithRange(baseTable,
                            partitionColumn, updatedPartitionNamesList, partitionExpr);
                    for (Map.Entry<String, Range<PartitionKey>> e : partitionNameWithRange.entrySet()) {
                        mvBaseTableUpdateInfo.addRangePartitionKeys(e.getKey(), e.getValue());
                    }
                    mvBaseTableUpdateInfo.addToRefreshPartitionNames(partitionNameWithRange.keySet());
                } else {
                    return null;
                }
            } catch (AnalysisException e) {
                LOG.warn("Mv {}'s base table {} get partition name fail", mv.getName(), baseTable.getName(), e);
                return null;
            }
        }
        return mvBaseTableUpdateInfo;
    }
}
