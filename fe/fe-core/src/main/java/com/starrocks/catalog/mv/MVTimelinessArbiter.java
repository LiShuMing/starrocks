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

package com.starrocks.catalog.mv;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static com.starrocks.catalog.MvRefreshArbiter.getMvBaseTableUpdateInfo;

public abstract class MVTimelinessArbiter {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessArbiter.class);

    protected final MaterializedView mv;

    public MVTimelinessArbiter(MaterializedView mv) {
        this.mv = mv;
    }

    /**
     * Materialized Views' base tables have two kinds: ref base table and non-ref base table.
     * - If non ref base tables updated, need refresh all mv partitions.
     * - If ref base table updated, need refresh the ref base table's updated partitions.
     * <p>
     * eg:
     * CREATE MATERIALIZED VIEW mv1
     * PARTITION BY k1
     * DISTRIBUTED BY HASH(k1) BUCKETS 10
     * AS
     * SELECT k1, v1 as k2, v2 as k3
     * from t1 join t2
     * on t1.k1 and t2.kk1;
     * <p>
     * - t1 is mv1's ref base table because mv1's partition column k1 is deduced from t1
     * - t2 is mv1's non ref base table because mv1's partition column k1 is not associated with t2.
     *
     * @return : partitioned materialized view's all need updated partition names.
     */
    public MvUpdateInfo getMVTimelinessUpdateInfo(boolean isQueryRewrite,
                                                  TableProperty.QueryRewriteConsistencyMode rewriteMode) {
        if (rewriteMode == TableProperty.QueryRewriteConsistencyMode.LOOSE) {
            return getMVTimelinessUpdateInfoInLoose(isQueryRewrite);
        } else {
            return getMVTimelinessUpdateInfoInChecked(isQueryRewrite);
        }
    }

    public abstract MvUpdateInfo getMVTimelinessUpdateInfoInChecked(boolean isQueryRewrite);


    /**
     * In Loose mode, do not need to check mv partition's data is consistent with base table's partition's data.
     * Only need to check the mv partition existence.
     */
    public abstract MvUpdateInfo getMVTimelinessUpdateInfoInLoose(boolean isQueryRewrite);

    protected MvUpdateInfo.MvToRefreshType determineRefreshType(Map<Table, Column> partitionInfos,
                                                                boolean isQueryRewrite) {
        TableProperty tableProperty = mv.getTableProperty();
        boolean isDisableExternalForceQueryRewrite = tableProperty != null &&
                tableProperty.getForceExternalTableQueryRewrite() == TableProperty.QueryRewriteConsistencyMode.DISABLE;
        for (BaseTableInfo tableInfo : mv.getBaseTableInfos()) {
            Table baseTable = MvUtils.getTableChecked(tableInfo);
            // skip view
            if (baseTable.isView()) {
                continue;
            }
            // skip external table that is not supported for query rewrite, return all partition ?
            // skip check external table if the external does not support rewrite.
            if (!baseTable.isNativeTableOrMaterializedView() && isDisableExternalForceQueryRewrite) {
                return MvUpdateInfo.MvToRefreshType.FULL;
            }
            if (partitionInfos.containsKey(baseTable)) {
                continue;
            }
            // If the non ref table has already changed, need refresh all materialized views' partitions.
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo = getMvBaseTableUpdateInfo(mv, baseTable, true, isQueryRewrite);
            if (mvBaseTableUpdateInfo == null || CollectionUtils.isNotEmpty(mvBaseTableUpdateInfo.getToRefreshPartitionNames())) {
                return MvUpdateInfo.MvToRefreshType.FULL;
            }
        }
        return MvUpdateInfo.MvToRefreshType.PARTIAL;
    }
}
